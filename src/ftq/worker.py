"""Worker: fetch jobs, run their handlers, and move each entry to its next state.

One worker process runs, on one event loop:

- the **fetch loop**: reclaims expired leases (the reaper, ADR-023), then reads new
  entries. Both are limited to the free slots (`concurrency - in_flight`), so reclaimed
  and fresh jobs share one in-flight cap, and nothing sits prefetched in this worker's PEL
  without being worked on (ADR-022).
- one task per **in-flight job**: runs its handler with a **heartbeat** task beside it
  (ADR-025), then commits, schedules a retry, or moves the job to the DLQ (ADR-026/027).
- a **maintenance loop**: moves due retries back to the stream, and prunes idle consumer
  records that own no pending entries (ADR-029).

Everything on the loop must keep yielding, or heartbeats stop and leases expire. So
blocking and CPU-bound handlers run in a thread or process pool (ADR-028).

Every handler run is bounded by a timeout (ADR-030). Past it, the worker stops waiting,
discards whatever the run produces later, and treats the attempt as failed. It also
stops the run where it can: an async handler is cancelled, a process-pool child is
killed. A thread can't be stopped, so it keeps running as an "orphan" that holds its
slot until it returns.
"""

import asyncio
import concurrent.futures
import contextlib
import importlib
import json
import logging
import multiprocessing
import os
import random
import secrets
import signal
import socket
import time
import weakref
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from typing import Any, Literal

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ftq.backoff import full_jitter_delay
from ftq.config import Settings
from ftq.keys import Keys
from ftq.ledger import EffectLedger
from ftq.logs import ContextLogger
from ftq.models import Job
from ftq.reaper import Reaper
from ftq.registry import AsyncSpec, HandlerSpec, JobContext, Registry, SyncSpec
from ftq.scheduler import Scheduler
from ftq.transitions import Commit, DeadReason, Outcome, Transitions

log = logging.getLogger(__name__)

# After a fetch fails even with client-side retries (Redis down or partitioned), wait this
# long before trying again rather than crash-looping the process.
_FETCH_ERROR_PAUSE_S = 1.0
# Errors are stored in the DLQ entry and the done hash; keep a traceback-sized cap.
_MAX_ERROR_CHARS = 2000
# A Redis round trip failed even after the client's retries: the transition may or may
# not have happened. The entry stays in the PEL either way, and the reaper sorts it out.
_REDIS_DOWN = (RedisConnectionError, RedisTimeoutError)
# How long a new process pool's children may take to start and import the handlers
# before the pool counts as broken (ADR-039). Generous on purpose: it only catches a
# start-up that will never finish, and no job's timeout runs while a pool starts.
_POOL_START_TIMEOUT_S = 120.0
# How long each warm-up task holds its child. Only a rate limit: while one child is
# still starting, the ready ones answer each warm-up round in ~this long rather than in
# microseconds, so the rounds don't spin (ADR-039). Correctness comes from the loop.
_WARM_UP_HOLD_S = 0.05


def default_worker_id() -> str:
    """Unique per process: host + pid + random suffix (pids repeat across containers)."""
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"


def _ignore_sigint() -> None:
    """Process-pool initializer. Ctrl-C signals the whole foreground process group; the
    worker handles it as "drain", so a pool child must not die of KeyboardInterrupt
    mid-job (that would turn a graceful stop into a failed attempt).
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _init_pool_child(modules: tuple[str, ...]) -> None:
    """Process-pool initializer: everything a child does before it can run a job.

    Import every process handler's module now. A job would otherwise import it on first
    use, on its own clock (ADR-039).
    """
    _ignore_sigint()
    for module in modules:
        importlib.import_module(module)


def _pool_child_ready() -> int:
    """A warm-up task. Only a child that got through the initializer can run it, so its
    pid says that child is ready. The pause only paces the rounds (_WARM_UP_HOLD_S)."""
    time.sleep(_WARM_UP_HOLD_S)
    return os.getpid()


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_CHARS]


def _job_context(job: Job) -> dict[str, Any]:
    """Per-job log fields (logs.py): every line about a job carries these."""
    return {"job_id": job.job_id, "job_type": job.type, "attempt": job.attempt}


def run_kind(spec: HandlerSpec) -> Literal["async", "thread", "process"]:
    return "async" if isinstance(spec, AsyncSpec) else spec.pool


class HandlerTimeout(Exception):
    """A handler run took longer than its timeout: a failed attempt (ADR-030)."""


class Worker:
    def __init__(
        self,
        redis: aioredis.Redis,
        settings: Settings,
        registry: Registry,
        worker_id: str | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.worker_id = worker_id or default_worker_id()
        self._log = ContextLogger(log, {"worker_id": self.worker_id})
        # A handler type's own timeout needs the same TTL margin as the default (ADR-010).
        for spec in registry.specs().values():
            if spec.timeout is not None:
                settings.check_ttl_covers(spec.timeout)
        self._redis = redis
        self._settings = settings
        self._registry = registry
        self._keys = Keys(settings.queue)
        self._ledger = EffectLedger(redis, self._keys, settings.done_ttl_seconds)
        self._transitions = Transitions(redis, settings, self.worker_id)
        self._reaper = Reaper(redis, settings, self.worker_id)
        self._scheduler = Scheduler(redis, settings)
        self._rng = rng or random.Random()  # retry jitter; tests pass a seeded one
        self._stop = asyncio.Event()
        self._in_flight: set[asyncio.Task[None]] = set()
        # The in-flight jobs that are suspects: reclaimed so often they may be what keeps
        # crashing workers. At most one at a time (ADR-035).
        self._suspects: set[asyncio.Task[None]] = set()
        self._next_reap = 0.0  # loop time of the next reaper pass; 0 = at startup
        self._thread_pool: ThreadPoolExecutor | None = None
        self._process_pool: ProcessPoolExecutor | None = None
        # Done once every child of the current pool is started and ready (ADR-039).
        self._pool_ready: asyncio.Future[None] | None = None
        # What a pool child imports before it's ready: the process handlers' modules.
        self._process_modules = tuple(
            sorted(
                {
                    spec.fn.__module__
                    for spec in registry.specs().values()
                    if isinstance(spec, SyncSpec) and spec.pool == "process"
                }
            )
        )
        # Pools this worker killed on purpose (a timeout). Jobs that were running in one
        # and fail with BrokenProcessPool are resubmitted, not failed (ADR-030).
        self._reset_pools: weakref.WeakSet[ProcessPoolExecutor] = weakref.WeakSet()
        # At most one process-pool job per child, so none waits inside the pool (ADR-030).
        self._process_slots = asyncio.Semaphore(settings.process_pool_size)
        # Runs that timed out (or were abandoned at shutdown) but haven't stopped yet: a
        # thread that can't be killed, or an async handler that ignored its cancellation.
        # Each one still holds a slot, so it counts against `concurrency`.
        self._orphans: set[asyncio.Future[Any]] = set()
        # The thread-pool futures among them, checkable after the loop has closed.
        self._orphan_threads: list[concurrent.futures.Future[Any]] = []

    # ------------------------------------------------------------ lifecycle

    def request_stop(self) -> None:
        """Stop fetching; `run()` then drains in-flight jobs and returns. Signal-safe."""
        if not self._stop.is_set():
            self._log.info("stop requested, draining in-flight jobs")
        self._stop.set()

    async def ensure_group(self) -> None:
        """Create the consumer group (and the stream) if they don't exist yet.

        Start ID `0`, not `$`: jobs enqueued before the first worker ever started must
        be delivered too. `$` would silently skip them, which would be job loss.
        """
        try:
            await self._redis.xgroup_create(
                self._keys.stream, self._settings.group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # BUSYGROUP = already exists, which is fine
                raise

    async def _connect(self) -> bool:
        """Create the group, waiting out a Redis outage; False if stopped first.

        A worker can start while Redis is unreachable: a restarted container coming up
        inside a network partition, say. Crashing then would only make its supervisor
        restart it into the same outage, again and again. So startup waits like the fetch
        loop does (ADR-022). Found by the chaos run (PROGRESS.md, Phase 4).
        """
        while not self._stop.is_set():
            try:
                await self.ensure_group()
                return True
            except _REDIS_DOWN as exc:
                self._log.warning("Redis unreachable at startup (%s); retrying", exc)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), _FETCH_ERROR_PAUSE_S)
        return False

    async def run(self) -> None:
        """Process jobs until `request_stop()`, then drain within `shutdown_grace`."""
        if not await self._connect():
            self._log.info("stopped before Redis was reachable")
            return
        self._log.info(
            "worker %s: started (queue=%s, concurrency=%d, lease=%.1fs, timeout=%.1fs, "
            "handlers=%s)",
            self.worker_id,
            self._settings.queue,
            self._settings.concurrency,
            self._settings.visibility_timeout,
            self._settings.job_timeout,
            self._registry.types(),
        )
        maintenance = asyncio.create_task(self._maintenance_loop())
        abandoned = False
        try:
            await self._fetch_loop()
            self._log.info("stopped fetching; %d job(s) in flight", len(self._in_flight))
        finally:
            maintenance.cancel()
            await asyncio.gather(maintenance, return_exceptions=True)
            try:
                abandoned = await self._drain()
            finally:
                self._shutdown_pools(abandoned)
        self._log.info("stopped")

    def threads_still_running(self) -> int:
        """Handler threads that timed out or were abandoned and haven't returned yet.
        A thread can't be killed; the CLI hard-exits over them after the drain (ADR-030)."""
        return sum(1 for cf in self._orphan_threads if not cf.done())

    async def _fetch_loop(self) -> None:
        loop = asyncio.get_running_loop()
        stop_waiter = asyncio.ensure_future(self._stop.wait())
        try:
            while not self._stop.is_set():
                # An orphaned run still uses a thread (or loop time), so it keeps its slot.
                free = self._settings.concurrency - len(self._in_flight) - len(self._orphans)
                if free <= 0:
                    # All slots busy: sleep until a job (or an orphan) finishes or a stop is
                    # requested. A busy worker doesn't reap: it couldn't run what it claimed.
                    await asyncio.wait(
                        {*self._in_flight, *self._orphans, stop_waiter},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue
                if loop.time() >= self._next_reap:
                    free -= await self._reap(free)
                    if free == 0:
                        continue
                try:
                    entries = await self._fetch(free)
                except _REDIS_DOWN as exc:
                    self._log.warning("fetch failed (%s); retrying", exc)
                    await asyncio.wait({stop_waiter}, timeout=_FETCH_ERROR_PAUSE_S)
                    continue
                # Even if a stop arrived during the fetch, these entries are now in our
                # PEL, so run them rather than abandon them. A `>` read is always an
                # entry's first delivery.
                for entry_id, fields in entries:
                    self._spawn(entry_id, fields, deliveries=1)
        finally:
            stop_waiter.cancel()

    async def _reap(self, free: int) -> int:
        """One reaper pass: claim up to `free` expired entries and start them."""
        loop = asyncio.get_running_loop()
        # One suspect at a time: if a suspect is what crashes workers, only it and this
        # worker's fresh jobs go down with it, never a second suspect (ADR-035).
        suspect_slots = 0 if self._suspects else 1
        try:
            claimed, more = await self._reaper.reclaim(free, suspect_slots)
        except _REDIS_DOWN as exc:
            self._log.warning("reclaim failed (%s)", exc)
            self._next_reap = loop.time() + self._settings.reap_interval
            return 0
        # More PEL left to scan: continue on the next iteration instead of waiting.
        self._next_reap = loop.time() + (0 if more else self._settings.reap_interval)
        for c in claimed:
            self._log.debug(
                "reclaimed entry (delivery %d)", c.deliveries, extra={"entry_id": c.entry_id}
            )
            task = self._spawn(c.entry_id, c.fields, c.deliveries)
            if c.is_suspect(self._settings):
                self._suspects.add(task)
                task.add_done_callback(self._suspects.discard)
        return len(claimed)

    async def _fetch(self, count: int) -> list[tuple[str, dict[str, str]]]:
        """Read up to `count` never-delivered entries (`>`), blocking up to block_ms.

        The blocking read is never cancelled mid-flight: entries Redis delivered to a
        cancelled read would sit in the PEL with nobody working on them. A stop request
        therefore waits for the current read to return (at most block_ms).
        """
        # redis-py's declared reply type is a broad union; the actual shape (verified
        # against redis-py 8.1 + Redis 8.8, decode_responses=True) is below.
        reply: Any = await self._redis.xreadgroup(
            self._settings.group,
            self.worker_id,
            {self._keys.stream: ">"},
            count=count,
            block=self._settings.block_ms,
        )
        # Shape: [[stream_name, [(entry_id, {field: value}), ...]]], or [] on timeout.
        if not reply:
            return []
        entries: list[tuple[str, dict[str, str]]] = reply[0][1]
        return entries

    def _spawn(self, entry_id: str, fields: dict[str, str], deliveries: int) -> asyncio.Task[None]:
        task = asyncio.create_task(self._process(entry_id, fields, deliveries))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return task

    async def _drain(self) -> bool:
        """Give in-flight jobs `shutdown_grace` seconds to finish, then abandon the rest.

        An abandoned job was never committed, so its entry stays in the PEL, and a reaper
        reclaims it once the lease expires. Nothing is lost; the job just runs again, and
        the ledger keeps its effects from repeating. Returns True if anything was abandoned.
        """
        if not self._in_flight:
            return False
        _done, pending = await asyncio.wait(self._in_flight, timeout=self._settings.shutdown_grace)
        if not pending:
            return False
        self._log.warning(
            "grace period (%.1fs) over, abandoning %d in-flight job(s) to be reclaimed",
            self._settings.shutdown_grace,
            len(pending),
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return True

    async def _maintenance_loop(self) -> None:
        """Move due retries to the stream; now and then, prune empty idle consumers.

        A failure here must never end the loop: if the scheduler silently stopped, retries
        would wait in the delayed set until some other worker moved them. So every error
        is logged and the loop carries on.
        """
        loop = asyncio.get_running_loop()
        next_prune = loop.time()  # first pass at startup cleans up after old restarts
        while True:
            moved = 0
            try:
                moved = await self._scheduler.move_due()
                if loop.time() >= next_prune:
                    next_prune = loop.time() + self._settings.consumer_prune_interval
                    pruned = await self._reaper.prune_consumers()
                    if pruned:
                        self._log.info(
                            "pruned %d idle consumer(s) with no pending entries", len(pruned)
                        )
            except Exception as exc:  # see docstring: log and keep going
                self._log.warning("maintenance pass failed: %r", exc)
            # A full batch means more may be due already: go again without sleeping.
            if moved < self._settings.scheduler_batch:
                await asyncio.sleep(self._settings.scheduler_interval)

    # ------------------------------------------------------------ one job

    async def _process(self, entry_id: str, fields: dict[str, str], deliveries: int) -> None:
        try:
            job = Job.from_fields(fields)
        except (KeyError, ValueError) as exc:  # ValueError covers JSON + pydantic errors
            # A malformed entry can never succeed; retrying it would only burn attempts.
            job_id = fields.get("job_id") or f"entry:{entry_id}"
            await self._to_dlq(entry_id, job_id, DeadReason.MALFORMED, _error_text(exc), 0)
            return

        if deliveries > self._settings.max_deliveries:
            # Delivered this often, the job most likely crashes the worker that runs it
            # (each crash leaves the entry to be reclaimed, bumping its count). Don't run
            # it again: that would take down this worker too.
            error = f"delivered {deliveries} times (max_deliveries={self._settings.max_deliveries})"
            await self._to_dlq(
                entry_id, job.job_id, DeadReason.MAX_DELIVERIES, error, job.attempt + 1
            )
            return

        spec = self._registry.get(job.type)
        if spec is None:
            error = f"no handler registered for type {job.type!r}"
            await self._to_dlq(entry_id, job.job_id, DeadReason.UNKNOWN_TYPE, error, job.attempt)
            return

        ctx = _job_context(job)
        self._log.debug("running job", extra=ctx)
        heartbeat = (
            asyncio.create_task(self._heartbeat_loop(entry_id, job)) if spec.heartbeat else None
        )
        failure: Exception | None = None
        try:
            result_json = await self._run_handler(spec, job)
        except Exception as exc:  # HandlerTimeout included: a timeout is a failed attempt
            failure = exc
        finally:
            if heartbeat is not None:
                # Wait for it to actually stop, so no beat still in flight on the client
                # side lands after the transition below. (One that does is harmless: the
                # ownership check turns it into LEASE_LOST. But it would log a false
                # "lost the lease" and inflate the lease_lost counter.) After a timeout
                # this is also what stops an orphaned thread's job from being kept alive.
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

        if failure is not None:
            await self._fail(entry_id, job, failure)
            return

        try:
            outcome = await self._transitions.commit(entry_id, job, result_json)
        except _REDIS_DOWN as exc:
            # The handler SUCCEEDED; only the commit's round trip failed, even after
            # client-side retries of the (idempotent) commit. This is not a handler
            # failure: leave the entry in the PEL. When it's redelivered, either the
            # commit did land (the redelivery is suppressed) or it didn't (the job
            # reruns, and the ledger suppresses its effects).
            self._log.warning("commit failed (%s); left for redelivery", exc, extra=ctx)
            return
        except RedisError as exc:  # a server-side error: a bug or unexpected key state
            self._log.error("commit errored (%r); left in the PEL", exc, extra=ctx)
            return
        if outcome is Commit.LATE_SUCCESS:
            self._log.warning(
                "job succeeded after it was moved to the DLQ; now SUCCEEDED", extra=ctx
            )
        self._log.debug("job %s", outcome.name, extra=ctx)

    async def _run_handler(self, spec: HandlerSpec, job: Job) -> str:
        """Run one attempt, bounded by the handler's timeout; its result as JSON.

        The run is a separate future and this coroutine only waits on it. That's what
        makes the timeout hold for every kind of handler: past the deadline we stop
        waiting and raise HandlerTimeout, whatever the run is doing, even if it ignores
        cancellation or can't be stopped at all (a thread). Whatever an abandoned run
        returns later goes nowhere: only this coroutine could commit it, and it has
        already moved on to the retry path. (A non-JSON result is a failure too.)
        """
        timeout = spec.timeout or self._settings.job_timeout
        # A process-pool job first waits for a free child, so the timeout clock only
        # counts the run itself. (Otherwise, with concurrency > process_pool_size, a job
        # could time out while queued, and the reset would kill jobs that did run.)
        slot = self._process_slots if run_kind(spec) == "process" else contextlib.nullcontext()
        loop = asyncio.get_running_loop()
        async with slot:
            run = self._start(spec, job)
            try:
                # The timeout measures the handler's run, nothing else (ADR-039). The
                # clock is stopped while a process-pool run waits for a new pool to be
                # ready, and restarts from zero with each run: a pool reset restarts a
                # bystander as a new run. Waking at an old deadline, we find it moved.
                while not run.future.done():
                    if run.started is None:
                        clock = asyncio.ensure_future(run.clock_running.wait())
                        try:
                            await asyncio.wait(
                                {run.future, clock}, return_when=asyncio.FIRST_COMPLETED
                            )
                        finally:
                            clock.cancel()
                        continue
                    remaining = run.started + timeout - loop.time()
                    if remaining <= 0:
                        break
                    await asyncio.wait({run.future}, timeout=remaining)
            except asyncio.CancelledError:
                # The worker is abandoning this job (shutdown past the grace period).
                self._stop_run(run, job, abandoning=True)
                raise
            if not run.future.done():
                self._stop_run(run, job, abandoning=False)
                raise HandlerTimeout(f"run exceeded its {timeout:g}s timeout ({run.kind} handler)")
        return json.dumps(run.future.result(), separators=(",", ":"))

    def _start(self, spec: HandlerSpec, job: Job) -> "_Run":
        if isinstance(spec, AsyncSpec):
            ctx = JobContext(job=job, ledger=self._ledger, worker_id=self.worker_id)
            run = _Run("async")
            run.future = asyncio.ensure_future(spec.fn(ctx))
            return run
        if spec.pool == "thread":
            thread = self._threads().submit(spec.fn, job)
            run = _Run("thread", thread=thread)
            run.future = asyncio.wrap_future(thread)
            return run
        run = _Run("process")
        run.stop_clock()  # until its pool is ready
        run.future = asyncio.ensure_future(self._run_in_process(spec, job, run))
        return run

    async def _run_in_process(self, spec: SyncSpec, job: Job, run: "_Run") -> Any:
        loop = asyncio.get_running_loop()
        while True:
            run.stop_clock()
            pool = run.pool = await self._ready_processes()
            run.start_clock()  # a restart is a new run: its timeout starts over
            try:
                return await loop.run_in_executor(pool, spec.fn, job)
            except BrokenProcessPool:
                if pool in self._reset_pools:
                    # We killed this pool because ANOTHER job in it timed out. This job
                    # did nothing wrong, so run it again in a fresh pool, same attempt.
                    self._log.info(
                        "process pool was reset; restarting job", extra=_job_context(job)
                    )
                    continue
                # A pool child died mid-job (killed from outside, or the handler crashed
                # the interpreter). The pool is now unusable, so replace it. This job, and
                # any other job that was running in the pool, counts as a failed attempt.
                if self._process_pool is pool:
                    self._process_pool = None
                    pool.shutdown(wait=False, cancel_futures=True)
                raise

    def _stop_run(self, run: "_Run", job: Job, *, abandoning: bool) -> None:
        """Stop a run we're no longer waiting for, as far as its kind allows (ADR-030)."""
        run.future.cancel()  # async: CancelledError at its next await; thread: stops nothing
        if run.kind == "process":
            # Kill the child. One child can't be killed without breaking the whole pool
            # (every job in it gets BrokenProcessPool), so reset the pool: kill all its
            # children and start fresh. The other jobs running in it restart in the new
            # pool without losing an attempt (_run_in_process). On a shutdown abandon,
            # _shutdown_pools terminates the children instead.
            if not abandoning and run.pool is not None:
                self._reset_process_pool(run.pool)
            return
        # Async or thread: until the run really stops, it holds a slot. A cooperative
        # async handler is gone within a loop iteration. A thread, or a handler that
        # swallows its cancellation, may run for a long time.
        orphan = run.future if run.thread is None else asyncio.wrap_future(run.thread)
        if orphan.done():
            return
        if run.thread is not None:
            self._orphan_threads = [t for t in self._orphan_threads if not t.done()]
            self._orphan_threads.append(run.thread)
        self._orphans.add(orphan)
        ctx = _job_context(job)
        loop = asyncio.get_running_loop()
        abandoned_at = loop.time()

        def finished(fut: asyncio.Future[Any]) -> None:
            self._orphans.discard(fut)
            if fut.cancelled():
                return  # an async handler that stopped when cancelled: nothing to report
            # Retrieve the outcome (so asyncio doesn't warn that it was never read), and
            # drop it. The attempt already failed; a result from it must never commit.
            outcome = "error" if fut.exception() is not None else "result"
            self._log.warning(
                "orphaned %s run finished %.1fs after the worker stopped waiting; "
                "its %s was discarded",
                run.kind,
                loop.time() - abandoned_at,
                outcome,
                extra=ctx,
            )

        orphan.add_done_callback(finished)

    def _reset_process_pool(self, pool: ProcessPoolExecutor) -> None:
        """Kill every child of `pool`; the next process-pool job starts a fresh pool."""
        if self._process_pool is pool:
            self._process_pool = None
        self._reset_pools.add(pool)
        # No public API for this in 3.12 (3.14 adds terminate_workers()); see ADR-028.
        children = list((pool._processes or {}).values())
        for proc in children:
            proc.kill()  # SIGKILL: a hung handler may be in any state, even ignoring SIGTERM
        pool.shutdown(wait=False)  # not cancel_futures: queued jobs must fail and resubmit
        self._log.info(
            "process pool reset: killed %d child(ren) to stop a timed-out job", len(children)
        )

    async def _heartbeat_loop(self, entry_id: str, job: Job) -> None:
        """Extend the lease every `heartbeat_interval` until cancelled or the lease is lost.

        On LEASE_LOST (another worker reclaimed the entry after this one stalled), stop
        heartbeating but let the handler finish. Its commit is first-wins, and a retry or
        DLQ move from here would be refused, so finishing can't cause a double effect.
        """
        while True:
            await asyncio.sleep(self._settings.heartbeat_interval)
            try:
                outcome = await self._transitions.heartbeat(entry_id)
            except RedisError as exc:
                # Keep trying: one missed beat is fine (the interval is at most a third of
                # the lease), and if Redis stays unreachable the reclaim is the right outcome.
                self._log.warning("heartbeat failed (%s)", exc, extra=_job_context(job))
                continue
            if outcome is Outcome.LEASE_LOST:
                self._log.warning(
                    "lost the lease (reclaimed by another worker)", extra=_job_context(job)
                )
                return

    async def _fail(self, entry_id: str, job: Job, exc: Exception) -> None:
        """Handler raised or timed out: retry with backoff, or DLQ once attempts are used up."""
        error = _error_text(exc)
        timed_out = isinstance(exc, HandlerTimeout)
        ctx = _job_context(job)
        attempts = job.attempt + 1  # handler runs so far, this one included
        if timed_out:
            self._log.warning(
                "%s (attempt %d of %d)", error, attempts, self._settings.max_attempts, extra=ctx
            )
        if attempts >= self._settings.max_attempts:
            await self._to_dlq(
                entry_id, job.job_id, DeadReason.MAX_ATTEMPTS, error, attempts, timed_out=timed_out
            )
            return
        delay = full_jitter_delay(
            job.attempt, self._settings.job_backoff_base, self._settings.job_backoff_cap, self._rng
        )
        try:
            outcome = await self._transitions.retry(entry_id, job, delay, timed_out=timed_out)
        except RedisError as err:
            self._log.warning("could not schedule retry (%r); left in the PEL", err, extra=ctx)
            return
        if outcome is Outcome.LEASE_LOST:
            self._log.warning("job failed after its lease was lost; no retry scheduled", extra=ctx)
        else:
            self._log.debug(
                "attempt failed (%s); retry %s in %.2fs", error, outcome.name, delay, extra=ctx
            )

    async def _to_dlq(
        self,
        entry_id: str,
        job_id: str,
        reason: DeadReason,
        error: str,
        attempts: int,
        *,
        timed_out: bool = False,
    ) -> None:
        ctx = {"job_id": job_id, "entry_id": entry_id}
        try:
            outcome = await self._transitions.dead(
                entry_id, job_id, reason, error, attempts, timed_out=timed_out
            )
        except RedisError as exc:
            self._log.warning("could not move job to the DLQ (%r); left in the PEL", exc, extra=ctx)
            return
        if outcome is Outcome.OK:
            self._log.warning("job -> DLQ (%s): %s", reason, error, extra=ctx)
        else:
            self._log.info("DLQ move: %s", outcome.name, extra=ctx)

    # ------------------------------------------------------------ pools (ADR-028)

    def _threads(self) -> ThreadPoolExecutor:
        """The thread pool, created on first use."""
        if self._thread_pool is None:
            # One thread per slot: a blocking handler never waits for a thread. That holds
            # with orphaned threads too, because each one keeps its slot (ADR-030).
            self._thread_pool = ThreadPoolExecutor(
                max_workers=self._settings.concurrency, thread_name_prefix="ftq-handler"
            )
        return self._thread_pool

    async def _ready_processes(self) -> ProcessPoolExecutor:
        """The process pool, once all its children are started and ready (ADR-039).

        Created on first use, and again after a reset or a break. A child's start-up
        (a new interpreter, then the handler imports) is the pool's cost, not a job's,
        and on a starved machine it can outlast a short timeout. Counted on the jobs'
        clocks, it made every run after a reset time out and reset the pool again.
        If the pool fails to start, this raises BrokenProcessPool, like a pool that
        breaks mid-job.
        """
        if self._process_pool is None:
            # "spawn" on every OS: forking a process that runs an event loop and Redis
            # connections copies them in an undefined state, and spawn is also what macOS
            # uses, so local runs and Linux containers behave alike.
            ctx = multiprocessing.get_context("spawn")
            size = self._settings.process_pool_size
            pool = ProcessPoolExecutor(
                max_workers=size,
                mp_context=ctx,
                initializer=_init_pool_child,
                initargs=(self._process_modules,),
            )
            self._process_pool = pool
            self._pool_ready = asyncio.ensure_future(self._warm_up(pool))
            # Read its outcome even if every job waiting on it was cancelled meanwhile.
            self._pool_ready.add_done_callback(lambda f: f.cancelled() or f.exception())
        pool, ready = self._process_pool, self._pool_ready
        assert ready is not None
        await asyncio.shield(ready)
        return pool

    async def _warm_up(self, pool: ProcessPoolExecutor) -> None:
        """Start all of `pool`'s children and return once each has answered.

        A spawn-context pool starts a child per submitted task while it has no idle one,
        so the first round of `process_pool_size` tasks starts them all. A child still
        starting can't take a task, so rounds repeat until every child's pid has come
        back. (A multiprocessing Barrier in the initializer would do the same in one
        round, but each pool would allocate named semaphores, and with a reset every few
        seconds under chaos the resource tracker warned they could leak.)
        """
        loop = asyncio.get_running_loop()
        started = loop.time()
        size = self._settings.process_pool_size
        ready: set[int] = set()
        while len(ready) < size:
            if loop.time() - started > _POOL_START_TIMEOUT_S:
                raise BrokenProcessPool(
                    f"{size - len(ready)} of {size} pool children not ready "
                    f"after {_POOL_START_TIMEOUT_S:g}s"
                )
            round_ = (loop.run_in_executor(pool, _pool_child_ready) for _ in range(size))
            ready.update(await asyncio.gather(*round_))
        self._log.info("process pool ready: %d child(ren) in %.2fs", size, loop.time() - started)

    def _shutdown_pools(self, abandoned: bool) -> None:
        if self._thread_pool is not None:
            # A thread can't be killed. Waiting for an abandoned or orphaned one could
            # take forever, so don't; the CLI exits hard over any that are left (ADR-030).
            stuck = abandoned or self.threads_still_running() > 0
            self._thread_pool.shutdown(wait=not stuck, cancel_futures=True)
        if self._process_pool is not None:
            if abandoned:
                # The abandoned jobs' children would otherwise run to completion, and the
                # interpreter waits for them at exit, so the grace period wouldn't bound
                # shutdown. Their entries are in the PEL, uncommitted, so killing them
                # loses nothing. (Python 3.14 has a public terminate_workers(); 3.12
                # needs the private process map.)
                for proc in list((self._process_pool._processes or {}).values()):
                    proc.terminate()
            self._process_pool.shutdown(wait=True, cancel_futures=True)


@dataclass(eq=False)
class _Run:
    """One handler run in progress (ADR-030)."""

    kind: Literal["async", "thread", "process"]
    future: asyncio.Future[Any] = field(init=False)  # what the worker waits on
    # thread: the thread's own future, which says whether it's still running after we
    # stop waiting (cancelling `future` can't stop a thread).
    thread: concurrent.futures.Future[Any] | None = None
    # process: the pool the run is in now; it changes when a pool reset restarts it.
    pool: ProcessPoolExecutor | None = None
    # Loop time the run (last) started; its timeout counts from here. None while a
    # process-pool run waits for its pool to be ready: no clock yet (ADR-039).
    started: float | None = field(default_factory=lambda: asyncio.get_running_loop().time())
    clock_running: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.clock_running.set()

    def start_clock(self) -> None:
        self.started = asyncio.get_running_loop().time()
        self.clock_running.set()

    def stop_clock(self) -> None:
        self.started = None
        self.clock_running.clear()
