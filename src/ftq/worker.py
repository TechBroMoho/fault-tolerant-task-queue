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
"""

import asyncio
import json
import logging
import multiprocessing
import os
import random
import secrets
import signal
import socket
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ftq.backoff import full_jitter_delay
from ftq.config import Settings
from ftq.keys import Keys
from ftq.ledger import EffectLedger
from ftq.models import Job
from ftq.reaper import Reaper
from ftq.registry import AsyncSpec, HandlerSpec, JobContext, Pool, Registry
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


def default_worker_id() -> str:
    """Unique per process: host + pid + random suffix (pids repeat across containers)."""
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"


def _ignore_sigint() -> None:
    """Process-pool initializer. Ctrl-C signals the whole foreground process group; the
    worker handles it as "drain", so a pool child must not die of KeyboardInterrupt
    mid-job (that would turn a graceful stop into a failed attempt).
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_CHARS]


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
        self._next_reap = 0.0  # loop time of the next reaper pass; 0 = at startup
        self._thread_pool: ThreadPoolExecutor | None = None
        self._process_pool: ProcessPoolExecutor | None = None

    # ------------------------------------------------------------ lifecycle

    def request_stop(self) -> None:
        """Stop fetching; `run()` then drains in-flight jobs and returns. Signal-safe."""
        if not self._stop.is_set():
            log.info("worker %s: stop requested, draining in-flight jobs", self.worker_id)
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

    async def run(self) -> None:
        """Process jobs until `request_stop()`, then drain within `shutdown_grace`."""
        await self.ensure_group()
        log.info(
            "worker %s: started (queue=%s, concurrency=%d, lease=%.1fs, handlers=%s)",
            self.worker_id,
            self._settings.queue,
            self._settings.concurrency,
            self._settings.visibility_timeout,
            self._registry.types(),
        )
        maintenance = asyncio.create_task(self._maintenance_loop())
        abandoned = False
        try:
            await self._fetch_loop()
            log.info(
                "worker %s: stopped fetching; %d job(s) in flight",
                self.worker_id,
                len(self._in_flight),
            )
        finally:
            maintenance.cancel()
            await asyncio.gather(maintenance, return_exceptions=True)
            try:
                abandoned = await self._drain()
            finally:
                self._shutdown_pools(abandoned)
        log.info("worker %s: stopped", self.worker_id)

    async def _fetch_loop(self) -> None:
        loop = asyncio.get_running_loop()
        stop_waiter = asyncio.ensure_future(self._stop.wait())
        try:
            while not self._stop.is_set():
                free = self._settings.concurrency - len(self._in_flight)
                if free == 0:
                    # All slots busy: sleep until a job finishes or a stop is requested.
                    # A busy worker doesn't reap either: it couldn't run what it claimed.
                    await asyncio.wait(
                        {*self._in_flight, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
                    )
                    continue
                if loop.time() >= self._next_reap:
                    free -= await self._reap(free)
                    if free == 0:
                        continue
                try:
                    entries = await self._fetch(free)
                except _REDIS_DOWN as exc:
                    log.warning("worker %s: fetch failed (%s); retrying", self.worker_id, exc)
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
        try:
            claimed, more = await self._reaper.reclaim(free)
        except _REDIS_DOWN as exc:
            log.warning("worker %s: reclaim failed (%s)", self.worker_id, exc)
            self._next_reap = loop.time() + self._settings.reap_interval
            return 0
        # More PEL left to scan: continue on the next iteration instead of waiting.
        self._next_reap = loop.time() + (0 if more else self._settings.reap_interval)
        for c in claimed:
            log.debug(
                "worker %s: reclaimed %s (delivery %d)", self.worker_id, c.entry_id, c.deliveries
            )
            self._spawn(c.entry_id, c.fields, c.deliveries)
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

    def _spawn(self, entry_id: str, fields: dict[str, str], deliveries: int) -> None:
        task = asyncio.create_task(self._process(entry_id, fields, deliveries))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

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
        log.warning(
            "worker %s: grace period (%.1fs) over, abandoning %d in-flight job(s) to be reclaimed",
            self.worker_id,
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
                        log.info(
                            "worker %s: pruned %d idle consumer(s) with no pending entries",
                            self.worker_id,
                            len(pruned),
                        )
            except Exception as exc:  # see docstring: log and keep going
                log.warning("worker %s: maintenance pass failed: %r", self.worker_id, exc)
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

        log.debug("worker %s: running job %s (%s)", self.worker_id, job.job_id, job.type)
        heartbeat = (
            asyncio.create_task(self._heartbeat_loop(entry_id, job.job_id))
            if spec.heartbeat
            else None
        )
        failure: Exception | None = None
        try:
            result_json = await self._run_handler(spec, job)
        except Exception as exc:
            failure = exc
        finally:
            if heartbeat is not None:
                # Wait for it to actually stop, so no beat still in flight on the client
                # side lands after the transition below. (One that does is harmless: the
                # ownership check turns it into LEASE_LOST. But it would log a false
                # "lost the lease" and inflate the lease_lost counter.)
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
            log.warning(
                "worker %s: commit of job %s failed (%s); left for redelivery",
                self.worker_id,
                job.job_id,
                exc,
            )
            return
        except RedisError as exc:  # a server-side error: a bug or unexpected key state
            log.error(
                "worker %s: commit of job %s errored (%r); left in the PEL",
                self.worker_id,
                job.job_id,
                exc,
            )
            return
        if outcome is Commit.LATE_SUCCESS:
            log.warning(
                "worker %s: job %s succeeded after it was moved to the DLQ; now SUCCEEDED",
                self.worker_id,
                job.job_id,
            )
        log.debug("worker %s: job %s %s", self.worker_id, job.job_id, outcome.name)

    async def _run_handler(self, spec: HandlerSpec, job: Job) -> str:
        """Run the handler and return its result as JSON (a non-JSON result is a failure)."""
        if isinstance(spec, AsyncSpec):
            result = await spec.fn(
                JobContext(job=job, ledger=self._ledger, worker_id=self.worker_id)
            )
        else:
            executor = self._executor(spec.pool)
            try:
                result = await asyncio.get_running_loop().run_in_executor(executor, spec.fn, job)
            except BrokenProcessPool:
                # A pool child died mid-job (killed, or the handler crashed the
                # interpreter). The pool is now unusable, so replace it. This job, and any
                # other job that was running in the pool, counts as a failed attempt.
                if self._process_pool is executor:
                    self._process_pool = None
                    executor.shutdown(wait=False, cancel_futures=True)
                raise
        return json.dumps(result, separators=(",", ":"))

    async def _heartbeat_loop(self, entry_id: str, job_id: str) -> None:
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
                # Keep trying: one missed beat is fine (the interval is at most half the
                # lease), and if Redis stays unreachable the reclaim is the right outcome.
                log.warning(
                    "worker %s: heartbeat for job %s failed (%s)", self.worker_id, job_id, exc
                )
                continue
            if outcome is Outcome.LEASE_LOST:
                log.warning(
                    "worker %s: lost the lease on job %s (reclaimed by another worker)",
                    self.worker_id,
                    job_id,
                )
                return

    async def _fail(self, entry_id: str, job: Job, exc: Exception) -> None:
        """Handler raised: retry with backoff, or DLQ once attempts are used up."""
        error = _error_text(exc)
        attempts = job.attempt + 1  # handler runs so far, this one included
        if attempts >= self._settings.max_attempts:
            await self._to_dlq(entry_id, job.job_id, DeadReason.MAX_ATTEMPTS, error, attempts)
            return
        delay = full_jitter_delay(
            job.attempt, self._settings.job_backoff_base, self._settings.job_backoff_cap, self._rng
        )
        try:
            outcome = await self._transitions.retry(entry_id, job, delay)
        except RedisError as err:
            log.warning(
                "worker %s: could not schedule retry of job %s (%r); left in the PEL",
                self.worker_id,
                job.job_id,
                err,
            )
            return
        if outcome is Outcome.LEASE_LOST:
            log.warning(
                "worker %s: job %s failed after its lease was lost; no retry scheduled",
                self.worker_id,
                job.job_id,
            )
        else:
            log.debug(
                "worker %s: job %s attempt %d failed (%s); retry %s in %.2fs",
                self.worker_id,
                job.job_id,
                job.attempt,
                error,
                outcome.name,
                delay,
            )

    async def _to_dlq(
        self, entry_id: str, job_id: str, reason: DeadReason, error: str, attempts: int
    ) -> None:
        try:
            outcome = await self._transitions.dead(entry_id, job_id, reason, error, attempts)
        except RedisError as exc:
            log.warning(
                "worker %s: could not move job %s to the DLQ (%r); left in the PEL",
                self.worker_id,
                job_id,
                exc,
            )
            return
        if outcome is Outcome.OK:
            log.warning("worker %s: job %s -> DLQ (%s): %s", self.worker_id, job_id, reason, error)
        else:
            log.info("worker %s: DLQ move of job %s: %s", self.worker_id, job_id, outcome.name)

    # ------------------------------------------------------------ pools (ADR-028)

    def _executor(self, pool: Pool) -> Executor:
        """The thread or process pool, created on first use."""
        if pool == "thread":
            if self._thread_pool is None:
                # One thread per slot: a blocking handler never waits for a thread.
                self._thread_pool = ThreadPoolExecutor(
                    max_workers=self._settings.concurrency, thread_name_prefix="ftq-handler"
                )
            return self._thread_pool
        if self._process_pool is None:
            # "spawn" on every OS: forking a process that runs an event loop and Redis
            # connections copies them in an undefined state, and spawn is also what macOS
            # uses, so local runs and Linux containers behave alike.
            self._process_pool = ProcessPoolExecutor(
                max_workers=self._settings.process_pool_size,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_ignore_sigint,
            )
        return self._process_pool

    def _shutdown_pools(self, abandoned: bool) -> None:
        if self._thread_pool is not None:
            # A thread can't be killed. An abandoned blocking handler keeps the process
            # alive until it returns; `docker stop` escalates to SIGKILL (ADR-028).
            self._thread_pool.shutdown(wait=not abandoned, cancel_futures=True)
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
