"""Worker: fetch jobs from the stream, run their handlers, commit results.

Phase 1 scope is the happy path plus idempotency and graceful shutdown. A job whose
handler raises (or whose entry can't be parsed, or whose type has no handler) is logged
and left in the Pending Entries List. Phase 2 adds the retry/DLQ transitions and the
reaper that reclaims such entries.

Fetching and the in-flight cap: the loop only asks XREADGROUP for as many entries as it
has free slots (`concurrency - in_flight`). Every fetched entry starts running
immediately, so nothing sits prefetched in this worker's PEL without being worked on.
"""

import asyncio
import json
import logging
import os
import secrets
import socket
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ftq.config import Settings
from ftq.keys import Keys
from ftq.ledger import EffectLedger
from ftq.lua import register
from ftq.models import Job
from ftq.registry import JobContext, Registry

log = logging.getLogger(__name__)

# After a fetch fails even with client-side retries (Redis down or partitioned), wait this
# long before trying again rather than crash-looping the process.
_FETCH_ERROR_PAUSE_S = 1.0


def default_worker_id() -> str:
    """Unique per process: host + pid + random suffix (pids repeat across containers)."""
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(3)}"


class Worker:
    def __init__(
        self,
        redis: aioredis.Redis,
        settings: Settings,
        registry: Registry,
        worker_id: str | None = None,
    ) -> None:
        self.worker_id = worker_id or default_worker_id()
        self._redis = redis
        self._settings = settings
        self._registry = registry
        self._keys = Keys(settings.queue)
        self._ledger = EffectLedger(redis, self._keys, settings.done_ttl_seconds)
        self._commit = register(redis, "commit")
        self._stop = asyncio.Event()
        self._in_flight: set[asyncio.Task[None]] = set()

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
            "worker %s: started (queue=%s, concurrency=%d, handlers=%s)",
            self.worker_id,
            self._settings.queue,
            self._settings.concurrency,
            self._registry.types(),
        )
        try:
            await self._fetch_loop()
            log.info(
                "worker %s: stopped fetching; %d job(s) in flight",
                self.worker_id,
                len(self._in_flight),
            )
        finally:
            await self._drain()
        log.info("worker %s: stopped", self.worker_id)

    async def _fetch_loop(self) -> None:
        stop_waiter = asyncio.ensure_future(self._stop.wait())
        try:
            while not self._stop.is_set():
                free = self._settings.concurrency - len(self._in_flight)
                if free == 0:
                    # All slots busy: sleep until a job finishes or a stop is requested.
                    await asyncio.wait(
                        {*self._in_flight, stop_waiter}, return_when=asyncio.FIRST_COMPLETED
                    )
                    continue
                try:
                    entries = await self._fetch(free)
                except (RedisConnectionError, RedisTimeoutError) as exc:
                    log.warning("worker %s: fetch failed (%s); retrying", self.worker_id, exc)
                    await asyncio.wait({stop_waiter}, timeout=_FETCH_ERROR_PAUSE_S)
                    continue
                # Even if a stop arrived during the fetch, these entries are now in our
                # PEL, so run them rather than abandon them.
                for entry_id, fields in entries:
                    self._spawn(entry_id, fields)
        finally:
            stop_waiter.cancel()

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

    def _spawn(self, entry_id: str, fields: dict[str, str]) -> None:
        task = asyncio.create_task(self._process(entry_id, fields))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    async def _drain(self) -> None:
        """Give in-flight jobs `shutdown_grace` seconds to finish, then abandon the rest.

        An abandoned job was never committed, so its entry stays in the PEL. The
        reaper (Phase 2) reclaims it after the lease expires. Nothing is lost; the job
        just runs again, and the ledger keeps its effects from repeating.
        """
        if not self._in_flight:
            return
        _done, pending = await asyncio.wait(self._in_flight, timeout=self._settings.shutdown_grace)
        if pending:
            log.warning(
                "worker %s: grace period (%.1fs) over, abandoning %d in-flight job(s) to be "
                "reclaimed",
                self.worker_id,
                self._settings.shutdown_grace,
                len(pending),
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------------ one job

    async def _process(self, entry_id: str, fields: dict[str, str]) -> None:
        try:
            job = Job.from_fields(fields)
        except (KeyError, ValueError) as exc:  # ValueError covers JSON + pydantic errors
            log.error("worker %s: malformed entry %s: %r", self.worker_id, entry_id, exc)
            return  # Phase 2: straight to the DLQ.

        handler = self._registry.get(job.type)
        if handler is None:
            log.error(
                "worker %s: no handler for type %r (job %s)", self.worker_id, job.type, job.job_id
            )
            return  # Phase 2: straight to the DLQ.

        log.debug("worker %s: running job %s (%s)", self.worker_id, job.job_id, job.type)
        try:
            result = await handler(
                JobContext(job=job, ledger=self._ledger, worker_id=self.worker_id)
            )
            result_json = json.dumps(result, separators=(",", ":"))
        except Exception:
            log.exception("worker %s: handler failed for job %s", self.worker_id, job.job_id)
            return  # Phase 2: retry with backoff, or DLQ after max attempts.

        try:
            committed = await self._commit_job(entry_id, job, result_json)
        except (RedisConnectionError, RedisTimeoutError) as exc:
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
        log.debug(
            "worker %s: job %s %s",
            self.worker_id,
            job.job_id,
            "committed" if committed else "was a duplicate (suppressed)",
        )

    async def _commit_job(self, entry_id: str, job: Job, result_json: str) -> bool:
        """Run commit.lua. True = first success recorded; False = duplicate suppressed."""
        outcome: Any = await self._commit(
            keys=[
                self._keys.stream,
                self._keys.done(job.job_id),
                self._keys.results,
                self._keys.stats,
            ],
            args=[
                self._settings.group,
                entry_id,
                job.job_id,
                result_json,
                str(self._settings.done_ttl_seconds),
                self.worker_id,
                str(job.enqueued_at_ms),
            ],
        )
        return int(outcome) == 1
