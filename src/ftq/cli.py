"""Command-line interface: `ftq worker | enqueue | stats | bench | dlq list|requeue`.

Configuration comes from `FTQ_*` env vars (config.py); flags override a few of them.
"""

import asyncio
import dataclasses
import importlib
import json
import logging
import os
import signal
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import redis.asyncio as aioredis
import typer

from ftq import dlq, logs
from ftq.bench import format_report, run_bench
from ftq.client import Client, QueueFull
from ftq.config import Settings, make_redis
from ftq.keys import Keys
from ftq.metrics import snapshot
from ftq.registry import Registry
from ftq.worker import Worker

app = typer.Typer(no_args_is_help=True, add_completion=False)
dlq_app = typer.Typer(no_args_is_help=True, help="Inspect and requeue dead-lettered jobs.")
app.add_typer(dlq_app, name="dlq")


def _settings(**overrides: Any) -> Settings:
    # pydantic-settings: explicit init values win over env vars.
    return Settings(**{k: v for k, v in overrides.items() if v is not None})


def _load_registry(spec: str) -> Registry:
    """Import `module:attribute` and check it's a Registry."""
    module_name, _, attr = spec.partition(":")
    registry = getattr(importlib.import_module(module_name), attr or "registry")
    if not isinstance(registry, Registry):
        raise typer.BadParameter(f"{spec} is not an ftq Registry")
    return registry


async def run_worker(settings: Settings, registry: Registry) -> Worker:
    """Run one worker until SIGTERM/SIGINT, then drain gracefully. Returns the worker."""
    redis = make_redis(settings)
    worker = Worker(redis, settings, registry)
    loop = asyncio.get_running_loop()
    # SIGTERM is what `docker stop` and ECS send. Both signals mean "stop fetching,
    # finish what you have"; the grace period bounds how long that takes.
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.request_stop)
    try:
        await worker.run()
    finally:
        await redis.aclose()
    return worker


@app.command()
def worker(
    handlers: Annotated[
        str, typer.Option(help="Handler registry to load, as module:attribute.")
    ] = "ftq.handlers:registry",
    queue: Annotated[str | None, typer.Option(help="Overrides FTQ_QUEUE.")] = None,
    concurrency: Annotated[int | None, typer.Option(help="Overrides FTQ_CONCURRENCY.")] = None,
) -> None:
    """Run a worker process until SIGTERM/SIGINT."""
    settings = _settings(queue=queue, concurrency=concurrency)
    logs.configure(settings.log_level, settings.log_format)
    finished = asyncio.run(run_worker(settings, _load_registry(handlers)))
    stuck = finished.threads_still_running()
    if stuck:
        # Handler threads that timed out or were abandoned and are still running. A
        # thread can't be killed, and the interpreter would wait for them at exit, maybe
        # forever, so the grace period wouldn't bound shutdown. Everything the queue
        # needs is already in Redis (their jobs were retried or are in the PEL), so exit
        # without waiting (ADR-030). This is also what `docker stop`'s SIGKILL would do.
        logging.getLogger("ftq.cli").warning(
            "exiting with %d handler thread(s) still running; they are killed with the process",
            stuck,
        )
        logging.shutdown()
        os._exit(0)


@app.command()
def enqueue(
    job_type: Annotated[str, typer.Argument(help="Handler type, e.g. send_email.")],
    payload: Annotated[str, typer.Option(help="Job payload as a JSON object.")] = "{}",
    idempotency_key: Annotated[
        str | None, typer.Option(help="Dedup key: a repeat returns the original job_id.")
    ] = None,
    queue: Annotated[str | None, typer.Option(help="Overrides FTQ_QUEUE.")] = None,
) -> None:
    """Enqueue one job and print its job_id."""
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise typer.BadParameter("payload must be a JSON object")
    settings = _settings(queue=queue)

    async def _enqueue() -> str:
        redis = make_redis(settings)
        try:
            return await Client(redis, settings).enqueue(
                job_type, parsed, idempotency_key=idempotency_key
            )
        finally:
            await redis.aclose()

    try:
        typer.echo(asyncio.run(_enqueue()))
    except QueueFull as exc:
        typer.echo(f"not enqueued: {exc}", err=True)
        raise typer.Exit(2) from None


@app.command()
def stats(
    queue: Annotated[str | None, typer.Option(help="Overrides FTQ_QUEUE.")] = None,
) -> None:
    """Print the queue's depth, in-flight, delayed, DLQ size, consumers, and counters (JSON)."""
    settings = _settings(queue=queue)
    typer.echo(json.dumps(_with_redis(settings, lambda r: snapshot(r, settings)), indent=2))


@app.command()
def bench(
    jobs: Annotated[int, typer.Option(help="How many jobs to enqueue.")] = 10_000,
    job_type: Annotated[str, typer.Option("--type", help="Handler type.")] = "send_email",
    payload: Annotated[str, typer.Option(help="Payload of every job, JSON.")] = "{}",
    batch: Annotated[int, typer.Option(help="Jobs per pipelined enqueue_many.")] = 500,
    timeout: Annotated[float, typer.Option(help="Give up waiting after this many s.")] = 600.0,
    queue: Annotated[str | None, typer.Option(help="Overrides FTQ_QUEUE.")] = None,
) -> None:
    """Enqueue N jobs, wait until workers commit them all, report rates + exactly-once check.

    Uses block-mode backpressure, so a run larger than the high watermark throttles
    instead of failing. Exits 1 if any job is missing or has more than one result.
    """
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise typer.BadParameter("payload must be a JSON object")
    settings = _settings(queue=queue, backpressure_mode="block")
    report = _with_redis(
        settings, lambda r: run_bench(r, settings, jobs, job_type, parsed, batch, timeout)
    )
    typer.echo(format_report(report))
    if report["missing"] or report["duplicate_results"]:
        raise typer.Exit(1)


def _with_redis[T](settings: Settings, fn: Callable[[aioredis.Redis], Awaitable[T]]) -> T:
    async def _run() -> T:
        redis = make_redis(settings)
        try:
            return await fn(redis)
        finally:
            await redis.aclose()

    return asyncio.run(_run())


@dlq_app.command("list")
def dlq_list(
    limit: Annotated[int, typer.Option(help="Show at most this many (oldest first).")] = 100,
    queue: Annotated[str | None, typer.Option(help="Overrides FTQ_QUEUE.")] = None,
) -> None:
    """Print DLQ entries as JSON lines: job_id, type, reason, error, attempts, deliveries."""
    settings = _settings(queue=queue)
    dead = _with_redis(settings, lambda r: dlq.list_dead(r, Keys(settings.queue), limit))
    for job in dead:
        typer.echo(json.dumps(dataclasses.asdict(job)))


@dlq_app.command("requeue")
def dlq_requeue(
    job_ids: Annotated[list[str] | None, typer.Argument(help="job_id(s) to requeue.")] = None,
    all_: Annotated[bool, typer.Option("--all", help="Requeue every DLQ entry.")] = False,
    queue: Annotated[str | None, typer.Option(help="Overrides FTQ_QUEUE.")] = None,
) -> None:
    """Put DEAD jobs back on the queue with a fresh set of attempts (same job_id)."""
    if all_ == bool(job_ids):
        raise typer.BadParameter("give job_id(s) or --all, not both or neither")
    settings = _settings(queue=queue)
    keys = Keys(settings.queue)

    async def _requeue(r: aioredis.Redis) -> int:
        if all_:
            return await dlq.requeue_all(r, keys)
        count = 0
        for job_id in job_ids or []:
            if await dlq.requeue(r, keys, job_id):
                count += 1
            else:
                typer.echo(f"{job_id}: not DEAD, skipped", err=True)
        return count

    typer.echo(f"requeued {_with_redis(settings, _requeue)}")
