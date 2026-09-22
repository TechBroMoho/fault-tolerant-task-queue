"""Command-line interface: `ftq worker`, `ftq enqueue`.

Configuration comes from `FTQ_*` env vars (config.py); flags override a few of them.
`stats`, `dlq`, and `bench` arrive in later phases.
"""

import asyncio
import importlib
import json
import logging
import signal
from typing import Annotated, Any

import typer

from ftq.client import Client
from ftq.config import Settings, make_redis
from ftq.registry import Registry
from ftq.worker import Worker

app = typer.Typer(no_args_is_help=True, add_completion=False)


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


def _configure_logging(level: str) -> None:
    # INFO covers lifecycle events only; per-job lines are DEBUG, so the default level
    # never logs every job (SPEC Phase 3). JSON logs arrive in Phase 3.
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def run_worker(settings: Settings, registry: Registry) -> None:
    """Run one worker until SIGTERM/SIGINT, then drain gracefully."""
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
    _configure_logging(settings.log_level)
    asyncio.run(run_worker(settings, _load_registry(handlers)))


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

    typer.echo(asyncio.run(_enqueue()))
