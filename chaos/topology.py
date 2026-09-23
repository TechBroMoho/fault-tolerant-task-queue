"""The chaos topology and the Docker commands that drive it.

    producer, verifier (host) ─────────────────────────────► redis
    worker-i (container) ──► toxiproxy:2000i (proxy redis_i) ──► redis

One Toxiproxy container hosts one proxy per worker, so a network fault can hit a single
worker (a partial partition) instead of all of Redis. The producer and the verifier
connect to Redis directly, so "accepted" is never ambiguous: an enqueue reply can't be
lost by a fault (SPEC §7, ADR-011).

`docker compose --scale` replicas can't each get their own proxy address, so the Compose
file is generated with one named service per worker (ADR-011). It is JSON, which Compose
reads as YAML, so no YAML dependency is needed.
"""

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path

PROJECT = "ftq-chaos"
REDIS_IMAGE = "redis:8.8.3"  # the same pin as docker-compose.yml (ADR-004)
TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0"
WORKER_IMAGE = "ftq-worker:local"
# Host ports, loopback only. Not the dev stack's 6379, so both can run side by side.
REDIS_PORT = 6390
TOXIPROXY_API_PORT = 8474
PROXY_BASE_PORT = 20000  # worker i's proxy listens on toxiproxy:(20000 + i)


def worker_name(i: int) -> str:
    return f"{PROJECT}-worker-{i}"


def proxy_name(i: int) -> str:
    return f"redis_{i}"


def compose_spec(workers: int, worker_env: dict[str, str], seed: int) -> dict[str, object]:
    """The Compose project: Redis, Toxiproxy, and `workers` named worker services."""
    services: dict[str, object] = {
        "redis": {
            "image": REDIS_IMAGE,
            "container_name": f"{PROJECT}-redis",
            # Same durability and eviction settings as the dev stack (ADR-013): AOF
            # everysec, and noeviction so a full Redis fails loudly instead of silently
            # dropping idempotency keys. A bigger cap: a 1M-job run keeps every done key.
            "command": [
                "redis-server",
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--maxmemory",
                "3gb",
                "--maxmemory-policy",
                "noeviction",
            ],
            "ports": [f"127.0.0.1:{REDIS_PORT}:6379"],
            "healthcheck": {
                "test": ["CMD", "redis-cli", "ping"],
                "interval": "1s",
                "timeout": "2s",
                "retries": 30,
            },
        },
        "toxiproxy": {
            "image": TOXIPROXY_IMAGE,
            "container_name": f"{PROJECT}-toxiproxy",
            # -seed makes the toxics' own randomness (latency jitter) reproducible.
            "command": ["-host=0.0.0.0", f"-seed={seed}"],
            "ports": [f"127.0.0.1:{TOXIPROXY_API_PORT}:8474"],
            "depends_on": {"redis": {"condition": "service_healthy"}},
        },
    }
    for i in range(1, workers + 1):
        services[f"worker-{i}"] = {
            "image": WORKER_IMAGE,
            "container_name": worker_name(i),
            "environment": {
                **worker_env,
                "FTQ_REDIS_URL": f"redis://toxiproxy:{PROXY_BASE_PORT + i}/0",
            },
            # No restart policy: the orchestrator's supervisor restarts crashed workers
            # itself, so it can count them and keep a killed worker down on purpose.
            "restart": "no",
            # Longer than FTQ_SHUTDOWN_GRACE, so the final stop is a real drain.
            "stop_grace_period": "20s",
            "depends_on": ["toxiproxy"],
        }
    return {"name": PROJECT, "services": services}


def write_compose(path: Path, workers: int, worker_env: dict[str, str], seed: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(compose_spec(workers, worker_env, seed), indent=2) + "\n")


@dataclass(frozen=True)
class Result:
    code: int
    out: str
    err: str


async def run(*args: str, check: bool = True, within: float = 120) -> Result:
    """Run a command (docker ...) without blocking the event loop."""
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), within)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"timed out after {within}s: {' '.join(args)}") from None
    result = Result(proc.returncode or 0, out.decode(), err.decode())
    if check and result.code != 0:
        raise RuntimeError(f"{' '.join(args)} exited {result.code}: {result.err.strip()}")
    return result


class Stack:
    """`docker compose` for the generated project, plus per-container commands."""

    def __init__(self, compose_file: Path) -> None:
        self._compose = ["docker", "compose", "-f", str(compose_file), "-p", PROJECT]

    async def compose(self, *args: str, within: float = 300) -> Result:
        return await run(*self._compose, *args, within=within)

    async def down(self) -> None:
        """Remove every container and the data volume (a fresh Redis for each run)."""
        await self.compose("down", "-v", "--remove-orphans", within=120)

    async def worker_states(self) -> dict[str, tuple[str, int | None]]:
        """Worker container name -> (state, exit code if exited)."""
        res = await run(
            "docker",
            "ps",
            "-a",
            "--filter",
            f"name=^{PROJECT}-worker-",
            "--format",
            "{{.Names}}\t{{.State}}\t{{.Status}}",
        )
        states: dict[str, tuple[str, int | None]] = {}
        for line in res.out.splitlines():
            name, state, status = line.split("\t")
            code = None
            if state == "exited" and "(" in status:  # "Exited (70) 2 seconds ago"
                code = int(status.split("(", 1)[1].split(")", 1)[0])
            states[name] = (state, code)
        return states


async def build_worker_image(repo_root: Path) -> None:
    """Build the worker image from the working tree (docker/Dockerfile)."""
    await run(
        "docker",
        "build",
        "-q",
        "-f",
        str(repo_root / "docker" / "Dockerfile"),
        "-t",
        WORKER_IMAGE,
        str(repo_root),
        within=600,
    )
