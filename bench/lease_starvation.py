"""Measure how close a long CPU-bound job gets to losing its lease (ADR-028).

Runs a real `ftq worker` subprocess with a 1 s lease and samples the job's PEL idle time
from this process, the same method as tests/integration/test_blocking_handlers.py:
`cpu_task` in the process pool vs. equivalent work blocking the event loop. Needs Redis
(`make up`). Reproduce: `uv run python bench/lease_starvation.py`
"""

import asyncio
import secrets
import signal
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root: reuse the test helpers
from tests.integration.helpers import hash_of, start_worker_process, wait_for, watching_lease

from ftq.client import Client
from ftq.config import Settings, make_redis
from ftq.keys import Keys

ENV = {
    "FTQ_VISIBILITY_TIMEOUT": "1.0",
    "FTQ_HEARTBEAT_INTERVAL": "0.2",
    "FTQ_REAP_INTERVAL": "0.05",
}


async def once(handlers: str, job_type: str, payload: dict[str, Any]) -> tuple[int, int, int]:
    s = Settings(queue=f"measure-{secrets.token_hex(4)}", block_ms=100, done_ttl_seconds=0)
    keys = Keys(s.queue)
    r = make_redis(s)
    proc, _ = await start_worker_process(s, handlers=handlers, env=ENV)
    try:
        job_id = await Client(r, s).enqueue(job_type, payload)
        async with watching_lease(r, keys.stream, s.group) as w:

            async def done() -> bool:
                return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

            await wait_for(done, within=60)
        results: Any = await r.xrange(keys.results)
        [(_i, e)] = results
        return w.max_idle_ms, w.samples, int(e["finished_at_ms"]) - int(e["enqueued_at_ms"])
    finally:
        proc.send_signal(signal.SIGTERM)
        await proc.communicate()
        doomed = [k async for k in r.scan_iter(match=f"{keys.prefix}:*")]
        if doomed:
            await r.delete(*doomed)
        await r.aclose()


async def main() -> None:
    runs: list[tuple[str, str, str, dict[str, Any]]] = [
        ("cpu_task, process pool", "ftq.handlers:registry", "cpu_task", {"rounds": 12_000_000}),
        (
            "hog_on_loop (control) ",
            "tests.integration.blocking_handlers:registry",
            "hog_on_loop",
            {"seconds": 2.5},
        ),
    ]
    for label, h, t, p in runs:
        for i in range(3):
            idle, n, run = await once(h, t, p)
            print(
                f"{label} run {i + 1}: job ran {run} ms, "
                f"max PEL idle {idle} ms (lease 1000), {n} samples"
            )


asyncio.run(main())
