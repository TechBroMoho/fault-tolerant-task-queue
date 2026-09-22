"""send_email: an I/O-bound job with an externally visible side effect."""

import asyncio
from typing import Any

from ftq.registry import JobContext


async def send_email(ctx: JobContext) -> dict[str, Any]:
    """Payload: `to` (str), optional `latency_ms` (int) to simulate a slow mail API.

    The effect key is derived from job_id, which every redelivery and every duplicate
    entry of this job shares, so the email is "sent" at most once (ADR-019).
    """
    to = str(ctx.job.payload.get("to", "user@example.com"))
    latency_ms = int(ctx.job.payload.get("latency_ms", 0))
    if latency_ms > 0:
        await asyncio.sleep(latency_ms / 1000)
    sent = await ctx.ledger.apply(f"send_email:{ctx.job.job_id}", to=to)
    return {"to": to, "sent_now": sent}
