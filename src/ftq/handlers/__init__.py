"""Built-in example handlers, exposed as `registry` (the worker's default `--handlers`).

- send_email: simulated I/O; the "send" goes through the EffectLedger.
- cpu_task: CPU-bound hashing in the process pool, for benchmarks (ADR-028).
- flaky, poison, crashy, slow, hang, hang_thread, hang_process: deliberate failures for
  the reliability tests and the chaos mix (faults.py). The hang handlers have their own
  short timeout, HANG_TIMEOUT.
"""

from ftq.handlers.cpu_task import cpu_task
from ftq.handlers.faults import (
    HANG_TIMEOUT,
    crashy,
    flaky,
    hang,
    hang_process,
    hang_thread,
    poison,
    slow,
)
from ftq.handlers.send_email import send_email
from ftq.registry import Registry

registry = Registry()
registry.register("send_email")(send_email)
registry.register_sync("cpu_task", pool="process")(cpu_task)
registry.register("flaky")(flaky)
registry.register("poison")(poison)
registry.register("crashy")(crashy)
registry.register("slow", heartbeat=False)(slow)
registry.register("hang", timeout=HANG_TIMEOUT)(hang)
registry.register_sync("hang_thread", pool="thread", timeout=HANG_TIMEOUT)(hang_thread)
registry.register_sync("hang_process", pool="process", timeout=HANG_TIMEOUT)(hang_process)
