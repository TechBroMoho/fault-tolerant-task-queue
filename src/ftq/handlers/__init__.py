"""Built-in example handlers, exposed as `registry` (the worker's default `--handlers`).

- send_email: simulated I/O; the "send" goes through the EffectLedger.
- cpu_task: CPU-bound hashing, for benchmarks.

Phase 2/4 add flaky, poison, and crashy handlers for the reliability and chaos tests.
"""

from ftq.handlers.cpu_task import cpu_task
from ftq.handlers.send_email import send_email
from ftq.registry import Registry

registry = Registry()
registry.register("send_email")(send_email)
registry.register("cpu_task")(cpu_task)
