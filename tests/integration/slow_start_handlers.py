"""Process-pool handlers in a module that takes START_S to import inside a pool child.

A pool child imports a handler's module on first use (the function is pickled by its
import path). This module makes that start-up cost deterministic, standing in for a
`spawn` child on a starved machine: CI's 4-vCPU runner, where pool resets cascaded
(ADR-039). The parent imports it instantly, so only pool start-up is slow.
"""

import multiprocessing
import os
import time
from typing import Any

from ftq.models import Job
from ftq.registry import Registry

from . import blocking_handlers

START_S = 2.0
# Set by a test to stagger the children: the k-th child to start (0-based) takes
# START_S * (k + 1), so a pool's children become ready at different times.
STAGGER_DIR_ENV = "FTQ_TEST_STAGGER_DIR"


def _start_up() -> None:
    stagger_dir = os.environ.get(STAGGER_DIR_ENV)
    k = 0
    if stagger_dir:
        while True:  # claim the lowest free index; O_EXCL makes the claim atomic
            try:
                os.close(os.open(os.path.join(stagger_dir, str(k)), os.O_CREAT | os.O_EXCL))
                break
            except FileExistsError:
                k += 1
    time.sleep(START_S * (k + 1))


if multiprocessing.parent_process() is not None:  # a pool child, not the test process
    _start_up()

registry = Registry()


def instant(job: Job) -> dict[str, Any]:
    """No work at all: under a 1 s timeout it only fails if start-up is on the clock."""
    return {"attempt": job.attempt}


registry.register_sync("instant", pool="process", timeout=1.0)(instant)


def hang_first_attempt(job: Job) -> dict[str, Any]:
    return blocking_handlers.hang_first_attempt_in_process(job)


registry.register_sync("hang_first_attempt", pool="process", timeout=1.5)(hang_first_attempt)


def spin(job: Job) -> dict[str, Any]:
    return blocking_handlers.spin_in_process(job)


registry.register_sync("spin_3s_timeout", pool="process", timeout=3.0)(spin)
registry.register_sync("spin_2s_timeout", pool="process", timeout=2.0)(spin)
