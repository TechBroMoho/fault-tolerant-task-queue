"""Registry behavior, key naming, and invariants of the Lua sources (pure logic)."""

from importlib.resources import files

import pytest

from ftq.handlers import registry as builtin
from ftq.keys import Keys
from ftq.lua import script_source
from ftq.models import Job
from ftq.registry import AsyncSpec, JobContext, Registry, SyncSpec


def test_duplicate_registration_rejected() -> None:
    registry = Registry()

    async def handler(ctx: JobContext) -> None:
        return None

    registry.register("a")(handler)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("a")(handler)
    # A sync handler can't take an async handler's type either.
    with pytest.raises(ValueError, match="already registered"):
        registry.register_sync("a", pool="thread")(_module_level_sync)
    assert registry.get("a") == AsyncSpec(fn=handler, heartbeat=True)
    assert registry.get("missing") is None


def _module_level_sync(job: Job) -> None:
    return None


def test_sync_registration_and_process_pool_picklability_check() -> None:
    registry = Registry()
    registry.register_sync("t", pool="thread", heartbeat=False)(_module_level_sync)
    assert registry.get("t") == SyncSpec(fn=_module_level_sync, pool="thread", heartbeat=False)

    def nested(job: Job) -> None:
        return None

    # A process pool pickles the function by import path; a nested one can't be imported.
    with pytest.raises(ValueError, match="module-level"):
        registry.register_sync("p", pool="process")(nested)
    registry.register_sync("thread-ok", pool="thread")(nested)  # threads don't pickle


def test_builtin_handlers() -> None:
    assert builtin.types() == ["cpu_task", "crashy", "flaky", "poison", "send_email", "slow"]
    # cpu_task must not run on the event loop, or it starves heartbeats (ADR-028).
    cpu = builtin.get("cpu_task")
    assert isinstance(cpu, SyncSpec) and (cpu.pool, cpu.heartbeat) == ("process", True)
    # slow jobs must NOT heartbeat: their leases are meant to expire (ADR-007).
    slow = builtin.get("slow")
    assert slow is not None and slow.heartbeat is False


def test_keys_share_one_hash_tag() -> None:
    k = Keys("emails")
    all_keys = [
        k.stream,
        k.delayed,
        k.dead,
        k.results,
        k.effects,
        k.stats,
        k.done("j"),
        k.idem("i"),
        k.ledger("l"),
    ]
    # Redis Cluster hashes only the first {...}; all of a queue's keys must share it.
    assert all(key.startswith("ftq:{emails}:") for key in all_keys)
    assert k.done("j") == "ftq:{emails}:done:j"


@pytest.mark.parametrize(
    "name",
    sorted(
        p.name.removesuffix(".lua")
        for p in (files("ftq") / "scripts").iterdir()
        if p.name.endswith(".lua")
    ),
)
def test_scripts_start_with_shebang(name: str) -> None:
    # `#!lua` makes Redis check OOM before a script runs, so a full Redis rejects the
    # whole script instead of failing after some of its writes (ADR-017).
    assert script_source(name).startswith("#!lua\n")


def test_no_script_uses_xtrim() -> None:
    # XTRIM MAXLEN can delete unacked entries: data loss (ADR-016).
    for path in (files("ftq") / "scripts").iterdir():
        if path.name.endswith(".lua"):
            code = [ln.split("--")[0] for ln in path.read_text().splitlines()]
            assert "XTRIM" not in "".join(code).upper(), path.name


def _code_lines(name: str) -> list[str]:
    """A script's lines with comments stripped."""
    return [ln.split("--")[0] for ln in script_source(name).splitlines()]


_WRITES = ("XACK", "XDEL", "XADD", "XCLAIM", "ZADD", "HSET", "HINCRBY", "DEL", "EXPIRE")


@pytest.mark.parametrize("name", ["heartbeat", "retry", "dead"])
def test_ownership_checked_scripts_check_the_pel_before_any_write(name: str) -> None:
    """SPEC §4: every non-commit transition checks ownership first. A textual check can't
    prove the logic right (the integration tests do), but it catches a refactor that moves
    a write above the XPENDING check."""
    lines = _code_lines(name)
    check = next(i for i, ln in enumerate(lines) if "'XPENDING'" in ln)
    owner_test = next(i for i, ln in enumerate(lines) if "p[1][2] ~= ARGV[3]" in ln)
    assert check < owner_test
    for ln in lines[:check]:
        assert not any(f"'{w}'" in ln for w in _WRITES), (name, ln)


def test_prune_checks_pending_before_delconsumer() -> None:
    """XGROUP DELCONSUMER discards the consumer's pending entries (ADR-029)."""
    lines = _code_lines("prune_consumers")
    pending_check = next(i for i, ln in enumerate(lines) if "'XPENDING'" in ln)
    guard = next(i for i, ln in enumerate(lines) if "#owned == 0" in ln)
    delete = next(i for i, ln in enumerate(lines) if "'DELCONSUMER'" in ln)
    assert pending_check < guard < delete
