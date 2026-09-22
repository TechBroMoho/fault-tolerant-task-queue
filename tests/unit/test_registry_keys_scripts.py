"""Registry behavior, key naming, and invariants of the Lua sources (pure logic)."""

from importlib.resources import files

import pytest

from ftq.handlers import registry as builtin
from ftq.keys import Keys
from ftq.lua import script_source
from ftq.registry import JobContext, Registry


def test_duplicate_registration_rejected() -> None:
    registry = Registry()

    async def handler(ctx: JobContext) -> None:
        return None

    registry.register("a")(handler)
    with pytest.raises(ValueError, match="already registered"):
        registry.register("a")(handler)
    assert registry.get("a") is handler
    assert registry.get("missing") is None


def test_builtin_handlers() -> None:
    assert builtin.types() == ["cpu_task", "send_email"]


def test_keys_share_one_hash_tag() -> None:
    k = Keys("emails")
    all_keys = [k.stream, k.results, k.effects, k.stats, k.done("j"), k.idem("i"), k.ledger("l")]
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
