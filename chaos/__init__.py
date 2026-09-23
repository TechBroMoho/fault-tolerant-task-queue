"""The chaos harness (SPEC §7, Phase 4): break things on purpose, then prove nothing was
lost or duplicated.

- `mix`: the job mix (normal, flaky, slow, hang, poison, crashy, ...) and what each kind
  must end as.
- `topology`: the generated Compose topology (one Toxiproxy proxy per worker) and the
  Docker commands the orchestrator uses.
- `faults`: the seeded fault schedule (kills, pauses, network faults) and its executor.
- `verifier`: invariants I1-I5 over the append-only logs and terminal states.
- `run`: the orchestrator, `uv run python -m chaos.run` (or `make chaos`).
"""
