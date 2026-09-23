# AWS results (Phase 7 and Phase 8)

Raw reports from `deploy/bench.py` (`make aws-bench`) on ECS/EC2 in us-west-2, layout
L2' (ADR-047/048): Redis on 1 m7i-flex.large, 2 loadgen hosts and 6 worker hosts on
c7i-flex.large. Account ids are scrubbed.

- **Provenance:** points 1–3 (scaling w01, w02, w04) came from an earlier session
  (Phase 8 part 1, 2026-09-23 15:43–16:01 UTC, code d853d21). Every other point except
  w12_block is from part 2 (19:12–20:32 UTC, code 7510107). The worker and loadgen code is identical
  between the two (`git diff d853d21 7510107 -- src bench docker pyproject.toml uv.lock`
  is empty). Each report's `meta.git` and `meta.date_utc` say which.
- **Recovered reports** (`meta.recovered`): scaling/w02 and backpressure/w12_reject were
  read back from their own CloudWatch streams after the driver failed to (ADR-048).
  Same runs, not reruns. w12_reject has no `services.json`: the driver lost it in the
  crash.
- **backpressure/w12_block came from a separate session** (Phase 8 part 3, 2026-09-23
  20:52–21:00 UTC, code 3d43177), after part 2's attempt crashed before measuring
  anything. The loadgen and worker code is the same (`git diff 7510107 3d43177 -- src
  bench/loadgen.py docker pyproject.toml uv.lock` is empty).
- **12 running workers:** `evidence/describe-services-12-workers.json`.
- **Charts and table:** `scaling.png`, `backpressure.png` and `summary.md` (`python -m
  bench.plot --aws`). Methodology and every claim: `docs/RESULTS.md`.
- `phase7/`: the Phase 7 smoke test and loadgen probes.
