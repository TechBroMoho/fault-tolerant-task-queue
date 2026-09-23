# PROGRESS

## Status

- **Current phase:** Phase 2 (reliability: leases + reaper, heartbeats, retries with backoff,
  delayed scheduler, DLQ + CLI, blocking handlers in pools, safe consumer pruning): **complete**,
  at the gate awaiting review.
- **Next:** Phase 3 (multiple worker processes, batching/pipelining measured before and after,
  backpressure with hysteresis, `ftq stats`, JSON logs). Starts on Mohammed's go-ahead.
- **Repo:** https://github.com/TechBroMoho/fault-tolerant-task-queue (public, default branch `main`, created 2026-09-22).
- **AWS:** nothing created. Spend to date: $0.

## Phase log

### Phase 2: Reliability (2026-09-22)

**Built**
- **Ownership-checked transitions** (ADR-024), one Lua script each, all starting with a
  one-id `XPENDING` owner check: `heartbeat.lua` (`XCLAIM … JUSTID` to self), `retry.lua`
  (to the delayed zset with the next attempt), and `dead.lua` (to the DLQ, terminal state
  DEAD). A non-owner gets `LEASE_LOST` and nothing changes. An owner holding a copy of an
  already-terminal job gets `TERMINAL`: the copy is dropped and counted as a duplicate.
  `transitions.py` wraps them with typed outcomes.
- **Reaper** (ADR-023): `reclaim.lua` = `XAUTOCLAIM` + each entry's delivery count + the
  `reclaimed` counter in one step. It runs inside the fetch loop and claims at most the
  free slots, so reclaimed and fresh jobs share one in-flight cap.
- **Heartbeats** (ADR-025): one task per in-flight job, every `heartbeat_interval` (config
  requires ≤ lease/2), `heartbeat=False` opt-out per handler. After `LEASE_LOST` a worker
  stops heartbeating and lets the handler finish.
- **Retries** (ADR-026): full-jitter backoff (`backoff.py`); `schedule.lua` atomically moves
  due retries back to the stream; every worker runs it from a maintenance loop.
- **DLQ** (ADR-027): four reasons (max_attempts, max_deliveries, malformed, unknown_type);
  the entry keeps the original job fields plus `dlq_*` metadata. Late success replaces DEAD
  and deletes the DLQ entry (commit.lua, ADR-009). `requeue.lua` + `ftq dlq list` /
  `ftq dlq requeue <job_id>… | --all` (same job_id, attempt 0).
- **Requirement 1: blocking/CPU-bound handlers can't starve heartbeats** (ADR-028):
  `register_sync(type, pool="thread" | "process")`. The process pool uses `spawn`, its
  children ignore SIGINT, a broken pool is replaced, and children are terminated when the
  grace period runs out. `cpu_task` now runs in the process pool. `ftq/__main__.py` got the
  `if __name__ == "__main__"` guard that spawn needs.
- **Requirement 2: consumer cleanup never deletes a consumer with pending entries**
  (ADR-029): `prune_consumers.lua` deletes an idle consumer only if `XPENDING` filtered by
  that consumer is empty, checked and deleted atomically. It runs from the maintenance
  loop at startup and every `consumer_prune_interval`.
- Handlers `flaky` (deterministic `fail_times`), `poison`, `crashy` (`os._exit`), and
  `slow` (no heartbeats). Eleven new settings; the full ADR-010 TTL relation is now
  validated (`Settings.job_lifetime_bound`, ≥ 10×). New counters: retried, scheduled, dead,
  late_successes, requeued, reclaimed, heartbeats, lease_lost, consumers_pruned.
- 64 new tests (103 total: 52 integration against real Redis, 51 unit):
  - Script level, every transition: owner / non-owner / re-send; two workers can't
    heartbeat-steal a lease (5 alternations, owner stays b, delivery count unchanged);
    DEAD never replaces SUCCEEDED; late success; 10 racing schedulers over 200 retries
    move each exactly once; reclaim respects `count`.
  - Worker level: a killed worker's job is reclaimed and completed once; flaky succeeds on
    attempt 2; poison goes to the DLQ with `dlq_attempts` = 3 = max_attempts; malformed and
    unknown types go straight to the DLQ; a 2 s heartbeating job under a 0.5 s lease is
    never reclaimed with a second reaper armed; a non-heartbeating job IS reclaimed and
    its effect still happens once; the **stale-worker test** in all three variants (retry
    refused, DLQ move refused, commit suppressed), each ending with one terminal state,
    one result, and one effect; heartbeats stop after a lost lease.
  - Real subprocesses: a `crashy` job kills two workers (exit 70 ×2), then the third worker
    sends it to the DLQ at delivery 3 > max_deliveries 2 without running it. A
    process-pool job past the grace period is terminated and SIGTERM exit stays under 5 s.
  - Requirement 1: a real `ftq worker` runs a ~2.6 s `cpu_task` under a 1 s lease while a
    second worker's reaper is armed: never reclaimed, sampled PEL idle stays below the
    lease. A control test with a loop-blocking handler shows the lease lapsing. A
    thread-pool blocking-I/O job keeps its lease too.
  - Requirement 2: a consumer owning a pending entry survives repeated prune passes (entry,
    owner, and delivery count intact), then is reclaimed and completed, and only then is the
    consumer pruned. A running worker prunes 3 empty idle consumers while a crashed one's
    job stays in the PEL across ~10 more passes.
  - DLQ via the real CLI: a job that charged a card and then failed is requeued once
    "fixed": 3 runs, `effects_applied` = 1.

**Acceptance evidence** (Redis 8.8.3 via `make up`):

```
$ make check > check.log 2>&1; echo "make check exit=$?"
make check exit=0
  49 files already formatted / All checks passed! / Success: no issues found in 43 source files
  ============================= 103 passed in 28.17s =============================
                                        # SPEC Phase 2: tests run in < 60 s total

$ for i in $(seq 1 10); do uv run pytest -q ...; done      # flakiness check
103 passed (x10, 27.50–32.74 s, every exit 0)

$ docker compose stop redis && uv run pytest -q; echo $?    # ADR-012 negative check
51 passed, 52 errors in 3.93s                              # every integration test: "Redis not reachable"
pytest exit (redis down)=1
```

Mutation checks. Each bug was planted by a script (one at a time), the relevant tests
were run, the file was restored byte for byte (sha256-verified), and `git diff -- src tests`
was empty afterwards. Every bug is caught:

```
retry.lua ownership check bypassed          -> 3 failed (stale_worker[raising_retry], ...)
dead.lua ownership check bypassed           -> 3 failed (stale_worker[raising_dlq], ...)
heartbeat.lua ownership check bypassed      -> 3 failed (lease ping-pong, heartbeats_stop_after_lease_lost, ...)
heartbeat.lua XCLAIM without JUSTID         -> 3 failed (delivery count bumped; long job "reclaimed")
retry.lua terminal-state check bypassed     -> 1 failed (retry_of_a_job_that_already_succeeded)
commit.lua late-success branch disabled     -> 1 failed (late_success_replaces_dead)
prune_consumers.lua pending guard removed   -> 2 failed (consumer_with_pending_work_is_never_deleted, ...)
sync handlers run on the event loop         -> 2 failed (process-pool cpu_task, thread-pool blocking_io)
max_deliveries check disabled               -> 1 failed (crash loop)
```

Requirement 1, measured (`uv run python bench/lease_starvation.py`, raw output in
`results/local/lease_starvation.txt`; 1 s lease, 0.2 s heartbeats):

```
cpu_task, process pool:      job ran 2594–2718 ms, max PEL idle 199–206 ms   (3 runs)
hog_on_loop (control):       job ran ~2501 ms,     max PEL idle 2484–2491 ms (3 runs)
```

Redis behaviours the design depends on were checked against 8.8.3 before building on them:
`XCLAIM … JUSTID` resets idle without touching the delivery count; `XAUTOCLAIM` increments
it and returns `[cursor, entries, deleted_ids]`; `XPENDING` (one-id range and
consumer-filtered), `XINFO CONSUMERS`, and `cjson` all work inside Lua.

**Open issues**
- A handler that hangs forever on a healthy worker keeps its lease forever (heartbeats keep
  it alive): stuck in the PEL, not lost. No per-job timeout yet (ADR-025); decide by Phase 4.
- `max_deliveries` default 10 and `max_attempts` 5 are provisional. Phase 4 sizes
  `max_deliveries` against the fault schedule (ADR-008).
- Process-pool jobs pay pickling + IPC per job; unmeasured until Phase 6. An abandoned
  thread-pool handler can't be killed and delays exit until `docker stop`'s SIGKILL (ADR-028).
- Killing pool children on abandon uses `ProcessPoolExecutor._processes` (private in 3.12).
- Backpressure (Phase 3) must count the delayed set in queue depth (ADR-026).
- Carried from Phase 1: the results/effects logs are never trimmed (ADR-021).

### Phase 1: Core queue (2026-09-22)

**Built**
- Runtime deps (pinned in `uv.lock`): pydantic 2.13.5, pydantic-settings 2.15.0, typer 0.27.2,
  uuid-utils 1.0.0. `ftq` console script + `python -m ftq`.
- `config.py`: `Settings` from `FTQ_*` env vars, validated (queue name is hash-tag safe,
  `block_ms < socket_timeout`); `make_redis()` with explicit timeouts, health check, and a
  bounded jittered retry (ADR-006). The README config table is generated from it.
- `keys.py` (`ftq:{<queue>}:…`, ADR-018), `models.py` (`Job`, UUIDv7 ids generated by the client,
  flat stream encoding validated by pydantic).
- Lua (`src/ftq/scripts/`, commented line by line, `#!lua` for OOM-before-run, ADR-017):
  - `enqueue.lua`: optional idempotency `SET NX EX` + `XADD`, `enqueued_at_ms` from Redis `TIME`.
  - `commit.lua`: first-wins on `done.state == SUCCEEDED`. A duplicate is acked, deleted, and
    counted; a first commit writes the done hash + results log + ack + delete + `processed`.
  - `ledger.lua`: `SET NX` marker + append-only effects log.
- `client.py` (`enqueue`), `ledger.py` (`EffectLedger`), `metrics.py` (counters live in a hash and
  are incremented inside the scripts), `registry.py` (`Registry`, `JobContext`).
- `worker.py`: the fetch loop requests only as many entries as it has free slots (in-flight cap),
  creates the group at `0` + MKSTREAM, never cancels the blocking read, pauses and retries on
  fetch errors, and on SIGTERM/SIGINT stops fetching and drains within `shutdown_grace`, then
  abandons the rest to the PEL (ADR-022).
- Handlers: `send_email` (optional simulated latency; effect via the ledger keyed by job_id) and
  `cpu_task` (iterated SHA-256). CLI: `ftq worker`, `ftq enqueue`.
- 35 new tests (39 total): 15 integration (real Redis) + 24 unit. They cover
  enqueue → processed → result stored (enqueued before the group exists); idempotent enqueue
  returning the original id, plus TTL expiry; a forced redelivery of a done job (handler ran
  twice, ledger count stays 1, `duplicates_suppressed` = 1, results log = 1); commit re-send and
  second-holder commit at the script level; ledger NX; 300 jobs with concurrency 7, each
  completed exactly once; and a **real subprocess + real SIGTERM**: the in-flight job finishes
  and a job enqueued after "stopped fetching" is not taken; a job past the grace period is
  abandoned to the PEL (uncommitted, no effect).
- ADR-016 to ADR-022 added; ADR-006 accepted with concrete values; ADR-010 partly implemented.

**Acceptance evidence** (Redis 8.8.3 via `make up`):

```
$ make check > check.log 2>&1; echo "make check exit=$?"
make check exit=0
  34 files already formatted / All checks passed! / Success: no issues found in 29 source files
  collected 39 items
  ============================== 39 passed in 4.62s ==============================

$ for i in $(seq 1 10); do uv run pytest -q ...; done      # flakiness check
39 passed in 4.63s   (x10, 4.60–4.73 s, no warnings)
```

Mutation checks (manual, one at a time, scripts restored afterwards and verified with `git diff`).
Each bug is caught:

```
commit.lua duplicate check bypassed (`if false then`)  -> 3 failed, 12 passed (integration)
ledger.lua SET without NX                               -> 2 failed, 13 passed
commit.lua XDEL removed from the success path           -> 5 failed, 10 passed
```

Negative check (ADR-012), exit codes captured unpiped:

```
$ docker compose stop redis && uv run pytest -q; echo $?
24 passed, 15 errors in 1.12s      # every integration test errors "Redis not reachable"
pytest exit (redis down)=1
$ make up && uv run pytest -q; echo $?
39 passed in 4.72s
pytest exit (redis up)=0
```

Manual CLI run: `ftq worker` in the background, then `ftq enqueue send_email … --idempotency-key
signup-ada` twice (the same job_id came back both times) and `ftq enqueue cpu_task`. Stats:
`processed=2, effects_applied=1`, stream length 0, and the done hash held
`{"to":"ada@example.com","sent_now":true}`. SIGTERM → worker exit 0 (stop to "stopped fetching" took
~0.87 s, the in-progress 1 s `BLOCK`). `uv build --wheel` includes `ftq/scripts/*.lua` and the
entry point.

**Open issues** (all planned for Phase 2 unless noted)
- A handler exception, a malformed entry, or an unknown job type is logged and **left in the PEL**
  (stuck, not lost) until Phase 2 adds retry/DLQ and the reaper.
- The full ADR-010 TTL relation isn't validated yet; it needs the lease/attempt/backoff knobs.
- Idle consumer records accumulate in the group across worker restarts (ADR-022). Cleanup must
  only remove consumers with zero pending entries (Phase 2/3).
- `cpu_task` blocks the event loop; Phase 2 must make sure it can't starve heartbeats.
- The results/effects logs are never trimmed. Fine for chaos; a retention decision is needed
  for long benchmarks (Phase 6/8, ADR-021).

### Phase 0: Bootstrap and plan (2026-09-22)

**Built**
- Repo layout per SPEC §6: `src/ftq/` (package + `py.typed`), `tests/{unit,integration}/`,
  `chaos/`, `bench/`, `deploy/terraform/`, `docker/`, `results/{local,ci,aws}/`.
- `pyproject.toml` (uv, `uv_build` backend, Python `>=3.12,<3.13`; runtime dep `redis`; dev deps
  `pytest`, `pytest-asyncio`, `pytest-timeout`, `mypy`, `ruff`) and `uv.lock`.
  Resolved: Python 3.12.13, redis-py 8.1.0, pytest 9.1.1, pytest-asyncio 1.4.0,
  pytest-timeout 2.4.0, mypy 2.3.1, ruff 0.16.8.
- `Makefile`: `setup`, `fmt`, `fmt-check`, `lint`, `typecheck`, `test`, `check`, `up`, `down`.
  Stubs for `chaos`, `bench`, and `aws-*` exit non-zero ("not implemented yet (Phase N)"), so a
  stub can never look like a pass.
- `docker-compose.yml`: `redis:8.8.3` with `noeviction`, `maxmemory 2gb`, AOF `everysec`, port
  bound to `127.0.0.1` only, and a healthcheck (`make up` uses `--wait`).
- `.gitignore` (secrets, `.env*`, Terraform state/vars, venv, caches, logs, `.DS_Store`),
  MIT `LICENSE`, a minimal `README.md`.
- Tests: a unit smoke test (package import/version) and 3 integration tests against real Redis
  (ping; server version is 8.8.3; `maxmemory-policy` is `noeviction`).
- `docs/DECISIONS.md` ADR-001 to ADR-014 (Streams vs Lists, Python, ECS on EC2 vs Fargate,
  Redis 8.8.3 with classic commands only, Python 3.12, redis-py 8 client config and client-side
  job IDs, design gaps #5 to #10 from the kickoff review, local Redis config, moving the repo
  out of iCloud).

**Acceptance evidence** (run from `~/code/fault-tolerant-task-queue`, Docker 29.4.3):

```
$ make up
 Container ftq-redis-1 Healthy
$ docker compose ps
NAME          IMAGE         STATUS
ftq-redis-1   redis:8.8.3   Up 2 seconds (healthy)

$ make check
uv run ruff format --check .
11 files already formatted
uv run ruff check .
All checks passed!
uv run mypy
Success: no issues found in 7 source files
uv run pytest
platform darwin -- Python 3.12.13, pytest-9.1.1, pluggy-1.6.0
plugins: timeout-2.4.0, asyncio-1.4.0
collected 4 items
tests/integration/test_redis_ping.py ...                                 [ 75%]
tests/unit/test_package.py .                                             [100%]
============================== 4 passed in 0.16s ===============================
make check exit=0
```

Negative check (ADR-012: with Redis down the suite must fail, not skip):

```
$ docker compose stop redis && uv run pytest -q
ERROR tests/integration/test_redis_ping.py::test_ping - Failed: Redis not rea...
ERROR tests/integration/test_redis_ping.py::test_pinned_redis_version - Faile...
ERROR tests/integration/test_redis_ping.py::test_noeviction_policy - Failed: ...
1 passed, 3 errors in 0.26s
pytest exit (redis down)=1
$ make up && uv run pytest -q
pytest exit (redis up)=0
```

**Open issues**
- None blocking Phase 1. Not needed until Phase 7: AWS CLI v2, Terraform, a named AWS profile,
  and the account's sign-up date (the Free plan closes the account 6 months after sign-up).
  The GitHub repo (needed for Phase 5 CI) now exists: https://github.com/TechBroMoho/fault-tolerant-task-queue.
- Post-gate review decisions: skip the HTTP enqueue endpoint (ADR-015); author name confirmed as
  Mohammed Jasim.

## Spend log

| Date | Resources created / destroyed | Duration | Est. cost |
|---|---|---|---|
| (none yet) | | | $0 |

## Things that went wrong

- **2026-09-22: `import ftq` failed, caused by iCloud.** The repo started in `~/Desktop`, which
  iCloud syncs. iCloud set the macOS hidden flag on `.venv`, and CPython 3.12.13 skips hidden
  `.pth` files, so uv's editable install was silently ignored (`ModuleNotFoundError: No module
  named 'ftq'`). We diagnosed it with `ls -lO` (every venv file showed `hidden`). A
  `.venv.nosync` symlink worked as a stopgap. The durable fix was moving the repo to `~/code/`
  (ADR-014).
- **2026-09-22: exit code not captured.** The shell is zsh, so `${PIPESTATUS[0]}` printed
  nothing when capturing pytest's exit code through a pipe. We re-ran without the pipe to get
  the real code. Lesson: capture exit codes with `; echo $?` on unpiped commands, or
  `set -o pipefail`.
- **2026-09-22: ADR-019 initially misdescribed Stripe.** The draft said Stripe, like us, doesn't
  compare payloads on a reused idempotency key. It does: Stripe rejects a reused key sent with
  different parameters. We caught this in review before committing and corrected the ADR
  (ours returns the original job_id without comparing; a payload-hash check is future work).
- **2026-09-22: repeated the piped-exit-code mistake.** The first `make check | tail; echo $?`
  printed tail's exit code. We re-ran it as `make check > log; echo $?` for the recorded evidence.
  The Phase 0 lesson still applies.

- **2026-09-22 (Phase 2): four test-harness races, no system bugs.** Every first-run failure
  in Phase 2 was the *test's* fault, and each fix made the test more precise rather than
  looser:
  (1) a PEL poll ran before the in-process worker had created the consumer group (`NOGROUP`):
  `running_worker` now creates the group before it yields;
  (2) a poll unpacked `[p] = pending(...)` before the worker had fetched the job: it now
  waits for exactly one entry owned by A;
  (3) the heartbeat-lease-lost test simulated worker B as a bare consumer name, which never
  heartbeats, so A's *reaper* correctly reclaimed the entry from "B" after 0.5 s. A now
  has one busy slot, so only its heartbeat could take the entry back;
  (4) the pruning test expected the crashed consumer to still exist after the live worker
  reclaimed its job, but the live worker had already, correctly, pruned it. The test now
  asserts that sequence end to end.
  Also caught in review before committing: a "watch across many prune passes" loop that
  took ~3 ms and so spanned no passes; its checks are now spaced one interval apart.
- **2026-09-22 (Phase 2): spawn would have re-run the CLI.** `python -m ftq` makes
  `ftq/__main__.py` the main module, and the process pool's `spawn` start method re-imports
  it in every child. Without an `if __name__ == "__main__"` guard, each pool child would have
  started its own `ftq worker`. Caught while designing the pool (ADR-028), before any test ran;
  the process-pool subprocess tests exercise the fixed path.
