# PROGRESS

## Status

- **Current phase:** Phase 3 (multiple worker processes, backpressure with hysteresis,
  pipelined `enqueue_many`, `ftq stats`, JSON logs, **per-job timeouts**, the worker Docker
  image): **complete**, awaiting Mohammed's review.
- **Next:** Phase 4 (chaos harness: per-worker Toxiproxy, kills/pauses/network faults,
  verifier I1–I5, mutation tests, N=100,000 locally). Starts on Mohammed's go-ahead.
- **Repo:** https://github.com/TechBroMoho/fault-tolerant-task-queue (public, default branch `main`, created 2026-09-22).
- **AWS:** nothing created. Spend to date: $0.

## Phase log

### Phase 3: Concurrency, backpressure, observability, per-job timeouts (2026-09-22)

**Built**
- **Per-job timeout (Mohammed's requirement, ADR-030).** `FTQ_JOB_TIMEOUT` (default 300 s),
  overridable with `register(..., timeout=)` / `register_sync(..., timeout=)`. The worker
  waits on each run's future for at most the timeout, then fails the attempt with
  `HandlerTimeout` (retry, or DLQ at `max_attempts`; the `timeouts` counter is incremented
  inside retry.lua/dead.lua). Then it stops the run where it can:
  - async handlers are cancelled;
  - a process-pool job's pool is reset (every child SIGKILLed), and the bystander jobs
    are resubmitted at the same attempt;
  - a thread-pool job's thread becomes an orphan. Heartbeats stop, it keeps its slot until
    it returns, its late result is discarded (never sent to commit), and `ftq worker`
    hard-exits after the drain if orphaned threads are still alive.
  - A handler that swallows its cancellation is orphaned the same way.
  - Process-pool runs take one of `process_pool_size` permits first, so queueing for a
    child never counts toward the timeout.
  - ADR-010's lifetime bound now includes the timeout, and the worker refuses to start if
    a per-type timeout isn't covered by the TTL.
- **Backpressure (ADR-031).** The admission check is inside `enqueue.lua` (depth = `XLEN` +
  `ZCARD delayed`, atomic with the `XADD`), with a per-queue hysteresis flag.
  - `reject` raises `QueueFull`; `block` polls with jitter until the depth falls below
    `low_watermark` or `block_timeout` runs out.
  - An idempotent repeat is answered even when full.
  - This deviates from SPEC's cached client-side check, and the ADR explains why: exact,
    no extra round trip, and N producers can't overshoot.
  - New counters: `rejected`, `blocked`, `timeouts`.
- **`enqueue_many`**, one pipelined round trip. Measured before and after (ADR-032, below).
  Commit batching was measured and not built yet: the worker is CPU-bound.
- **`ftq stats`** (JSON: depth, in_flight, undelivered, delayed, dlq, consumers, full,
  watermarks, counters). **`ftq bench --jobs N`** enqueues N jobs in block mode, waits for
  every first commit, and checks exactly-once against the results log. `ftq enqueue`
  exits 2 on `QueueFull`.
- **JSON logs (ADR-033):** `logs.py` (formatter, a `ContextLogger` that merges `worker_id`
  with per-job `job_id`/`attempt`/`job_type`), `FTQ_LOG_FORMAT=json|text`. INFO =
  lifecycle only.
- **Multiple worker processes:** `docker/Dockerfile` (multi-stage uv build, `python:3.12.13-slim-bookworm`,
  non-root uid 10001, 255 MB), a Compose `worker` service behind a profile,
  `make up WORKERS=N`.
- **Test split (ADR-034):** `make test` / `make check` = fast set; `make test-all` /
  `make check-all` = everything (CI must run `check-all`). 5 existing tests that take over
  1 s were marked `slow` (plus 1 new one); none was changed otherwise.
- 26 new tests (139 total): 11 timeout, 7 backpressure, 1 multi-process, 7 unit. Two
  existing tests changed:
  - The ADR-010 bound test now asserts the new formula, and also that a longer timeout
    invalidates a TTL that was enough.
  - The keys test covers `:full`.

**Acceptance evidence** (Redis 8.8.3 via `make up`):

```
$ make check > check.log 2>&1; echo "make check exit=$?"
make check exit=0
  56 files already formatted / All checks passed! / Success: no issues found in 49 source files
  ===================== 119 passed, 20 deselected in 15.90s ======================

$ make check-all > checkall.log 2>&1; echo "make check-all exit=$?"
make check-all exit=0
  ============================= 139 passed in 51.68s =============================

$ for i in 1 2 3 4 5; do uv run pytest -q ...; done       # flakiness, full suite
exit 0 x5: 56.66 / 59.49 / 62.87 / 58.46 / 56.68 s          # why the split was needed

$ docker compose stop redis && uv run pytest -q -m "not slow"; echo $?   # ADR-012
58 passed, 20 deselected, 61 errors in 4.43s                 # pytest exit (redis down)=1
```

SPEC demo, `make up WORKERS=4` + `ftq bench --jobs 50000` (4 worker containers, concurrency
10 each, `send_email` with no latency; raw: `results/local/phase3_demo_bench.json`, rerun
`phase3_demo_bench_run2.json`, stats after the rerun `phase3_demo_stats.json`):

```
run 1: enqueue 35 696 jobs/s, drained in 2.68 s = 18 659 jobs/s; completed 50000, missing 0, duplicate_results 0
run 2: enqueue 38 382 jobs/s, drained in 2.51 s = 19 938 jobs/s; completed 50000, missing 0, duplicate_results 0
$ uv run ftq stats   (after run 2, fresh stack)
depth 0, in_flight 0, undelivered 0, delayed 0, dlq 0, consumers 4, full false,
processed 50000, effects_applied 50000, duplicates_suppressed 0, rejected 0, blocked 0
worker logs for run 2: 4 lines total, all INFO "started"   (per-job lines are DEBUG)
```

These are smoke numbers: a 2.5 s run whose drain overlaps the enqueue, one producer, on a
laptop. They are **not** the Phase 6 steady-state benchmark and shouldn't be quoted as
throughput.

Backpressure demo, same stack: `FTQ_HIGH_WATERMARK=2000 FTQ_LOW_WATERMARK=1500 ftq bench
--jobs 20000 --payload '{"latency_ms": 5}'` (block mode; raw:
`results/local/phase3_demo_backpressure.json`):

```
completed 20000, missing 0, duplicate_results 0; producer: accepted 20000, rejected 0,
blocked 5032 (jobs that had to wait), blocked_seconds 3.51; drain 4.26 s = 4 691 jobs/s
```

Batching measured before and after (`uv run python bench/pipelining.py`; raw:
`results/local/pipelining.{txt,json}`; 20 000 jobs, 3 runs, median):

```
enqueue() one at a time    3 957 jobs/s   x1.0
enqueue_many, batch 10    11 421          x2.9
enqueue_many, batch 100   20 910          x5.3   (range 15 817-22 211: noisy, see ADR-032)
enqueue_many, batch 500   47 615          x12.0
worker drain at concurrency 1 / 10 / 50: 1 114 / 3 859 / 7 641 jobs/s, worker CPU 0.27 / 0.62 / 0.93
```

Mutation checks: scripted, one bug at a time, each file restored and sha256-verified. The
relevant test file was run each time:

```
orphans don't hold a slot                         -> CAUGHT (orphaned_thread_keeps_its_slot)
no hard exit over stuck threads                   -> CAUGHT (sigterm_exits_promptly..., 10 s timeout)
process pool not reset on timeout                 -> CAUGHT (test hung; killed after 90 s)
bystanders fail instead of restarting             -> CAUGHT (pool_reset_restarts_bystanders)
timeout clock includes waiting for a pool child   -> CAUGHT (waiting_for_a_pool_child...)
wait for a cancelled async run to stop            -> CAUGHT (swallows_its_cancellation)
orphaned thread's late result gets committed      -> CAUGHT (duplicates_suppressed 1 != 0;
                                                     commit.lua suppressed it: still 1 result)
heartbeat keeps running after a timeout           -> CAUGHT (lease_lost 1 != 0)
a timeout doesn't count as an attempt             -> CAUGHT (always_hangs_ends_in_the_dlq)
retry.lua doesn't count timeouts                  -> CAUGHT
enqueue admits at depth == high                   -> CAUGHT
no hysteresis (opens below high)                  -> CAUGHT
delayed set not counted in depth                  -> CAUGHT
full check before the idempotency lookup          -> MISSED, test strengthened -> CAUGHT
blocked counted on every poll                     -> CAUGHT
in-script depth disabled (a stale client check)   -> CAUGHT (concurrent_producers_never_overshoot)
```

**Decisions worth Mohammed's review**
- ADR-031 deviates from SPEC §4's recommended design: the admission check is inside the
  script instead of a cached client-side check.
- ADR-030: `ftq worker` hard-exits (`os._exit(0)`) when orphaned threads are still alive
  after the drain. A timed-out thread keeps its slot. A process-pool timeout resets the
  whole pool, and its bystanders restart from scratch without losing an attempt.
- ADR-032: no worker-side commit batching yet. The default `concurrency` stays 10 (7.6K jobs/s
  at 50 vs 3.9K at 10 for one in-process worker) until Phase 6 tunes it.
- Logs are JSON by default (`FTQ_LOG_FORMAT=text` for terminals).

**Open issues**
- Batch-100 pipelined sends on this laptop sometimes take ~2× longer than the linear curve;
  unexplained (ADR-032). Re-check on Linux before quoting batch-size numbers.
- Hung threads accumulate until they return, and a worker whose slots are all held by
  orphans stops fetching (visible, by design, ADR-030). A restart clears it.
- The hysteresis flag only changes on an enqueue (ADR-031). `ftq stats` compensates.
- Carried: `max_deliveries` / `max_attempts` sizing (Phase 4, ADR-008); results/effects logs
  are never trimmed (ADR-021); the private `_processes` map (ADR-028, now also used by the
  pool reset); the network-fault paths still untested until Toxiproxy (Phase 4).
- CI (Phase 5) must run `make check-all`, not `make check`.

### Phase 2 review (2026-09-22)

A skeptical post-gate review of the Phase 2 work, asked for by Mohammed. The goal: find
correctness bugs, races between the retry/reclaim/commit paths, tests that can't fail,
and doc claims with no test behind them. Every finding below was confirmed by a failing
test or a surviving mutation before it was fixed.

**Bugs fixed**
- **`dlq requeue --all` never terminated while workers ran** (real bug). It paged forward
  until the DLQ was empty. A requeued poison job dies again within milliseconds and lands
  at the end of the DLQ, so the sweep chased it forever. A new test (20 poison jobs, a live
  worker, page size 1) timed out at 10 s before the fix. Now the sweep snapshots the DLQ's
  last id first and stops there (ADR-027).
- **The heartbeat margin was wrong** (config bug). The validator allowed
  `heartbeat_interval = lease / 2`, and ADR-025 said one lost beat "never" expires a lease.
  Beats land every `interval + RTT`, so after one lost beat the idle time reaches
  `2 × (interval + RTT)`, which is past the lease at exactly `/2`. Now it requires
  `≤ lease / 3`. The defaults (10 s / 30 s) already satisfied it.
- **A heartbeat in flight could land after the commit or retry** (a race, harmless to
  state). The task was cancelled but not awaited. The ownership check made a late beat a
  no-op `LEASE_LOST`, but it logged a false "lost the lease" warning and inflated the
  counter. It is now cancelled and awaited before any transition.
- **Only connection errors were handled around transitions.** A `ResponseError` from
  commit, retry, or DLQ moves escaped the job task ("Task exception was never retrieved").
  Now every `RedisError` is logged and the entry is left in the PEL.
- `lease_lost` is documented as an upper bound on real lease losses: a retry or DLQ move
  re-sent after a lost reply is refused and counted too.

**Races between retry, reclaim, and commit: checked, found safe.** Each case below was
reasoned through against the scripts, and each now has a test. Two of the tests are new:
the review first said all of them were "covered", but two were not.
- Stale commit after the reclaimer **committed**: suppressed (`test_stale_worker[committing]`).
- Stale retry or DLQ move after a reclaim: `LEASE_LOST`, nothing changes
  (`test_stale_worker[raising_*]` and the script-level stale tests).
- Stale commit after the reclaimer **scheduled a retry**: a's commit is the first-wins
  success; the retry then runs as a suppressed duplicate, and a failing copy is dropped
  (`TERMINAL`) rather than retried or dead-lettered
  (`test_stale_commit_after_the_reclaimer_scheduled_a_retry`, new).
- Stale commit after a **DLQ move**: a late success, which replaces DEAD and deletes the DLQ
  entry (`test_late_success_replaces_dead_and_removes_dlq_entry`).
- Stale commit after a DLQ move **and a requeue**: requeue cleared DEAD, so it's a plain
  first success, and the requeued copy is suppressed; one result
  (`test_stale_commit_after_the_job_was_dead_lettered_and_requeued`, new).
- Two copies of one job_id, one finished: the other's retry or DLQ move returns
  `TERMINAL` (`test_retry_of_a_job_that_already_succeeded_drops_the_copy`,
  `test_dead_never_replaces_succeeded`).
- A reaper claim racing the owner's commit: both are atomic scripts; whichever runs second
  sees the other's result (commit is first-wins; a claim of an acked entry finds nothing).
  This one is argued from atomicity, not tested as a race.
- A consumer deleted while blocked in `XREADGROUP`: impossible while the threshold exceeds
  `block_ms`, which the config enforces (`test_live_idle_workers_never_prune_each_other`).

**Tests that couldn't fail, now fixed** (see "Things that went wrong"): the Ctrl-C test
(it signalled before the pool child existed) and the idle-prune test (`inactive` is -1
until a consumer's first successful read). A test step that proved nothing (a thread-pool
job offered as evidence of process-pool replacement) was removed. The scheduler race
test is now bounded, so a regression fails in 10 s instead of hanging until pytest's 60 s
timeout. The malformed/unknown-type test now asserts "straight to the DLQ" (0 attempts,
1 delivery).

**Claims that had no test, now tested** (10 new tests in the review, counting the two race tests above; 113 total):
- Pool children ignore SIGINT: Ctrl-C drains a mid-job process-pool task.
- A dead pool child fails one attempt and the pool is replaced.
- The scheduler drains a backlog without sleeping between full batches.
- The worker's retry due time stays within the backoff bound.
- The reaper's cursor continues through a long PEL (`more` flag).
- Pending ids whose data was deleted are logged at ERROR.
- Two live idle workers never prune each other (Redis `idle` vs `inactive` semantics).
- `requeue --all` is bounded and skips orphan entries.

**A doc claim that was false.** "Spawn re-runs `__main__.py` in every child without a
guard": removing the guard broke nothing, and CPython's spawn skips package `__main__`
modules. It is corrected in ADR-028 and the code comment.

**Still untested (documented as such).** Heartbeat retry after a Redis error, fetch/reap
pauses on connection errors, and the "lost reply → re-send" behaviour of each script all
need network fault injection (Toxiproxy, Phase 4). Also untested: the worker-level use of
the reaper's `more` flag (the flag itself is tested) and the maintenance loop surviving
errors.

**Evidence**

```
$ make check > check.log 2>&1; echo "make check exit=$?"
make check exit=0
  49 files already formatted / All checks passed! / Success: no issues found in 43 source files
  ============================= 113 passed in 42.56s =============================

$ for i in 1 2 3 4 5; do uv run pytest -q ...; done      # flakiness check
exit 0 x5; 33.83–47.33 s   (runs 1–2 predate the two race tests: 111; runs 3–5: 113)
```

The suite is still under SPEC Phase 2's 60 s, but the slowest run (47 s) leaves less
margin. Phase 3 should keep the subprocess tests marked `slow` and watch the total.

Mutation round 2, against the new tests and the corrected claims. Same method as Phase 2:
scripted, one bug at a time, each file restored byte for byte (sha256-verified):

```
requeue_all unbounded (no end id)            -> CAUGHT (requeue_all_is_bounded...)
pool children not terminated past grace      -> CAUGHT (sigterm_past_grace_terminates_process_pool_job)
broken process pool not replaced             -> CAUGHT (a_dead_pool_child_fails_one_attempt...)
scheduler sleeps after every batch           -> CAUGHT (scheduler_drains_a_backlog...)
reaper cursor not kept between passes        -> CAUGHT (reaper_cursor_continues...)
retry delay sent in us instead of ms         -> CAUGHT (worker_schedules_retry_within_the_backoff_bound)
deleted pending ids not logged               -> CAUGHT (reclaim_reports_pending_entries...)
pool children don't ignore SIGINT            -> MISSED, test rewritten -> CAUGHT (child's KeyboardInterrupt)
prune uses 'inactive' instead of 'idle'      -> MISSED, test rewritten -> CAUGHT
__main__ guard removed                       -> MISSED: the claim was false (see above), not the test
requeue.lua leaves the DEAD record           -> CAUGHT (stale_commit_after_..._dead_lettered_and_requeued)
retry.lua terminal-state check bypassed      -> CAUGHT (stale_commit_after_the_reclaimer_scheduled_a_retry)
```

Mohammed's decisions: the four "decisions worth review" are accepted (ADR-024: no terminal
check on heartbeats, and a handler keeps running after it loses its lease; ADR-028: the
spawn start method, and the private `_processes` map). The hung handler gets a per-job
timeout in Phase 3 (ADR-025).

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
  grace period runs out. `cpu_task` now runs in the process pool.
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
  it alive): stuck in the PEL, not lost (ADR-025). **Decided (Mohammed): a per-job timeout
  in Phase 3**, details in the Phase 3 prompt.
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

- **2026-09-22 (Phase 2): four test-harness races.** Every first-run failure in Phase 2 was
  the *test's* fault, and each fix made the test more precise rather than looser. (The
  suite passing didn't mean there were no system bugs: the post-gate review found two,
  below.)
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
- **2026-09-22 (Phase 2 review): a documented fix for a bug that didn't exist.** Phase 2
  added an `if __name__ == "__main__"` guard to `ftq/__main__.py`, and ADR-028 plus this log
  said that without it every spawned pool child would re-run the CLI. Nothing tested that.
  The review's mutation check removed the guard and every test still passed. CPython's
  `multiprocessing.spawn._fixup_main_from_name` deliberately skips `*.__main__` modules.
  The guard stays as hygiene; the claim was corrected in ADR-028. Lesson: a design-time
  "this would break" belongs in the docs only once a test shows it breaking.
- **2026-09-22 (Phase 2 review): two tests that couldn't fail.** Mutation checks showed
  (1) the Ctrl-C test passed even with the pool's SIGINT-ignoring initializer removed,
  because it signalled before the lazily created pool child existed; and (2) the
  "live idle workers aren't pruned" test passed even when pruning keyed on the wrong XINFO
  field, because `inactive` is -1 for a consumer that never had a successful read. Both
  were rewritten until the planted bug made them fail: the job now writes a marker from
  inside the child before the signal, and each consumer gets one successful read first.
- **2026-09-22 (Phase 2 review): `ftq dlq requeue --all` looped forever on a live system.**
  `requeue_all` paged forward through the DLQ until it found no more entries. With workers
  running, a requeued poison job fails again within milliseconds and is appended to the
  END of the DLQ, so the sweep kept finding "new" entries and requeueing the same jobs
  forever. Every Phase 2 test ran `--all` with no worker running, so none could see it.
  Found by asking what `--all` does under concurrent writes. Reproduced first by
  `test_requeue_all_is_bounded_while_workers_keep_killing_jobs` (20 poison jobs, a live
  worker, page size 1), which timed out at 10 s. Fix: snapshot the DLQ's last id before
  sweeping and stop there (ADR-027). A mutation check confirms that removing the bound
  fails the test again. Lesson: a "process everything in this stream" loop over a stream
  that the system itself appends to needs an explicit end point.
- **2026-09-22 (Phase 2 review): the heartbeat margin allowed a lease to expire after one
  missed beat.** The config accepted `heartbeat_interval ≤ visibility_timeout / 2`, and
  ADR-025 claimed one lost beat "never" expires a healthy lease. But beats land every
  `interval + one Redis round trip`, not every `interval`. After one lost beat the entry's
  idle time reaches `2 × (interval + RTT)` before the next beat lands, which at exactly
  `lease / 2` is already past the lease, and the reaper can take a healthy job. No test
  caught it: every test used 5× margins, so the boundary was never exercised, and the unit
  test asserted the wrong boundary as valid. Found by redoing the arithmetic in review.
  Fix: require `heartbeat_interval ≤ visibility_timeout / 3`, leaving a full interval
  spare for one lost or slow beat. The unit test now rejects `lease / 2`. The defaults
  (10 s / 30 s) already met the new rule, so no default changed. Lesson: a safety bound
  written as "N beats per lease" has to count the time a beat takes, not just the
  interval.
- **2026-09-22 (Phase 3): a design flaw caught before it shipped: queued process jobs
  would have timed out.** The first version of the timeout started each job's clock when
  it was submitted to the process pool. With `concurrency` 10 > `process_pool_size` 2,
  jobs wait *inside* the executor for a child. A job could time out while queued, and
  its pool reset would kill the jobs that were running, which then restarted and queued
  again: a cascade. Found by asking what the timeout measures when the pool is smaller
  than the worker. Fixed with a semaphore of `process_pool_size` permits taken before the
  clock starts. A test (three 1 s jobs, a 1.5 s timeout, one child) proves it, and removing
  the semaphore makes the test fail. Lesson: a timeout must say which wait it bounds.
- **2026-09-22 (Phase 3): a test that couldn't fail.** The first
  `test_an_idempotent_repeat_is_answered_even_when_full` sent its repeat *before* anything
  had tripped the full flag, so a script that checked the flag before the idempotency key
  still passed. The mutation run caught it (MISSED). The test now trips the flag first and
  asserts it's set before the repeat.
- **2026-09-22 (Phase 3): an unverified claim written into an ADR.** The first draft of
  ADR-033 said the demo "produced only lifecycle lines". Nobody had looked, and the
  containers' logs were already gone. The demo was rerun and the log lines counted (4,
  all INFO "started") before committing, and the ADR now states that number. Same lesson
  as Phase 2's `__main__` guard: check it, then write it.
- **2026-09-22 (Phase 3): a mutant that hung the harness.** With the pool reset disabled,
  the hung child kept the test process alive past pytest's 40 s timeout: the worker's pool
  shutdown waits for its children. The mutation harness now runs each test run in its own
  session and SIGKILLs the whole process group after 90 s, counting that as caught. The
  stray pytest and pool-child processes from the first attempt were found with `pgrep` and
  killed.
