# PROGRESS

## Status

- **Current phase:** Phase 6 (local benchmark harness): **complete**, awaiting
  Mohammed's review. Local / Docker Desktop numbers only (not the headline).
  - Redis's single main thread is the bottleneck from 4 workers up.
  - Throughput per worker count varied up to ~1.7× between two sessions on the same
    code (ADR-042).
- **Follow-up done:** CI #7's chaos failure (I4 `kills = 2 < 3`, a harness scheduling
  gap, fixed in ADR-043), and the 1M record: 7 of 7 passing 1M runs on the current
  queue code.
- **Phase 7 pre-flight (read-only, $0) done 2026-09-23.** **Blocker:** the EC2 vCPU
  quota is 5, so at most two 2-vCPU instances can run; the plan needs ~16.
  Waiting on Mohammed to choose a sizing option (quota increase or not). Nothing billable
  is created without an itemized estimate and an explicit "yes".
- **Repo:** https://github.com/TechBroMoho/fault-tolerant-task-queue (public, default branch `main`, created 2026-09-22).
- **AWS:** nothing created. Spend to date: $0.

## Phase log

### Phase 7 pre-flight: read-only AWS checks (2026-09-23, $0)

Profile `ftq`, region `us-west-2`, AWS CLI 2.37.0. Only read-only calls, plus
`run-instances --dry-run` (creates nothing). No Cost Explorer calls ($0.01 each).
- **Identity:** `arn:aws:iam::<acct>:user/ftq-admin`, an IAM user, not root
  (`AdministratorAccess` attached).
- **Free plan** (`aws freetier get-account-plan-state`): type FREE, status ACTIVE,
  **remaining credits $100.00**, **expires 2027-03-19** 02:30 UTC.
- **Instance types** (`describe-instance-types --filters free-tier-eligible=true`):
  c7i-flex.large (2 vCPU / 4 GiB), m7i-flex.large (2 / 8), t3.micro, t3.small,
  t4g.micro, t4g.small, t8i.micro, t8i.small. All are 2 vCPU.
  - Whether the Free plan *blocks* other types is **not verified**. `run-instances
    --dry-run` said "would have succeeded" for c7i.2xlarge, and also for 3×
    c7i-flex.large (6 vCPU, over the quota). So dry-run checks only IAM, not quotas or
    plan limits, and proves nothing here.
- **vCPU quota** L-1216C47A (Running On-Demand Standard): **5.0** (AWS default 5.0,
  adjustable). No quota requests in the history. Fargate's vCPU quota is 6.0 (unused;
  staying on EC2, per SPEC).
- **Account is empty:** 0 instances, EIPs, NAT gateways, ECS clusters, ECR repos,
  budgets. The default VPC has default subnets in us-west-2a–d.
- **ECS-optimized AMI** (SSM `/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended`):
  al2023-ami-ecs-hvm-2023.0.20260918, 30 GB gp3 root.
- **Prices** (Pricing API, on-demand, us-west-2): c7i-flex.large $0.08479/h,
  m7i-flex.large $0.09576/h, t3.small $0.0208/h, gp3 $0.08/GB-month, public IPv4
  $0.005/h, CloudWatch Logs ingestion $0.50/GB, ECR storage $0.10/GB-month.

**Consequence:** the SPEC's 16-vCPU fleet (12 workers + Redis + loadgen) doesn't fit in
5 vCPUs. Options and estimates were given to Mohammed in chat; the choice is his.

### Phase 7 prep: the worker's connection pool (2026-09-23)

**Fixed the latent pool limit from Phase 6 (ADR-044), test first.**
- `Settings.redis_max_connections = max(100, 2 × concurrency + 2)`, which `make_redis`
  now passes. That's 2 connections per in-flight job (handler or transition, plus
  heartbeat), plus the fetch and maintenance loops. The pool still raises when
  exhausted; it doesn't block.
- New `tests/integration/test_connection_pool.py`: concurrency 100, all 100 jobs
  mid-ledger-call during a 1 s `CLIENT PAUSE ALL`.
  - Old code: failed 3 of 3 (992–1,092 `Too many connections` warnings each).
  - New code: passed 30 of 30.
- A unit test pins the formula.

```
$ make check-all > log 2>&1; echo "make check-all exit=$?"
make check-all exit=0
======================= 210 passed in 121.58s (0:02:01) ========================
```

### Phase 6 follow-up: CI #7's chaos failure and the 1M record (2026-09-23)

**CI #7 ([run 35854442425](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35854442425), 662300e, docs-only) failed in `chaos`,
not `check`.** It is not the known flaky test: `check` (200 tests) and `docker`
passed.
- Seed 845664227, 100K jobs, 4 workers. The queue was right: I1, I2, I2b, I3, I5, and
  W1 all passed, with 99,974 SUCCEEDED and exactly the 26 expected DEAD.
- **I4 failed: `kills = 2 < 3`.** The plan had 4 kills. Two were skipped with `docker
  kill … is not running`: a crashy job had just killed that worker (36 crash restarts
  in a 77 s fault phase on 4 workers), and the supervisor hadn't restarted it yet. The
  injector gave up at once.
- The same class of bug as Phase 4's seed 1661764791: the plan left I4 to chance.
- **Fix (ADR-043), test first.** A kill or pause aimed at a worker that is down waits
  for the supervisor to restart it, retrying every 0.5 s for up to 10 s. The kill's
  hold is released between attempts, so the supervisor can restart the worker. I4's
  minimums are unchanged.
  - 3 new unit tests with a fake `docker` (kill and pause wait and happen; a worker that
    stays down is skipped with its hold released). All 3 failed on the old injector.
  - Mutation checks: no retry → CAUGHT (3 failed); the hold kept between attempts →
    CAUGHT (1 failed).
- Local 100K runs with 4 workers on the fix both PASSED with 0 skips:
  - a random seed, 1427530277 (`results/local/chaos_report_phase6_injector_retry_w4.json`);
  - the failing seed, 845664227 (`results/local/chaos_report_phase6_seed845664227_w4.json`).

  Neither needed a retry: local timing didn't recreate the crash-then-kill collision.
  So the retry path is proven by the unit tests, not yet by a live run.
- The failed report is in `results/ci/chaos_failures/run35854442425_seed845664227_I4.json`.
  It wasn't rerun until green. The next CI run is on the fix.
- Both workflows now log the runner's CPU model (`lscpu`), for the reason below.

**Every 1M chaos run** (chaos-scale, 4 workers = one per vCPU, `ubuntu-24.04`).
- **The queue code is the same in all of them:** `git diff c20d0fd HEAD -- src` is
  empty. c0f5657 was a docs-only commit on top of c20d0fd.
- **Every run:** N = 1,000,000 accepted, all of I1–I5 and W1 passed, 999,740 SUCCEEDED
  and 260 DEAD (200 poison, 30 hang_forever, 30 crashy), 0 timeouts of runs that can't
  hang, and Redis peak 514–515 MiB.
- Reports: `results/ci/chaos_report_run<id>_N1M_w4.json`.

```
#   run                                   code     seed        job time  harness  kills pauses net  skipped  reclaimed  dup suppr.  max delivery  result
3   https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35838910547  c20d0fd  1644982976  34m57s    2,070 s   33    33    96   3        9,183      2,792       8             PASS
5   https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35846621221  c0f5657   618731388  34m25s    2,033 s   29    43    88   0        9,092      2,851       7             PASS
6   https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35846624796  c0f5657   400193833  34m49s    2,055 s   34    30   102   3        9,145      2,755       7             PASS
7   https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35846628370  c0f5657  1816374322  24m19s    1,429 s   28    42    97   0        9,151      2,565       7             PASS
8   https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35846631919  c0f5657   884433612  24m22s    1,424 s   33    37    95   0        9,083      2,579       7             PASS
9   https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35846635450  c0f5657   261336795  35m44s    2,111 s   35    35    97   2        9,275      2,774       7             PASS
10  https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35846639056  c0f5657  1393279984  34m36s    2,043 s   29    29   105   4        8,958      2,757       6             PASS
```

- All seven seeds are distinct. Crash restarts were 360 in each run except #10 (358).
  That's within I3's bound: a crashy job can be dead-lettered by a reaper before its
  last run.
- "Max delivery" is the highest delivery count of any non-crashy job; the limit is 12.
- **Why #7 and #8 took ~24 min against ~35:**
  - The fault phase lasts as long as the enqueue, and the producer is throttled by
    backpressure to what the workers finish. It was 1,219 and 1,239 s against
    1,731–1,824 s.
  - The workers finished faster because the runner's cores were faster. Same code, same
    job mix, and the same work in the counters (reclaims, timeouts, and pool resets
    within 2 % of the others).
  - The CPU-bound measurements roughly halved or dropped:
    - process-pool start-up (spawning an interpreter, importing modules), single-thread
      CPU: mean 0.97 and 0.99 s against 2.20–2.32 s;
    - Redis CPU 19.3 % and 18.5 % against 24.8–26.2 %;
    - Toxiproxy 19.1 % and 19.5 % against 27.1–29.6 %.
  - GitHub-hosted runners aren't uniform hardware (these ran in six different Azure
    regions). The workflow didn't log the CPU model, so the chip behind #7/#8 can't be
    named. It does now, and the first run with it shows the spread within a single
    workflow run: [run 35861752180](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35861752180)
    (639e3fa, all green) ran `check` on an AMD EPYC 7763 and `chaos` on an AMD EPYC 9V45.
- **chaos-scale #4 ([run 35838915586](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35838915586), 4 min 31 s) was not a 1M run.**
  It was the Phase 5 check of the oversubscribed setup: `workflow_dispatch` with
  N = 100,000 and 8 workers on the 4-vCPU runner. It passed (0 timeouts of runs that
  can't hang, 185 pool resets). It's already recorded in the Phase 5 section and
  `results/ci/chaos_report_run35838915586_N100K_w8.json`.
- #1 and #2 were the two failing 1M runs on pre-fix code (Phase 5, ADR-039).

**Answer for the resume bullet: 7 passing 1M-job chaos runs on the current queue code**
(1 on c20d0fd, 6 on c0f5657; `src/` identical to HEAD), 7 of 7 on that code, all on
GitHub Actions. Each: 0 lost jobs, 0 duplicate results, 0 duplicate effects.
- **Not "on every push":** they were `workflow_dispatch` runs.
- **The nightly schedule hasn't produced a run.** Today's 09:23 UTC slot came about
  3 h after the workflow was added (06:32 UTC), and `gh run list --event schedule` is
  empty (checked 12:36 UTC). GitHub doesn't guarantee that scheduled runs fire.
  Tomorrow's slot will show whether it works; until then "nightly" is configured, not
  observed.
- Per-push CI runs 100K.

### Phase 6: Local benchmark harness (2026-09-23)

**Built** (ADR-041)
- **`bench/loadgen.py`: the benchmark itself.** It needs only `FTQ_REDIS_URL` and
  running workers, so Phase 8 runs it unchanged from the worker image
  (`python -m bench.loadgen`).
  - Producers: multi-process and open loop. Every 10 ms tick, each one sends what has
    come due as one `enqueue_many`. Or saturation mode (`--rate 0 --max-depth D`):
    back-to-back batches held to a bounded backlog.
  - Configurable job type, payload fields, and payload size.
  - Every second it samples depth, counters, Redis CPU (main thread), and memory,
    against Redis `TIME`.
  - Then it waits for the drain and reads the append-only results log:
    - completion throughput over the steady-state window [warmup, warmup + measure);
    - end-to-end p50/p95/p99/p99.9 (enqueue → commit, both Redis `TIME`, 1 ms
      resolution);
    - enqueue call latency with batch sizes;
    - accepted/rejected/blocked counts;
    - per-worker completions;
    - `INFO commandstats` per job;
    - an exactly-once check (accepted = distinct results, no job_id twice, empty DLQ,
      drained).
  - The raw histograms are stored, so every percentile can be recomputed.
- **`bench/run.py`: the local Docker driver.** Its own Compose project (`ftq-bench`,
  Redis on 6391) with a fresh Redis and N workers for every point. The loadgen runs as
  a container on the same network. Suites: `concurrency`, `scaling`, `latency`,
  `backpressure`, `iothreads`, `point`.
  - CPU evidence:
    - each container's cgroup `cpu.stat` (exact CPU time);
    - the whole VM's `/proc/stat`;
    - both timestamped by the VM clock (= Redis's), so no clock offset.
  - Jobs per CPU-second per worker.
  - Worker logs are saved and their lines counted in the report.
- **`bench/plot.py`** writes these into `results/local/bench/`, all labelled local:
  - `scaling.png` (per session);
  - `bottleneck.png`;
  - `redis_cost.png`;
  - `concurrency.png`;
  - `latency.png`;
  - `backpressure.png`;
  - `summary.md` / `summary.json`.
- `bench/analysis.py`: the pure arithmetic (window, nearest-rank percentiles,
  upper-edge histograms, interpolated CPU). `make bench` runs scaling + latency +
  backpressure + charts.
- The worker image now carries `bench/`. matplotlib is a dev dependency, and mypy now
  covers `bench/`, which found a broken committed script (below).
- New tests, 186 → 200:
  - `test_bench_analysis.py`: 11 unit tests;
  - `test_loadgen.py`: 3 real-Redis runs (open-loop rate, latency window, and the
    exactly-once verdict; the saturation guard; a run that didn't drain is *not*
    exactly-once, and refusals are counted as refusals).
- **Fixed:** `bench/lease_starvation.py` had been broken since Phase 3 (a helper's
  signature changed). It's re-run, exit 0.

**Acceptance: charts + raw JSON committed; the bottleneck analysis with evidence.**
- 59 points across all suites (`results/local/bench/*/`). **Every one exactly-once:**
  0 duplicate results, 0 missing, empty DLQ, and 0 retries, reclaims, lease losses, or
  DLQ moves. Worker logs: 2 lines in total, both the `MaxConnectionsError` warning at
  concurrency 100 (below).
- Reproduce any point with its report's `meta.reproduce`, or a suite with
  `uv run python -m bench.run <suite>`.
- Environment:
  - MacBook Pro M4 (Mac16,1): 4 performance + 6 efficiency cores.
  - Docker Desktop 29.4.3: 10 vCPUs, 7.75 GiB.
  - Redis 8.8.3: AOF everysec, noeviction, 4 GB.
  - Jobs: `send_email`, 100 B payload, `FTQ_CONCURRENCY=50`.
- Code: the first set (concurrency r1, scaling r1–r3, latency, backpressure) ran on the
  clean commit bab46ca. The diagnostic runs ran on 5e2555a, which only adds
  commandstats, the io-threads knob, and repeats; the loadgen measurement code is
  unchanged otherwise. Each report's `meta.git` has its commit.

- CI on the pushed code: [run 35854003589](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35854003589)
  (903229c): check, docker (the image now carries `bench/`), and chaos 100K all passed.

**Scaling** (saturated, completed jobs/s in the 30 s window; two sessions, ADR-042 §4):

```
workers   session A 10:09-10:24 UTC (3 runs)   session B 10:41-10:52 UTC (2 runs)   Redis main thread   busiest worker
1          5,808  6,157  6,783   (6,157)        7,817  7,827   (7,822)              0.35-0.38           1.00
2          9,052 10,527 12,312  (10,527)       13,776 15,348  (14,562)              0.61-0.66           0.97-0.99
4         13,833 14,068 19,091  (14,068)       20,333 21,386  (20,860)              0.89-0.94           0.90-0.92
8         14,137 14,714 14,821  (14,714)       22,713 25,717  (24,215)              0.91-0.93           0.64-0.77
12        11,160 12,487 12,937  (12,487)       20,823 23,624  (22,223)              0.88-0.91           0.42-0.51
```

**Bottleneck (ADR-042):**
- **1–2 workers:** the workers are CPU-bound (each ~1.0 core).
- **4 or more workers: Redis's single main thread.** It's 0.88–0.94 busy, the workers
  wait (0.42–0.77 of a core each), and throughput stops rising.
- **Per job:**
  - 3 Lua script calls (enqueue, commit, ledger), 18–25 µs;
  - ~1/13 of an `XREADGROUP`;
  - the main thread's total is 36–79 µs, so about half of it is socket I/O, parsing,
    and the event loop outside commands.
- **The two sessions differ because the host did,** not the queue or the method:
  - the same commands per job, at 62–65 µs vs 36–40 µs per job for 8 workers;
  - interleaved saturation and fixed-rate runs agreed;
  - AC power, no Low Power Mode, no thermal warnings;
  - the cause (performance/efficiency-core placement or host load) wasn't isolated.
- **Redis io-threads 4** (paired, interleaved, 4 producers, backlog full): 21.0–21.5K
  against 23.0–24.6K with io-threads 1. The main thread fell to 0.70 busy, but the extra
  threads cost ~0.8 cores on a VM already ~8/10 busy. Locally, total CPU is the next
  limit. Phase 8 should re-test it with Redis on its own instance.
- **The load generator** was never CPU-bound (0.08–0.49 of a core per process). But it
  is latency-bound at one call in flight per process, so above ~20K/s it needs 4
  processes. At 12 workers the backlog fell to ≤ 198, and those points partly measure
  the producers.

**Concurrency** (1 worker, 3 repeats, completed jobs/s):
- 10: 6,192 / 6,257 / 6,288;
- 25: 6,797 / 7,422 / 7,509;
- 50: 6,799 / 7,944 / 8,100;
- 100: 7,877 / 8,002 / 8,114.

At 10, fetches return 2 jobs per `XREADGROUP`, and Redis spends 53–55 µs per job
against 43–44 µs at 50. The benchmark uses 50, and the library default stays 10
(ADR-042).

**Latency** (8 workers, open loop, session A timing 10:25–10:29 UTC; offered 10–90 % of
that session's 14,714/s median):

```
offered/s   completed/s   e2e p50   p95    p99     enqueue call p50 / p99 (jobs per call, median)
 1,471       1,471.3       2 ms     21     241     1.4 / 15.3 ms (7)
 3,678       3,678.0       3        17      43     1.5 / 15.5 (18)
 7,357       7,357.2       3        15      46     1.6 / 19.7 (37)
11,035      11,033.9       5        18      41     2.2 / 27.2 (55)
13,246      13,278.0       6        27      55     2.7 / 38.7 (67)
```

The 241 ms p99 at the lowest load is as measured: a VM-level stall is the likely cause
(that run's producers also lagged 0.40 s at one point, the most of the five). It isn't
explained further.

**Backpressure** (8 workers, 22,071 jobs/s offered open loop, watermarks 20,000 /
15,000):
- **reject:** 21,991/s offered, 20,911/s accepted, 20,469/s completed. 54,525 jobs
  rejected; the script's own `rejected` counter agrees exactly. Depth peaked at 18,433
  in the window (19,780 over the whole run), under the 20,000 high watermark.
- **block:** 11,281 jobs had to wait (12.97 s of waiting in total), 0 rejected, 20,639/s
  completed. Depth peaked at 19,493 over the whole run.
- Offered was only ~1.07× what this fleet completed at that time: the 1.5× was relative
  to session A's median. The reject run's depth passed the low watermark only after
  ~28 s, so Phase 8's backpressure demo should offer well above measured capacity.

**Mutation checks** (scripted, one at a time, each file restored and sha256-verified):

```
analysis: histogram rounds down (floor)                -> CAUGHT
analysis: busy_fraction extrapolates past its samples  -> CAUGHT
analysis: percentile rank floored (interpolates)       -> CAUGHT
loadgen: exactly-once ignores drain and missing        -> CAUGHT
loadgen: latency not limited to the window             -> CAUGHT
loadgen: no depth guard                                -> CAUGHT
loadgen: refused jobs counted as accepted              -> CAUGHT
loadgen: offered/accepted window includes the warmup   -> CAUGHT
```

**Decisions for Mohammed's review**
- ADR-041: the harness design. Notably, saturation holds a bounded backlog rather than
  using the queue's own `block` mode, whose re-polls would load Redis with refused
  enqueues.
- ADR-042:
  - benchmark at `FTQ_CONCURRENCY=50`, with the library default kept at 10;
  - Redis io-threads stays 1 locally;
  - no commit batching until AWS data says Redis-bound below target.
- How to quote local numbers: a range per worker count, never one "capacity". Every
  saturated run with ≥ 4 workers did ≥ 11,160 jobs/s, but the headline is Phase 8's.

**Open issues**
- ~~**The worker's Redis connection pool isn't sized for its concurrency.**~~ Fixed
  2026-09-23 (ADR-044): see "Phase 7 prep" above.
- The session-to-session drift on this laptop is unexplained (ADR-042 §4).
- Phase 8 needs an ECS driver for the loadgen and task-level CPU from the ECS metadata
  stats endpoint (to be verified), plus ≥ 4 producer processes.
- Carried: the flaky Phase 3 test (no failure since); results/effects logs never
  trimmed (ADR-021); the retry-ownership check has no verifier-level evidence
  (ADR-037). Five `chaos-scale` runs were dispatched on GitHub at 10:04 UTC on c0f5657.
  They're not part of this phase, and I didn't check their results.

**Evidence (local)**

```
$ make check-all > log 2>&1; echo "make check-all exit=$?"      (run after the last code change)
make check-all exit=0
  77 files already formatted / All checks passed! / Success: no issues found in 71 source files
  ======================= 200 passed in 121.10s (0:02:01) ========================
```

### Phase 5: CI on GitHub Actions (2026-09-23)

**Built**
- **`.github/workflows/ci.yml`**, on every push and PR (ADR-040):
  - `check`: `make setup`, `make up` (the Compose Redis: a `services:` container can't
    set `noeviction`, and the suite checks it), `make check-all`, timed, with
    `--durations=20`.
  - `docker`: builds the worker image and runs it.
  - `chaos`: N=100,000 with **one worker per vCPU** (4). The report and worker logs are
    uploaded whether it passes or not.
  - No retries anywhere.
- **`.github/workflows/chaos-scale.yml`**: N=1,000,000 **nightly** (09:23 UTC) and on
  `workflow_dispatch`, which takes N, workers, and the drain timeout.
- Actions was already enabled (`gh api …/actions/permissions`: `enabled: true`,
  `allowed_actions: all`) and had no workflows before this phase. Actions are pinned to
  release tags (checkout v7.0.1, setup-uv v10.2.0, upload-artifact v7.0.1), uv
  0.11.16, and Python 3.12.13.
- README: CI badge.
- **Fixed, found by CI** (details under "Things that went wrong"):
  - **The pool-reset cascade (ADR-039).** The per-job timeout now measures handler
    execution only. Each (re)started run gets a full timeout, and a new pool is warmed
    (imports done, every child answering) before any run's clock starts.
  - A CLI test that compared Rich-styled output (ANSI escapes under `GITHUB_ACTIONS`).
  - Harness: a `--run-dir` outside the repo crashed after the run and lost the report.
- **The chaos report now records** Redis memory, pool start-up times (count, mean,
  max), and timeouts of runs that can't hang (evidence, not invariants; I1–I5, W1, and
  I4's minimums are unchanged).
- New tests, 182 → 186 (all in `test_timeouts.py`; the handlers are in
  `slow_start_handlers.py`, whose module takes 2 s to import in a pool child):
  - a restarted bystander gets a fresh timeout;
  - pool start-up doesn't count toward the timeout;
  - after a reset, no clock starts until the new pool is ready;
  - no clock starts until every child is ready (children ready at 2 s and 4 s).
  Each failed on the code before its fix: 3/3, 3/3, 3/3, and CAUGHT as a mutant
  (below).

**Acceptance: green CI on GitHub.**
- **Run [35838899855](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35838899855)**
  (c20d0fd): check, docker, and chaos all passed.
  - `make check-all`: 186 passed in 109.9 s (pytest), 118 s wall.
  - Chaos 100K, 4 workers: **PASSED**. 99,974 SUCCEEDED and 26 DEAD (exactly the
    expected ones), 0 timeouts of runs that can't hang, 192 pool resets, 1,009
    reclaims, 330 duplicates suppressed; 242 s. Report:
    `results/ci/chaos_report_run35838899855_N100K_w4.json`.
- **1M run [35838910547](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35838910547)**
  (c20d0fd, 4 workers): **PASSED**, all of I1–I5 and W1.
  - 1,000,000 accepted: 999,740 SUCCEEDED and 260 DEAD (200 poison, 30 hang_forever,
    30 crashy).
  - I2: every effect key once, 2,928 effects suppressed. I2b: one result per SUCCEEDED
    job, 2,792 duplicates suppressed.
  - Faults: 33 kills, 33 pauses, 96 network windows, and 360 crash restarts. 9,183
    reclaims, 3,718 timeouts, 1,839 pool resets, 0 timeouts of runs that can't hang.
    Highest delivery of a non-crashy job: 8 (limit 12).
  - Redis peak 514 MiB of the 3 GiB cap.
  - **Chaos step: 34 min 48 s** (harness: 2,070 s total, fault phase 1,766 s, drain
    227 s). Report: `results/ci/chaos_report_run35838910547_N1M_w4.json`.
- **8 workers on the runner, after the fix: run [35838915586](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35838915586)**
  (100K): **PASSED**, the same oversubscribed setup that had failed 3 of 3. 0
  timeouts of runs that can't hang, 185 pool resets, and pool start-up 6.4 s mean /
  13.5 s max: three times the 2 s hang timeout, with no cascade. Report:
  `results/ci/chaos_report_run35838915586_N100K_w8.json`.
- Local 100K, 8 workers, on the same code: **PASSED**, W1 clean, 0 timeouts of runs that
  can't hang, pool start-up 0.70 s mean / 6.42 s max
  (`results/local/chaos_report_phase5_warm_pool.json`; its `run.git` shows the working
  tree before the c20d0fd commit, whose `src/` it matches).

**Scale decision (SPEC §7): 100K on every push, 1M nightly + on demand.** 1M took 34 min
48 s on the runner, about 3× the ~12 min per-push bar. The resume bullet's "on every
code change … 1M+ tasks" is therefore **not** true as written. What is true: 100K jobs on
every push, and 1M nightly, which has passed once so far (ADR-040 has proposed
wording).

**Timing: `make check-all` on the runner.** Wall time tracks pytest's own: 87 s wall /
83.1 s pytest (182 tests, run 35827596244), 118 s / 109.9 s (186 tests, run
35838899855). The local wall-time gap (Phase 4 open issue) didn't appear on Linux. Job
timeouts were set from these: check 15 min, docker 10, chaos 20, 1M 90.

**The known flaky test in CI.**
`test_waiting_for_a_pool_child_does_not_count_toward_the_timeout` passed in all 4 CI
`check` runs (the first run's one failure was the ANSI test). There was no failure, so
there are no counters to record. It is still unexplained locally.

**Every CI run of this phase**, none dropped:

```
run          workflow     code     N / workers  result
35827248457  ci           f0b22b3  100K / 8     check FAIL (ANSI test); chaos FAIL I1/I3 (6 false DEAD, cascade)
35827263800  chaos-scale  f0b22b3  1M / 8       FAIL I1/I3 (375 DEAD vs 260 expected); 1,724 s
35827596244  ci           73fa623  100K / 8     check PASS (182, 83.1 s); chaos FAIL I1/I3 (22 false DEAD)
35828108573  ci           901d763  100K / 8     check PASS; chaos FAIL I1/I3 (35 false DEAD): fresh-timeout fix alone
35828117698  chaos-scale  901d763  1M / 8       FAIL I1/I3 (436 DEAD vs 260); 2,056 s
35838899855  ci           c20d0fd  100K / 4     ALL PASS (acceptance run)
35838915586  chaos-scale  c20d0fd  100K / 8     PASS (cascade gone even oversubscribed)
35838910547  chaos-scale  c20d0fd  1M / 4       PASS; 34 min 48 s
```

The failed runs' reports are in `results/ci/chaos_failures/`. In the three failed 100K
runs on the old code, timeouts of runs that can't hang were 219, 329, and 337 (counted
from the artifacts' logs), against 0 in every run since.

**Mutation checks** (scripted, one at a time, each file restored and sha256-verified):

```
worker: restart keeps the first run's deadline      -> CAUGHT (fresh_timeout + reset test)
worker: clock starts before the pool is ready       -> CAUGHT (3 failed)
worker: initializer skips the handler imports       -> CAUGHT (3 failed)
worker: warm-up ends after one round                -> CAUGHT (every_child)
worker: clock not stopped while waiting for a pool  -> CAUGHT
worker: warm-up tasks don't hold their child        -> MISSED, equivalent (the hold only paces the
                                                       rounds; the loop still waits for every child)
(Barrier design, replaced) no barrier               -> MISSED, then the staggered test -> CAUGHT
```

**Decisions for Mohammed's review**
- ADR-039: the timeout measures handler execution only (as asked). The Barrier version
  was dropped for semaphore-leak warnings, which W1 caught.
- ADR-040: one chaos worker per vCPU in CI; Redis through `make up` rather than a
  service container (a SPEC deviation); 1M nightly.
- The resume bullet's wording (ADR-040, Consequences).

**Open issues**
- The nightly 1M has one pass so far; the bullet shouldn't lean on it until there's a
  record.
- The flaky Phase 3 test: still unexplained, and no CI failure yet.
- Carried from Phase 4: results/effects logs never trimmed (ADR-021); the retry-ownership
  check has no verifier-level evidence (ADR-037).

**Evidence (local)**

```
$ make check-all > log 2>&1; echo "make check-all exit=$?"
make check-all exit=0
  69 files already formatted / All checks passed! / Success: no issues found in 62 source files
  ======================= 186 passed in 107.25s (0:01:47) ========================
```

### Phase 4 review (2026-09-22)

A skeptical post-gate review, asked for by Mohammed: look for correctness bugs, tests or
verifier rules that can't fail, and doc claims with no result file behind them. Plus a
specific hypothesis about the flaky test. Every fix was preceded by a failing test or a
surviving mutant.

**Mohammed's decisions:**
- ADR-037 accepted as is, and SPEC §7's wording amended to match.
- `AGENTS.md` (from another tool) deleted.

**Bugs fixed**
- **The suspect rule didn't cover low `max_deliveries`** (a real bug in the ADR-035 fix).
  With `max_deliveries` below `suspect_deliveries` (e.g. 2 with the default 3), entries
  reached the DLQ before they could become suspects, so a crashy job's companions
  followed it there again.
  - The existing crash-loop test runs with exactly `max_deliveries=2`, but has no
    companions.
  - `test_crash_isolation` is now parametrized over `max_deliveries` {3, 2}. At 2, all 4
    innocent jobs ended DEAD in both orderings.
  - Fix: `Settings.suspect_threshold = min(suspect_deliveries, max_deliveries)`.
- **The verifier ignored sick workers.** The supervisor restarted any worker that exited,
  so a crash with a traceback (like the ADR-038 startup bug) only showed up to someone
  reading logs. New check **W1_workers_healthy**: fail on any exit not caused by a
  crashy job, or any ERROR or non-JSON worker log line.

**Tests that couldn't fail, now fixed.** Nine verifier failure branches had no test that
made them fail: stray effects, results for jobs never accepted, a non-DEAD job in the DLQ,
a missing DLQ entry, poison with the wrong attempt count, a wrong reason, hang_forever with
a non-timeout error, each I5 leftover, and W1. Each now has one. The mutation round then
showed one of *those* tests couldn't fail either. The I5 "pel" case left its entry in the
stream too, so the stream check failed first, and a verifier that ignored the PEL still
passed (MISSED). The case now deletes the pending entry's data, so only the PEL check can
see it (CAUGHT).

**Doc claims corrected** (each checked against a committed result file):
- "3.1–4.3 CPUs": 4.3 came from an uncommitted run. The reports are committed now, and
  the range is restated from them (3.1–4.8).
- "Consumers pruned 23–34" was the count of log lines; the counter says 37–41.
- "Duplicate commits suppressed" misnamed `duplicates_suppressed`, which also counts
  dropped retry/DLQ copies.
- "Failed 2 of 5 runs": only one of the two failures was ever identified as that test.
- "The first debug run caught the supervisor double-counting": it was debug run 2.
- ADR-008's "4 earlier runs" had three uncommitted reports behind it. All 100K reports of
  the phase are now in `results/local/chaos_history/` or `chaos_failures/`.

**Re-run on the final code.** The verifier changed, so the three 100K acceptance runs
were redone on `b8fca66`: all PASSED, W1 included (Phase 4 section above). The 464635e
set stays as history.

**The flaky test: the hypothesis, and what was ruled out.**
`test_waiting_for_a_pool_child_does_not_count_toward_the_timeout` (1 failure in 6 passes
at the gate; reclaims in 3 of 39 instrumented probe runs). Mohammed's hypothesis was that heartbeats only start once a
job gets a pool child, so a job waiting longer than its lease is reclaimed while
healthy.
- **Ruled out.** In `Worker._process` the heartbeat task starts before `_run_handler`
  takes the pool permit. The new `test_a_job_waiting_for_a_pool_child_keeps_its_lease`
  runs one child and a 0.5 s lease: job 1 spins 2.5 s while job 2 waits for the permit,
  and a sampler reads both entries' Redis idle time every 20 ms. It passes 8/8, with
  idle staying under the lease and no reclaims or reruns.
- **The test really can catch it.** With the hypothesized bug planted (the heartbeat
  starts inside the permit), it fails: "a lease lapsed: idle reached 612 ms", deliveries
  up to 3. The file was restored and sha256-verified.
- **Also ruled out.**
  - Event-loop stalls: worst 3 ms over 12 runs, one of which reclaimed.
  - Redis latency events: none ≥ 50 ms in 15 runs, and `aof_delayed_fsync` 0.
  - A Redis clock jump: a sampler compared Redis `TIME` with the host's monotonic clock
    every 20 ms for 420 s and 400 s. Worst skew was 36 ms isolated and 60 ms under load
    (the fast suite running alongside in a loop). A jump would be ≥ 400 ms, the lapse
    needed.
- **Not reproduced since.** 0 failures in 80 runs (40 isolated, 40 under that load),
  against 4 occurrences earlier that evening (the gate failure and 3 probe runs). Every
  one whose counters were captured came with reclaims (`reclaimed` 1–3). They happened right after heavy chaos
  runs in Docker Desktop, and a VM-level stall of Redis is the leading unconfirmed
  suspect.
- The test's assertions are unchanged. Its failure message now includes
  `reclaimed`/`lease_lost`/`heartbeats`, so a future failure (in CI, say) says which it
  was.

**Mutation checks** (review round; scripted, each file restored and sha256-verified):

```
config: suspect threshold not capped at max_deliveries  -> CAUGHT (3 failed: test_config, crash_isolation[2-*])
verifier: W1 ignores unexpected exits                   -> CAUGHT
verifier: W1 ignores error lines                        -> CAUGHT
verifier: I2 ignores stray effects                      -> CAUGHT
verifier: I2b ignores results of unaccepted jobs        -> CAUGHT
verifier: I3 tolerates a missing DLQ entry              -> CAUGHT
verifier: I3 ignores non-DEAD jobs in the DLQ           -> CAUGHT
verifier: I3 ignores hang_forever's error               -> CAUGHT
verifier: I5 ignores the PEL                            -> MISSED, test fixed -> CAUGHT
worker: heartbeats start only once a pool child is ours -> CAUGHT (keeps_its_lease: idle 612 ms)
```

**Checked and found fine** (reasoned through, no change):
- The put-back leaves the entry owned by the reaper that returned it. A stale previous
  owner's transitions get `LEASE_LOST` either way, and its commit is still first-wins.
- A suspect task that finishes before `_suspects.add` can't be counted: the add happens
  synchronously right after `create_task`, before the task first runs.
- The crashy subtraction in the report's histogram is exact even with lost reclaims:
  each delivery count is claimed and counted exactly once.
- The drain only starts after every fault is healed (the injector's tasks are awaited
  first), and the final SIGTERM drain leaves anything unfinished in the PEL, where I5
  sees it.

**Evidence**

```
$ make check-all > log 2>&1; echo "make check-all exit=$?"
make check-all exit=0
  68 files already formatted / All checks passed! / Success: no issues found in 61 source files
  ======================== 182 passed in 86.06s (0:01:26) ========================
$ make check > log 2>&1; echo "make check exit=$?"
make check exit=0
  ===================== 149 passed, 33 deselected in 17.01s ======================
```

### Phase 4: Chaos testing harness (2026-09-22)

**Built**
- **`chaos/` package, `make chaos N=… [CHAOS_WORKERS=8] [SEED=s]`** (ADR-036):
  - A generated Compose project: Redis on port 6390, one Toxiproxy 2.12.0 with a proxy
    per worker, and `worker-1..8`. The producer and the verifier connect to Redis
    directly (resolves ADR-011).
  - A seeded fault plan: kill + restart, pause for 1.5–3 leases, and Toxiproxy
    `reset_peer`, downstream `timeout` (lost replies), `latency`, and partition, each on
    one worker's proxy. Every kind is planned ≥ 4 times.
  - An injector that records what actually happened, and a supervisor that restarts
    crashed workers and counts restarts by exit code.
  - The mix enqueued at 2,000 jobs/s; then heal, drain, graceful stop, verify, and a JSON
    report. Worker logs and the accepted-job list go to `chaos/runs/`.
- **Verifier I1–I5** (`chaos/verifier.py`), counted from the append-only effects and
  results logs, the done hashes, and the DLQ. It is stricter than SPEC's wording in a few
  places: no late successes, no stray results or effects, the DLQ holds only the expected
  jobs, and crashy jobs never run past `max_deliveries` (checked from crash counts). I4
  also requires timeouts, pool resets, and crash restarts. The report adds each
  non-crashy job's highest delivery count (the ADR-008 margin).
- **The job mix** (`chaos/mix.py`): normal, flaky (k ≤ 3), slow (no heartbeat), `cpu_task`,
  and hang jobs of every kind. Per Mohammed's requirement these are the new built-in
  `hang` / `hang_thread` / `hang_process` handlers with a 2 s timeout: under chaos their
  timeouts cancel async runs, orphan threads, and **reset process pools** (~200 resets
  per 100K run). Plus always-hanging, poison, and crashy jobs.
- **Two system bugs fixed** (see "Things that went wrong"):
  - **ADR-035:** a crashy job's companions followed it into the DLQ (a false DEAD). Now an
    entry redelivered ≥ 3 times is a *suspect*, and each worker runs at most one at a
    time.
  - **ADR-038:** a worker that started while Redis was unreachable crashed (exit 1). It
    now waits.
- **`max_deliveries` / `max_attempts` sized** for chaos at 12 / 8, with a false-DEAD
  estimate (ADR-008): ≈ 1.5 × 10⁻³ per 100K run, driven by slow jobs. The observed
  highest delivery of any non-crashy job was 5–7 across all ten 100K runs.
- **Worker count: 8** (ADR-036). Docker has 10 CPUs and 7.75 GiB; 8 saturated workers plus
  Redis plus Toxiproxy fit in 10 CPUs. The committed 100K runs measured 3.1–4.8 CPUs in
  use.
- **Mutation tests** (`test_chaos_verifier.py`): ledger without NX → I2 fails; commit
  without its done check → I2b fails; retry without its ownership check → **the verifier
  can't see it** (ADR-037, below).
- New tests: 25, taking the suite from 139 to 164.
  - Crash isolation: 2 subprocess tests.
  - Reclaim suspects: 3 script tests.
  - Startup: 2 tests.
  - Built-in hang handlers: 1 test.
  - Verifier: 5 mutation tests and 9 rule tests.
  - Planner: 3 unit tests.
  - Two existing unit tests were extended (the built-in handler list, and the new key's
    hash tag).
- Phase 5's CI command is recorded: `make check-all` (ADR-034).

**Acceptance evidence** (Docker 29.4.3, 10 CPUs / 7.75 GiB, Redis 8.8.3):

```
$ make check > log 2>&1; echo "make check exit=$?"
make check exit=0
  69 files already formatted / All checks passed! / Success: no issues found in 61 source files
  ===================== 134 passed, 30 deselected in 15.00s ======================
$ make check-all > log 2>&1; echo "make check-all exit=$?"
make check-all exit=0
  ======================== 164 passed in 72.17s (0:01:12) ========================
```

**N = 100,000, three runs, all PASSED** on code `b8fca66`, the final Phase 4 code after the
review (random seeds; raw reports `results/local/chaos_report.json`,
`chaos_report_run2.json`, `chaos_report_run3.json`). Each report's `run.git` lists its
dirty paths: PROGRESS.md, docs/DECISIONS.md, docs/SPEC.md, and the renames of the older
reports into `results/local/chaos_history/`. `git diff b8fca66 -- src chaos tests Makefile
pyproject.toml uv.lock docker` was empty. Reproduce with each report's `run.reproduce`, e.g.
`uv run python -m chaos.run --jobs 100000 --workers 8 --concurrency 16 --seed 2102781500 --rate 2000 --fault-tail 20`.

```
seed        I1-I5+W1  SUCCEEDED/DEAD  dup results  dup effects  kills pauses net  crash    reclaimed  duplicates  timeouts  pool    max delivery   time
                                      (log)        (log)                           restarts            suppressed            resets  (non-crashy)
2102781500  PASS      99974 / 26      0            0            6     5      23   36       959        253         388       206     6 (limit 12)   93.5 s
1464630989  PASS      99974 / 26      0            0            5     7      20   36       800        266         369       188     6              79.6 s
622097151   PASS      99974 / 26      0            0            5     5      23   36       959        278         373       190     5              85.0 s
```

Each run: 100,000 accepted jobs.
- I1: 99,974 SUCCEEDED and 26 DEAD (20 poison and 3 hang_forever after 8 attempts, 3
  crashy at delivery 13), `late_successes` 0.
- I2: 97,824 effects logged for the 97,824 jobs with effects, each key exactly once.
  Effects suppressed: 326 / 312 / 277.
- I2b: 99,974 results, one per SUCCEEDED job.
- I3: the DLQ holds exactly those 26.
- I5: stream, PEL, and delayed set all 0.
- W1: 0 unexpected worker exits and 0 ERROR or non-JSON log lines.
- Planned faults skipped (a worker a crashy job had just killed can't be paused or
  killed): 1 / 0 / 2.
- Orphaned threads that finished late: 46–47. Consumers pruned under chaos
  (`consumers_pruned`): 37–41.
- `duplicates_suppressed` counts every redundant copy the scripts dropped: duplicate
  commits, and retry or DLQ moves that found the job already terminal.
- CPU (`docker stats` means): 4.3–4.8 CPUs in total, busiest worker 0.82–0.93, Redis
  0.23–0.25, Toxiproxy 0.33–0.36. Across all six committed 100K reports it's 3.1–4.8.
- Run 1's summary as printed:

```
==== chaos run PASSED (seed 2102781500, 100000 jobs, 8 workers) ====
  faults {'kills': 6, 'pauses': 5, 'network_windows': 23, ... 'skipped': 1} | crash restarts {'70': 36}
  processed 99974 dead 26 reclaimed 959 duplicates_suppressed 253 effects_suppressed 326 timeouts 388 lease_lost 66
  ... excluding crashy jobs: {2: 791, 3: 99, 4: 27, 5: 5, 6: 1} (max 6, max_deliveries 12)
```

**Every chaos run of this phase**, including the failures (none was dropped):

```
run                          code        N      result
debug 1 (seed 1)             uncommitted 5K     harness error after the run (run_dir path bug); found the startup bug (ADR-038)
debug 2 (seed 1)             uncommitted 5K     I1-I3,I5 ok; I4 FAIL kills 2 < 3 (32 s fault phase too short)
debug 3 (seed 2)             uncommitted 30K    I1-I3,I5 ok; I4 FAIL pauses 2 < 3 (36 s)
pre-commit (seed 236340187)  uncommitted 100K   PASSED
seed 1280290511              b4f6779     100K   PASSED
seed 1661764791              b4f6779     100K   FAILED: I3 crashy at delivery 14; I4 pauses 2 < 3
                                                (raw: results/local/chaos_failures/2026-09-22_seed1661764791_I3_I4.json)
seed 371414021               b4f6779     100K   PASSED
seeds 115930751, 2145370571, 425084029
                             464635e     100K   PASSED x3 (the first acceptance set; before W1)
3 acceptance runs (above)    b8fca66     100K   PASSED x3 (with W1)
```

Every one of those reports is committed except the three debug runs:
`results/local/chaos_history/` holds the pre-commit run, the two passing b4f6779 runs,
and the 464635e set; `results/local/chaos_failures/` holds the failed run.

The seed-1661764791 failure is written up below. The verifier's I3 expectation was wrong
and the fault plan left I4 to chance; the system was right. Both were fixed with
tests, and I4's minimums were not lowered. The 464635e set was the next three runs
after that fix; the b8fca66 set was the next three after the review's fixes.

Mutation checks. Scripted, one bug at a time, each file restored and sha256-verified:

```
reclaim.lua: suspects never held back               -> CAUGHT (crash_isolation)
worker: always offers a suspect slot                -> CAUGHT (crash_isolation[last])
worker: reclaimed suspects not tracked              -> CAUGHT (crash_isolation[last])
reclaim.lua: put-back keeps the bumped count        -> CAUGHT (at_most_the_suspects...)
reclaim.lua: put-back marks the entry fresh         -> CAUGHT
reclaim.lua: DLQ-bound entries count as suspects    -> CAUGHT (past_max_deliveries...)
worker: startup doesn't wait for Redis              -> CAUGHT (test_startup, both)
hang: async handler never hangs                     -> CAUGHT (built_in_hang_handlers)
verifier: crashy must be at exactly max+1           -> CAUGHT (rules[14])
verifier: crashy may be dead-lettered early         -> CAUGHT (rules[12])
verifier: no crash-count bound                      -> CAUGHT (ran_past_max_deliveries)
verifier: I1 ignores each kind's expected ending    -> CAUGHT (i1_requires...[DEAD])
verifier: I1 ignores late successes                 -> CAUGHT
verifier: I2 counts distinct keys, not log entries  -> CAUGHT (ledger_without_nx_fails_i2)
verifier: I2b counts distinct ids, not log entries  -> CAUGHT (commit_without_the_done_check)
verifier: I4 minimums never checked                 -> CAUGHT
planner: opening not guaranteed (random only)       -> CAUGHT (test_chaos_plan)
planner: opening rounds 2                           -> CAUGHT
planner: two faults on one worker at once           -> CAUGHT
retry.lua: ownership bypassed, vs Phase 2 tests     -> CAUGHT (3 failed)
```

The three SPEC §7 mutation checks are automated tests. Two of them catch their bug as
SPEC says:
- ledger without NX: I2 fails with "slow:… 2 effects";
- commit without its done check: I2b fails with "(slow, SUCCEEDED): 2 results".

The third, retry.lua without its ownership check, does **not** fail I1, I2, or I2b, and
can't. The test builds the exact window: A's stale retry lands while B still owns and
runs the entry. The mutant acts (`retried` = 1, against 0 in the control), but B's commit
is first-wins, the needless rerun is suppressed, and the ledger stops its effect. See
ADR-037.

**Decisions worth Mohammed's review**
- **ADR-037: the retry-ownership mutation check deviates from SPEC §7.** The verifier sees
  outcomes, and the terminal-state check, the entry's `XDEL`, first-wins commit, and the
  ledger each absorb a non-owner retry, so no outcome changes. The Phase 2 script and
  stale-worker tests enforce the check itself (3 fail under the mutant). Options: accept
  this, or add an append-only transition log so the verifier can count runs per
  attempt (new hot-path instrumentation, not added unasked).
- **ADR-035: suspect isolation in `reclaim.lua`.** Claim-and-put-back (`XCLAIM … IDLE
  RETRYCOUNT JUSTID`), chosen over peek-then-claim.
- **I3's crashy rule** is "past `max_deliveries`, and never run past it", not "exactly at
  `max_deliveries + 1`" (ADR-036).
- The flaky k comes from the seeded RNG at enqueue time, not a hash of the job_id: the
  ids are created inside `enqueue_many` (ADR-036).
- Chaos settings (lease 2 s, `max_deliveries` 12, `max_attempts` 8, 16 slots per worker)
  are the chaos run's, not new library defaults.

**Open issues** (as of the gate; the review below updates the first one)
- **A flaky Phase 3 test, found by this phase's repeat runs:**
  `test_waiting_for_a_pool_child_does_not_count_toward_the_timeout` failed 1 of 6 passes
  of the new-test set.
  - An instrumented copy (scratch only) ran 12 + 12 + 15 times with the probe added. Its
    two failures each had `reclaimed` 2–3 and `lease_lost` 2–3, with runs
    `[2, 2, 2]` / `[1, 2, 2]`. The jobs weren't timed out or reset: they were
    **reclaimed**, because their 0.5 s test lease lapsed.
  - Not the cause, as far as I can measure: the worker's event loop (worst stall 3 ms in
    12 runs, one of which reclaimed) and Redis (`latency-monitor-threshold 50`: no events
    in 15 runs; `aof_delayed_fsync` 0).
  - Root cause **unknown**. The test itself is unchanged: a longer lease there would hide
    this, and that's Mohammed's call. Production leases are 30 s.
- **Occasional slow test-process wall time (unexplained; may matter for Phase 5 CI
  runtime).** Measured:
  - In a 6-pass loop of the new-test set, pytest reported 34–37 s per pass, but three
    passes took **168 s, 169 s, and 125 s** of wall time (the other three: 38, 46, and 52
    s). In a separate 15-run loop of one test, one iteration took ~44 s instead of ~4 s.
  - Startup is fast: `uv run python` 0.03 s, importing ftq + redis + pytest 0.13 s.
  - No orphaned processes were left after any of 6 passes. (Pool children orphaned
    earlier came from a pytest process I killed mid-run.)
  - An exit-timing probe was **inconclusive**. It hooked `atexit` and ran 5 iterations;
    in all 5 the process exited within 0.1 s of the session ending (`atexit` reached
    after 0.0 s, process wall 80.2–80.4 s = session 80.0–80.2 s). But the probe was
    broken: its script had no `__main__` guard, so the spawned pool children re-ran
    `pytest.main`, and every session failed (rc = 1) and took 80 s. So it neither shows
    nor rules out the slowness, and all its files were deleted.
  - Where the extra time goes is still unknown: before the session, at exit, or in
    `uv`. None of these runs showed it in CI-shaped conditions.
  - **Phase 5:** time `make check-all` on the hosted runner, and if wall time far exceeds
    pytest's reported time, find the cause before setting job timeouts.
- 1M-job runs haven't been tried. Redis memory at 1M is estimated from ADR-010, not
  measured; the chaos Redis caps at 3 GB with noeviction. Phase 5 measures 1M.
- The retry-ownership check has no verifier-level evidence, by construction (ADR-037).
- A reaper holding a suspect claims and returns other suspects every pass: a few wasted
  commands (ADR-035).
- Carried: results/effects logs never trimmed (ADR-021); the private `_processes` map
  (ADR-028); batch-100 pipelining noise (ADR-032); the hysteresis flag only moves on an
  enqueue (ADR-031).

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

- **2026-09-23 (Phase 6 follow-up): I4 failed in CI again, and again the fault plan
  left it to chance.** CI #7 executed 2 of 4 planned kills; the other two hit a worker
  that a crashy job had just killed. Phase 4 guaranteed that every kind is *planned*
  ≥ 4 times and assumed at most one would be skipped. On 4 workers, with 36 crash
  restarts packed into a 77 s fault phase, two were. The fix makes a planned kill or
  pause wait for its worker instead of giving up (ADR-043). Lesson: "planned with a
  spare" isn't "executed". The guarantee has to hold at the point where the fault
  happens.

- **2026-09-23 (Phase 6): my first capacity number didn't survive a second session.**
  - The first scaling set (3 repeats, 10:09–10:24 UTC) put 8 workers at 14.1–14.8K
    jobs/s. Minutes later, the backpressure runs completed 20.5K/s on the same fleet,
    and the backpressure chart's title called 22K/s "1.5× capacity".
  - Two explanations fit: the saturation method costs Redis extra per job, or the
    machine changed. I tested both instead of picking one.
    - Interleaved saturation / fixed-rate pairs: saturation wasn't lower (19.0 / 24.8 /
      20.2K against 19.1 / 14.7 / 22.2K).
    - `INFO commandstats`, added for this: the same commands per job in every run.
    - Two more scaling repeats later: 8 workers at 22.7K and 25.7K.
  - Redis's main thread needed 62–65 µs per job at 8 workers in the first session and
    36–40 µs later, for identical work. The host changed, not the queue (ADR-042).
  - The scaling chart now shows each session separately, and the backpressure title
    states the offered rate, not a multiple of a capacity that moved.
  - Lesson: on a shared laptop, one session's median isn't "the capacity". Repeat
    across sessions, and check the per-unit cost, not just the total.
- **2026-09-23 (Phase 6): the benchmark found a latent connection-pool limit in the
  worker.**
  - At `FTQ_CONCURRENCY=100`, both runs logged one WARNING: `maintenance pass failed:
    network:MaxConnectionsError`.
  - redis-py 8.1's asyncio pool defaults to 100 connections and raises as soon as
    they're all in use. `make_redis` doesn't size it, and a worker's in-flight jobs,
    heartbeats, fetch, and maintenance can together need more than 100.
  - No job was affected (0 retries, reclaims, lease losses, or DLQ moves in any of the
    runs), but the limit is real. It's flagged as a separate task rather than changed
    inside a benchmark phase (ADR-042, open issues).
  - Found only because the driver saves every worker's log and the report counts the
    lines.
  - Fixed on 2026-09-23 (ADR-044). A test that pauses Redis with 100 jobs in flight
    failed 3 of 3 on the old code and passed 30 of 30 on the fix.
- **2026-09-23 (Phase 6): a committed measurement script had been broken since Phase 3.**
  `bench/lease_starvation.py` called `start_worker_process(..., **ENV)`. Phase 3
  (fdf7387) changed the helper to take `env=`, so the script raised `TypeError`. Nothing
  type-checked `bench/`. Adding `bench` to mypy's files (for the new harness) found it.
  It's fixed and re-run (exit 0; cpu_task max idle 198–201 ms against a 1 s lease). The
  committed Phase 2 output was produced before the break, so it stands.
- **2026-09-23 (Phase 6): my own leftover data slowed the test suite 2.3×.** After two
  smoke runs of the loadgen against the dev Redis (its default URL), `make check-all`
  took 270 s instead of ~118 s. Every integration test's teardown SCANs the whole
  keyspace for its queue's keys, and the smoke runs had left 56K keys (7-day TTLs).
  Deleting those two queues' keys restored 118.6 s. The benchmark driver never touches
  the dev Redis (its own Compose project, port 6391); only hand-run loadgen smoke tests
  did.
- **2026-09-23 (Phase 6): smaller slips, each caught before it cost a result.**
  - The first scaling chart had a conclusion for a title ("stops scaling at 4 workers")
    before any data existed. It's neutral now.
  - The first loadgen sent one job per call at low rates. It sent whatever had come due
    during the previous call instead of waiting for the tick. Caught in the smoke run
    (median batch 1); now it's 10 per tick at 1K/s per producer.
  - `argparse.REMAINDER` swallowed the driver's own flags and passed them to the loadgen
    (it failed, nothing was recorded).
  - I ran `bench/plot.py` on the host during the measurement window of
    `method/w08_saturate_r1`: a few CPU-seconds on a 10-core host, disclosed here and
    left in.
  - The io-threads runs with 2 producer processes never kept the backlog full (depth
    min 77–350). They measured the load generator (one ~40 ms call of 500 jobs in
    flight per process), not Redis. They were re-run with 4 producers. The 2-producer
    runs stay committed, and ADR-042 says what they are.

- **2026-09-23 (Phase 5): the pool-reset cascade.** CI's first chaos runs on a 4-vCPU
  runner failed I1/I3: healthy `hang_process` jobs ended DEAD (6, 22, and 35 false DEADs
  per 100K; ~115 and ~176 extra at 1M). The mechanism:
  1. A timeout resets the process pool (ADR-030).
  2. The runs in the new pool pay the pool's start-up on their own clocks (a new `spawn`
     interpreter, then the handler imports). With 8 saturated workers on 4 vCPUs that
     took 2 s or more, against a 2 s hang timeout.
  3. So they time out too, each one costing a healthy job an attempt, and each resets
     the pool again. Back to 2.
  The worker logs showed it: 219–337 timeouts per run of runs that can't hang (0
  locally), and 464–555 pool resets (~200 locally). A restarted bystander also kept its
  first run's deadline and sometimes timed out 15 ms after restarting. That fix
  (901d763) was real but not enough: the next run failed worse. The fix that worked
  (ADR-039): runs don't start their clocks until the pool is warm, and each run gets a
  full timeout. On the same oversubscribed setup it passed, with pool start-up measured
  at 6.4 s mean. ADR-030 had *measured* this start-up cost (~0.3 s locally) and accepted
  it. Lessons: a fixed cost on the timeout clock is a coupling that only shows under load
  (a laptop with headroom never showed it); and a failure that resets shared state (the
  pool) turns one timeout into many.
- **2026-09-23 (Phase 5): my first warm-up leaked-semaphore warnings, and W1 caught it.**
  A multiprocessing Barrier per pool passed every test, but the first local chaos run
  failed W1 on 8 non-JSON lines ("ResourceTracker called reentrantly … might leak").
  Replaced by warm-up rounds that allocate nothing; the rerun was clean.
- **2026-09-23 (Phase 5): a test that only passed without a CI env.** Rich forces styling
  when `GITHUB_ACTIONS=true`, which split `--all` into two styled spans, and
  `test_dlq_requeue_cli_argument_handling` failed on the runner. Reproduced locally with
  the variable set, and fixed by comparing text without escapes (assertion unchanged).
- **2026-09-23 (Phase 5): a chaos harness crash that lost a report.** `--run-dir` outside
  the repo raised `ValueError` *after* the run, in `relative_to`. Found by the first
  local smoke run; the path is now kept absolute.

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
- **2026-09-22 (Phase 4): jobs next to a crashing job were dead-lettered with it (a real
  bug, found by analysis and confirmed by a test).**
  - While sizing `max_deliveries` (ADR-008), I asked what happens to the other jobs a
    crashy job kills. Their entries expire at the same moment as the crashy one. A single
    `XAUTOCLAIM` batch then claims them all together, they crash the next worker
    together, and each crash adds a delivery to every one of them. They reach
    `max_deliveries` in the same step as the crashy job and go to the DLQ with it.
  - No `max_deliveries` value helps, because the companions' count always equals the
    crashy job's.
  - `test_crash_isolation.py` reproduced it before any fix: 4 innocent `send_email` jobs
    fetched with one crashy job all ended DEAD, whether the crashy job came first or last
    in the PEL.
  - Fix (ADR-035): an entry at delivery ≥ 3 is a suspect, and a worker runs one suspect
    at a time. Suspects it can't take are put back with their delivery count restored.
    Six mutants of the fix are caught.
  - Lesson: "size the limit against the faults" first needs a check that every source of
    redelivery is independent. This one wasn't.
- **2026-09-22 (Phase 4): a worker restarted inside a network partition crashed (found by
  the first chaos run).**
  - Debug run 1 recorded a restart with exit code 1, not the crashy 70. The worker's log
    held a `ConnectionError` traceback from `XGROUP CREATE`, the first command a worker
    sends.
  - The supervisor had restarted worker 7 (after a crashy job killed it) during a 2.1 s
    partition of that worker's proxy. The fetch loop waits out outages (ADR-022), but
    startup didn't.
  - Two tests reproduced it: a worker pointed at a port with nothing listening, with a
    TCP forwarder to Redis started later. Fix (ADR-038): startup retries like the fetch
    loop.
  - This is the kind of bug only a restart-during-fault schedule finds.
- **2026-09-22 (Phase 4): the verifier expected something the system rightly doesn't do,
  and the fault plan left I4 to chance.**
  - A 100K run (seed 1661764791, code b4f6779) failed I3: a crashy job was in the DLQ at
    delivery 14, and I3 demanded exactly `max_deliveries + 1` = 13. Crash restarts were
    exactly 36 = 3 × 12, so it never ran at 13. The worker logs showed several `reclaim
    failed (Error while reading … Connection reset by peer)`: a reclaim that ran in Redis
    and lost its reply. The entry sat claimed but unseen and was claimed again at 14.
    That's correct behaviour. The DLQ move of an over-limit entry is a transition a fault
    can interrupt too.
  - I3 now checks what safety needs: never dead-lettered with deliveries left, and never
    *run* past the limit. The second part is new: crash count ≤ crashy jobs × limit.
  - The same run failed I4 (2 pauses, minimum 3): after the opening round every fault was
    random, and this plan drew no more pauses. The plan now guarantees 4 of each kind.
    The minimums weren't touched.
  - All three acceptance runs are the next three after the fix, and the failed run's
    report is committed (`results/local/chaos_failures/`).
  - Lesson: an exact expected count under faults needs to say why no fault can change
    it. This one couldn't.
- **2026-09-22 (Phase 4): harness mistakes, each caught before it produced a result.**
  1. Debug run 1 finished and then crashed while writing its report. `run_dir` was
     relative, and `Path.relative_to` raised. The verifier result was lost (the worker
     logs had been saved). Fixed with `.resolve()`.
  2. A launch line chained `make chaos` after a `ruff check` that failed, so no run
     started. The wait loop then polled a log file that didn't exist until the 10-minute
     shell limit. Now every run is launched on its own line and its log is checked right
     away.
  3. The first stale-retry test let worker A's own reaper reclaim A's expired entry, so
     "B's run" was really A's. The control failed, which exposed it. A now has one slot
     (a busy worker doesn't reap) and no heartbeats, and B heartbeats.
  4. The supervisor could count a deliberately killed worker as crashed if the kill was
     healed between its two reads. It now checks the hold before and after reading
     states.
  5. That same stale-retry test asserted exact reclaim counts. Under load it failed with
     `reclaimed` 2 (`assert 2 == 1`): the needless retry sometimes lands on A, and B
     then reclaims it too. (A draft said "2 of 5 runs". Only one of that loop's two
     failures was identified as this test; the other wasn't captured.) It now asserts the mutant's actual signature (`retried` = 1) and the
     verifier's verdict, which don't depend on timing.
  6. Reports marked the tree dirty for untracked files (e.g. an `AGENTS.md` that isn't
     part of the project), and then for doc edits made during the runs. They now say
     which tracked paths differ, and PROGRESS states the code diff was empty.
- **2026-09-22 (Phase 4 review): my own fix had a hole, and my own verifier had tests that
  couldn't fail.**
  - ADR-035's suspect rule starts at delivery 3. Nothing stopped `max_deliveries` from
    being lower, and at 2 the rule never triggered, so the false-DEAD bug was back. The
    crash-isolation test only ever ran with `max_deliveries` 3.
  - Nine verifier failure branches had never been shown failing. When they got tests, one
    of the new tests couldn't fail either: it put a PEL entry in the stream too, so the
    stream check did all the work. A mutant caught it.
  - The docs quoted numbers from uncommitted reports and misread a log-line count as a
    counter.
  - Lessons:
    - A fix with a threshold needs a test at the threshold's boundary with the other
      limits.
    - A test for a check must make that check the *only* thing that can fail.
    - Every number in the docs gets traced to a committed file before it's written.
