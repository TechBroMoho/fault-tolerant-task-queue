# Results

Every number here is read from a raw result file committed in `results/`, written by a
committed script, and each one links to its file. Where a number was derived (a median, a
ratio), the derivation is stated. Nothing was rerun to look better: failed runs are listed
with the passing ones, and their reports are kept (`results/ci/chaos_failures/`,
PROGRESS.md "Things that went wrong").

- [Claims and evidence](#claims-and-evidence): the table SPEC §9 asks for.
- [Methodology](#methodology): what "throughput", "latency", and "exactly-once" mean here.
- [AWS benchmark](#aws-benchmark-phase-8): scaling, headline, backpressure.
- [Chaos tests](#chaos-tests-phases-4-5): every 1M-job run, per-push CI, the recording.
- [Local benchmark](#local-benchmark-phase-6): the laptop runs that validated the harness.
- [Cost](#cost), [what was not measured](#what-was-not-measured), and the
  [resume bullets](#resume-bullets) built from these numbers.

## Claims and evidence

| Claim | Measured value | Definition | Environment | Raw file(s) | Reproduce | Date (UTC) |
|---|---|---|---|---|---|---|
| **12 parallel workers on AWS** | 12 worker tasks running, 0 pending, one completed deployment, on 6 `c7i-flex.large` hosts (2 per host) | `aws ecs describe-services` during the headline run; the driver's per-point snapshot lists each task's EC2 instance and type | AWS us-west-2a, ECS on EC2 | [`describe-services-12-workers.json`](../results/aws/evidence/describe-services-12-workers.json) (taken 19:30:42 during headline r1), [`headline/w12_r1.services.json`](../results/aws/headline/w12_r1.services.json) | `make aws-bench` (below) | 2026-09-23 |
| **10K+ jobs/sec** | **19,240 jobs/s**, median of 3 runs (19,106 / 19,240 / 19,367; spread 1.4 %) | Jobs whose first commit landed inside a 300 s steady-state window, ÷ 300 s. `send_email` jobs (simulated I/O + one ledger effect), 100 B JSON payload, 50 jobs in flight per worker (`FTQ_CONCURRENCY=50`, see the note under Methodology), 12 workers, backlog held at ~20K | AWS, below | [`headline/w12_r1.json`](../results/aws/headline/w12_r1.json), [`w12_r2.json`](../results/aws/headline/w12_r2.json), [`w12_r3.json`](../results/aws/headline/w12_r3.json) | `make aws-bench BENCH_ARGS="--suites headline"` | 2026-09-23 19:27–19:58 |
| **Throttling intake during traffic spikes** | Offered 28,856 jobs/s (1.5 × the headline median) to 12 workers. **Reject mode:** accepted 17,681/s, **refused 1,424,186 jobs**. **Block mode:** producers **waited 73,325 times** (557 s in total over 8 producer processes), 0 refused. Queue depth stayed between the 150K low and 200K high watermarks: max 199,670 (reject) and 198,167 (block) over the whole run | Offered/accepted per second from the producers' own counts in the 120 s window; depth sampled once a second from Redis | AWS, below | [`backpressure/w12_reject.json`](../results/aws/backpressure/w12_reject.json), [`w12_block.json`](../results/aws/backpressure/w12_block.json), chart [`backpressure.png`](../results/aws/backpressure.png) | `make aws-bench BENCH_ARGS="--suites backpressure"` (needs the headline results) | 2026-09-23 19:59, 20:53 |
| **Crash tests run on every code change via GitHub Actions** | Every push runs a 100,000-job chaos test. It passed on the current code | `.github/workflows/ci.yml` triggers on `push` and `pull_request`; job `chaos` runs `chaos.run --jobs 100000` | GitHub-hosted `ubuntu-24.04`, 4 vCPUs, 4 workers | [`ci.yml`](../.github/workflows/ci.yml); green run [35919749932](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35919749932) (190344c), its report [`chaos_report_run35919749932_N100K_w4.json`](../results/ci/chaos_report_run35919749932_N100K_w4.json) | push a commit | 2026-09-23 21:05 |
| **Randomly killing workers and cutting connections mid-job** | Per 1M run: 25–35 `docker kill`s, 29–43 multi-second `docker pause`s (longer than the lease), 88–105 network faults (connection resets, black holes, latency, partitions) on single workers, 358–360 crash restarts; 8,958–9,275 jobs reclaimed from dead or paused workers, 2,565–2,929 duplicate results suppressed | Counts the harness executed and the queue's own counters, from each report's `faults` and `verifier` blocks; invariant I4 fails the run if any is below its minimum | as below | the 1M reports in the chaos table below | `gh workflow run chaos-scale.yml -f jobs=1000000` | 2026-09-23 |
| **0 lost jobs and 0 duplicate results across 1M+ tasks** | **10 of 10** runs of 1,000,000 jobs passed (3 of them on the final code, 190344c): every one passed I1–I5 (0 lost, 0 duplicate results, 0 duplicate effects, DLQ exactly the intended poison/crashy/hang-forever jobs, fully drained). **Where:** on-demand `workflow_dispatch` runs of `chaos-scale.yml`, not per push. A nightly schedule is configured but hasn't fired yet. Verifier mutation tests pass (see below) | The verifier checks every accepted job_id against the append-only results and effects logs (SPEC §4) | GitHub-hosted `ubuntu-24.04`, 4 workers | the 1M reports in the chaos table below | `gh workflow run chaos-scale.yml -f jobs=1000000` | 2026-09-23 |

## Methodology

**Throughput** (`throughput.completed_per_s`) is jobs whose first commit landed inside the
steady-state window, divided by its length. The window is [warmup, warmup + measure) on
Redis's clock (`TIME`, read inside the Lua scripts), so enqueue and completion times from
different machines are comparable (ADR-020). Warmup is 20 s and cooldown 5 s on AWS. The
count comes from the queue's append-only results log, not from the producers
(`bench/loadgen.py`, ADR-041).

**Saturation.** The scaling and headline runs keep a backlog: producers send batches back
to back and pause while the stream holds 20,000 jobs (`--max-depth 20000`), so workers
never wait for work and Redis never spends time refusing jobs. Because of that backlog,
**the end-to-end latency of those runs (p50 ≈ 1.0 s at 12 workers) is mostly queueing.**
It's not the latency of an idle queue. For latency against offered load below capacity,
see the local latency suite.

**Exactly-once check in a benchmark** (`exactly_once.ok`): the accepted count (summed
over every loadgen host) equals the number of distinct job_ids in the results log, no
job_id appears twice, the DLQ is empty, the queue drained, and every host reported. It
compares counts, not sets of job_ids. Redis is flushed before each point and the load
generator is the only producer, so a result can only belong to a job this run enqueued.
The chaos verifier is the identity-level check.

**Redis CPU** (`redis.main_thread_busy`) is the main thread's CPU seconds per wall second
inside the window, from `INFO cpu` sampled every second. 1.0 is one full core, and it's
Redis's ceiling: every command and Lua script runs on that thread.

**AWS environment** (every AWS report's `meta.environment`, `env`):
- us-west-2a, one subnet, ECS on the EC2 launch type (the Free plan doesn't list Fargate).
- Redis 8.8.3 on its own `m7i-flex.large` (2 vCPU, 8 GiB): `maxmemory 6500mb`,
  `noeviction`, `io-threads 1`, **no AOF and no RDB snapshots** (throughput runs;
  durability is out of scope for the benchmark; see [what was not measured](#what-was-not-measured)).
- Workers on 6 `c7i-flex.large` (2 vCPU, 4 GiB), 2 worker containers per host, one Python
  process each.
- 2 loadgen hosts (`c7i-flex.large`), 4 producer processes each (ADR-046/047).
- `FTQ_CONCURRENCY=50`: the Terraform default (`worker_concurrency`) at each run's commit
  (d853d21, 7510107, 3d43177), with no override in the recorded plan (PROGRESS). **The
  Phase 8 reports don't record it themselves.** The driver's snapshot records the
  worker's task-definition settings since the Phase 6–8 review.
- **Flex instances** advertise a 40 % CPU baseline with bursts to 100 %. The data shows
  no throttling: the four 12-worker saturated runs spread over 38 minutes (19:20–19:58)
  are 19,106–19,426 jobs/s with no downward trend.
- Reproduce the whole session (BILLABLE, ~$0.85/hour, needs the Phase 7 base stack):

  ```bash
  make aws-image
  make aws-plan TF_VARS="-var worker_hosts=6 -var workers=12 -var loadgen_hosts=2 -var max_vcpus=18 -var redis_maxmemory=6500mb"
  make aws-up
  make aws-bench BENCH_ARGS="--out results/aws-rerun"
  make aws-down
  ```

## AWS benchmark (Phase 8)

Charts: [`results/aws/scaling.png`](../results/aws/scaling.png),
[`results/aws/backpressure.png`](../results/aws/backpressure.png) (`uv run python -m
bench.plot --aws`). Table: [`results/aws/summary.md`](../results/aws/summary.md). Which
session each point came from: [`results/aws/README.md`](../results/aws/README.md).

| Point | Workers | Completed jobs/s | e2e p50 / p99 (ms) | Redis main thread | Exactly-once | Raw report |
|---|---|---|---|---|---|---|
| scaling | 1 | 3,564 | 6,106 / 6,679 | 26 % | True | [`scaling/w01.json`](../results/aws/scaling/w01.json) |
| scaling | 2 | 7,314 | 2,942 / 3,224 | 47 % | True | [`scaling/w02.json`](../results/aws/scaling/w02.json) (recovered, see below) |
| scaling | 4 | 13,375 | 1,575 / 1,821 | 75 % | True | [`scaling/w04.json`](../results/aws/scaling/w04.json) |
| scaling | 8 | 19,194 | 1,051 / 1,406 | 93 % | True | [`scaling/w08.json`](../results/aws/scaling/w08.json) |
| scaling | 12 | 19,426 | 1,023 / 1,379 | 96 % | True | [`scaling/w12.json`](../results/aws/scaling/w12.json) |
| headline r1 | 12 | 19,240 | 1,026 / 1,424 | 96 % | True | [`headline/w12_r1.json`](../results/aws/headline/w12_r1.json) |
| headline r2 | 12 | 19,106 | 1,032 / 1,443 | 96 % | True | [`headline/w12_r2.json`](../results/aws/headline/w12_r2.json) |
| headline r3 | 12 | 19,367 | 1,020 / 1,411 | 96 % | True | [`headline/w12_r3.json`](../results/aws/headline/w12_r3.json) |
| backpressure, reject | 12 | 17,784 | 9,749 / 13,616 | 99 % | True | [`backpressure/w12_reject.json`](../results/aws/backpressure/w12_reject.json) (recovered) |
| backpressure, block | 12 | 17,235 | 10,111 / 12,982 | 99 % | True | [`backpressure/w12_block.json`](../results/aws/backpressure/w12_block.json) |

Scaling and backpressure windows are 180 s and 120 s; headline windows are 300 s.

![AWS scaling](../results/aws/scaling.png)

**The bottleneck is Redis's single main thread** (ADR-042 found the same locally).
Throughput scales almost linearly to 4 workers (3.75 × one worker). By 8 workers, Redis's
main thread is 93 % busy and throughput stops rising: 8 → 12 workers adds 1 % (19,194 →
19,426) while the thread goes to 96 %. The workers weren't measured for CPU on AWS (see
below), but more of them made no difference, which is what a Redis-bound system shows.
Each job costs Redis three Lua scripts (enqueue, commit, ledger) plus its share of a
batched `XREADGROUP` (per-job command counts: every report's `redis_commands`).

![AWS backpressure](../results/aws/backpressure.png)

**Backpressure.** Offered load was 1.5 × the headline median (28,859 jobs/s target). In
**reject** mode, enqueue raises `QueueFull` above the 200K high watermark until depth falls
below 150K (hysteresis, ADR-031), so accepted load saw-tooths between those marks while
the offered rate stays flat. In **block** mode, the producers wait instead. Their open-loop
schedule falls behind (max lag 52 s), so "offered" equals "accepted" in that report and
the 28,859/s target is drawn as its own line. In both modes, completions held at the
fleet's capacity (17.2–17.8K/s; Redis is also serving the refused or waiting enqueues)
and **every accepted job completed exactly once** (2,759,680 and 2,685,015 jobs).

**Recovered reports.** The driver lost two reports while polling AWS (a race with
CloudWatch's log delivery, then a transient AWS CLI error; ADR-048). Both were read back
from the same run's own CloudWatch log stream by `python -m deploy.bench recover`, which
checks the label, suite, and run id and writes `meta.recovered`. They weren't reruns.
`w12_reject` has no `services.json` snapshot: it was held in memory when the driver
crashed.

**Phase 7 smoke** ([`results/aws/phase7/smoke.txt`](../results/aws/phase7/smoke.txt)):
`ftq bench --jobs 20000` as an ECS task on the small stack, all 20,000 completed exactly
once.

## Chaos tests (Phases 4–5)

How it works: README, "How the chaos test works", and ADR-036. Invariants: I1 no loss, I2
no duplicate effects, I2b no duplicate results, I3 DLQ correct, I4 the faults really
happened (it fails the run otherwise), I5 drained, W1 workers healthy.

**Every 1,000,000-job run on GitHub Actions**, generated by
`uv run python -m chaos.summary results/ci/chaos_report_run*_N1M_w4.json`:

| report | git | date (UTC) | jobs | workers | seed | result | SUCCEEDED | DEAD | I1 | I2 | I2b | I3 | I4 | I5 | kills | pauses | net faults | crash restarts | reclaimed | dup. results suppressed | dup. effects suppressed | max delivery* | s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| chaos_report_run35838910547_N1M_w4.json | c20d0fd | 2026-09-23 09:20 | 1,000,000 | 4 | 1644982976 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 33 | 33 | 96 | 360 | 9,183 | 2,792 | 2,928 | 8 | 2,070 |
| chaos_report_run35846621221_N1M_w4.json | c0f5657 | 2026-09-23 10:38 | 1,000,000 | 4 | 618731388 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 29 | 43 | 88 | 360 | 9,092 | 2,851 | 2,899 | 7 | 2,033 |
| chaos_report_run35846624796_N1M_w4.json | c0f5657 | 2026-09-23 10:38 | 1,000,000 | 4 | 400193833 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 34 | 30 | 102 | 360 | 9,145 | 2,755 | 2,961 | 7 | 2,055 |
| chaos_report_run35846628370_N1M_w4.json | c0f5657 | 2026-09-23 10:28 | 1,000,000 | 4 | 1816374322 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 28 | 42 | 97 | 360 | 9,151 | 2,565 | 2,496 | 7 | 1,429 |
| chaos_report_run35846631919_N1M_w4.json | c0f5657 | 2026-09-23 10:28 | 1,000,000 | 4 | 884433612 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 33 | 37 | 95 | 360 | 9,083 | 2,579 | 2,557 | 7 | 1,424 |
| chaos_report_run35846635450_N1M_w4.json | c0f5657 | 2026-09-23 10:39 | 1,000,000 | 4 | 261336795 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 35 | 35 | 97 | 360 | 9,275 | 2,774 | 2,941 | 7 | 2,111 |
| chaos_report_run35846639056_N1M_w4.json | c0f5657 | 2026-09-23 10:38 | 1,000,000 | 4 | 1393279984 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 29 | 29 | 105 | 358 | 8,958 | 2,757 | 2,850 | 6 | 2,043 |
| chaos_report_run35921059844_N1M_w4.json | 190344c | 2026-09-23 21:47 | 1,000,000 | 4 | 526828304 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 33 | 32 | 98 | 360 | 9,224 | 2,799 | 2,956 | 7 | 2,050 |
| chaos_report_run35921067352_N1M_w4.json | 190344c | 2026-09-23 21:46 | 1,000,000 | 4 | 1442412128 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 25 | 40 | 95 | 360 | 9,098 | 2,794 | 2,845 | 10 | 1,948 |
| chaos_report_run35921074863_N1M_w4.json | 190344c | 2026-09-23 21:48 | 1,000,000 | 4 | 485044386 | PASS | 999,740 | 260 | ok | ok | ok | ok | ok | ok | 29 | 37 | 95 | 360 | 9,214 | 2,929 | 2,964 | 7 | 2,069 |

*max delivery: the highest delivery count of any non-crashy job.

- **10 of 10 passed**, 10 distinct seeds, each 999,740 SUCCEEDED and exactly the 260
  intended DEAD (200 poison, 30 hang-forever, 30 crashy). Run links:
  `https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/<id>`, with the
  id in each file name.
- **Delivery-count margin:** the highest delivery count of a non-crashy job was 6–8 in
  the first seven runs and 10 in one run on 190344c, against `max_deliveries` 12
  (ADR-008). None came near being wrongly dead-lettered, but the margin is 2, not 4.
- **Code:** c20d0fd and c0f5657 have the same `src/`. 190344c adds the connection-pool
  fix (ADR-044) and the fault injector's wait-for-restart (ADR-043); the 190344c runs were
  dispatched in the Phase 6–8 review so that the 1M claim covers the final queue code.
- **Failed 1M runs,** all before the ADR-039 fix and all kept:
  [`run35827263800`](../results/ci/chaos_failures/run35827263800_N1M_seed1053529954_I1_I3.json)
  and [`run35828117698`](../results/ci/chaos_failures/run35828117698_N1M_seed22258524_I1_I3.json),
  8 workers on a 4-vCPU runner. The chaos test found a real bug: under CPU starvation,
  per-job timeouts cascaded into false DEAD jobs (PROGRESS "Things that went wrong";
  ADR-039).
- **Per push (100K jobs, 4 workers):** every push runs it. The latest, on 190344c:
  [`chaos_report_run35919749932_N100K_w4.json`](../results/ci/chaos_report_run35919749932_N100K_w4.json).
  One per-push failure since the fix (CI #7, [`run35854442425`](../results/ci/chaos_failures/run35854442425_seed845664227_I4.json))
  was the harness, not the queue: I4 found too few kills because the injector skipped a
  kill aimed at a worker that was already down. All the queue invariants passed in that
  run. Fixed in ADR-043.
- **Mutation tests** (`tests/integration/test_chaos_verifier.py`, part of `make check-all`
  and CI): bypassing the ledger's NX check fails I2; bypassing the commit's `done` check
  fails I2b. Removing the ownership check from the retry script is invisible to the
  verifier, and an automated test asserts exactly that (ADR-037: first-wins commit, the
  entry's `XDEL`, and the ledger absorb a non-owner's retry). 3 Phase 2 stale-worker and
  script tests catch that mutant; that was checked by hand in Phase 4.

**Terminal recording.** [`results/local/chaos_demo.cast`](../results/local/chaos_demo.cast)
is an asciicast of `make chaos-record N=100000`: 100,000 jobs, 8 workers, seed
2034644800, on this Mac, code 190344c. It shows the fault schedule firing, the progress
lines, and the verifier's verdict. Idle gaps over 2 s are shortened on playback. The
transcript is [`chaos_demo.txt`](../results/local/chaos_demo.txt) and the report
[`chaos_demo.json`](../results/local/chaos_demo.json). Replay:
`uvx --from asciinema==2.4.0 asciinema play results/local/chaos_demo.cast`.

## Local benchmark (Phase 6)

MacBook Pro M4, Docker Desktop with 10 vCPUs, where Redis, the workers, and the load
generator share one VM. These runs validated the harness and located the bottleneck. They
aren't the headline. Every report, chart, and the full table:
[`results/local/bench/`](../results/local/bench/),
[`summary.md`](../results/local/bench/summary.md). Local Redis ran with AOF `everysec`
(unlike AWS).

- 59 points, every one exactly-once.
- **Scaling:** the saturated throughput with ≥ 4 workers was 11,160–25,717 jobs/s. It
  depended on the session: the same commands per job cost Redis 36–40 µs in one session
  and 62–65 µs in the other (ADR-042 §4). So a local number is quoted as a range per
  worker count, never as one capacity.
- **Bottleneck:** 1–2 workers are CPU-bound (each ~1.0 core). From 4 workers up, Redis's
  main thread is 0.88–0.94 busy and the workers wait (0.42–0.77 of a core each).
  ([`bottleneck.png`](../results/local/bench/bottleneck.png)).
- **Latency below capacity** (8 workers, open loop, 10–90 % of capacity): end-to-end p50
  2–6 ms, p99 41–55 ms, except a 241 ms p99 at the lowest load (a VM stall, not
  explained further). ([`latency.png`](../results/local/bench/latency.png)).

## Cost

About **$1.56 at list prices** for all AWS work (Phase 7 $0.06; Phase 8 $0.26 + $1.13 +
$0.11). These are estimates from instance-hours × Pricing API prices (PROGRESS spend log),
paid from Free plan credits. The Budgets API's ActualSpend was $0.00 at 21:20 UTC on
2026-09-23, last refreshed at 14:22 UTC, before any session. Billing lags about 24 h.
Every session ended with `make aws-down` and a CLEAN `make aws-verify-clean`.

## What was not measured

- **Worker CPU on AWS.** Locally, cgroup CPU shows the workers waiting on Redis. On AWS
  only Redis's own CPU was sampled; the ECS task-metadata stats endpoint wasn't wired in.
  The AWS bottleneck claim rests on Redis's main thread at 93–96 % and on 8 → 12 workers
  adding 1 %.
- **Redis `io-threads` on AWS.** Locally, 4 io-threads was slower (the VM was out of
  CPU; ADR-042). Phase 8 ran with 1 and didn't repeat the comparison on a dedicated host.
- **Durability under a Redis crash.** AOF was off for the AWS runs, and no run killed Redis
  itself. The zero-loss claim covers worker crashes, pauses, and network faults, with
  Redis as the trust boundary (SPEC §4).
- **Latency at production-like load on AWS.** Every AWS point was saturated, so its
  latency is queueing. The only below-capacity latency curve is local.
- **The nightly 1M run.** It's configured (`chaos-scale.yml`, 09:23 UTC) but hasn't fired
  yet. GitHub doesn't guarantee scheduled runs. Every 1M result above was dispatched by
  hand.

## Resume bullets

Written for a non-technical reader, using only measured numbers. Each number links to its
evidence.

```
Fault-Tolerant Task Queue | Python, Redis, Docker, AWS ECS, GitHub Actions
• Built a distributed task queue in Python and Redis that splits background jobs (like
  sending emails) across 12 parallel workers on AWS, completing 19,000+ jobs per second,
  and automatically holds back or turns away new work when traffic exceeds capacity
• Designed automated crash tests that run on every code change via GitHub Actions,
  randomly killing workers and cutting their connections mid-job; across 10 test runs of
  1 million jobs each, 0 jobs were lost and 0 results were duplicated
```

| Phrase | Evidence |
|---|---|
| 12 parallel workers on AWS | [describe-services: 12 running](../results/aws/evidence/describe-services-12-workers.json); 6 hosts × 2, [snapshot](../results/aws/headline/w12_r1.services.json) |
| 19,000+ jobs per second | 3 five-minute runs: [19,240](../results/aws/headline/w12_r1.json), [19,106](../results/aws/headline/w12_r2.json), [19,367](../results/aws/headline/w12_r3.json); median 19,240. Every run is above 19,000 |
| holds back or turns away new work when traffic exceeds capacity | at 1.5 × capacity: [1,424,186 jobs turned away](../results/aws/backpressure/w12_reject.json) (reject mode), [73,325 waits](../results/aws/backpressure/w12_block.json) (block mode); queue stayed under its 200K limit |
| crash tests on every code change via GitHub Actions | [`ci.yml`](../.github/workflows/ci.yml) runs a 100,000-job chaos test on every push; [green run on the final code](https://github.com/TechBroMoho/fault-tolerant-task-queue/actions/runs/35919749932) |
| killing workers and cutting connections mid-job | ~30 kills, ~35 pauses, ~100 network faults per 1M run (chaos table above) |
| 10 runs of 1 million jobs, 0 lost, 0 duplicated | the 1M reports in the chaos table above, all I1–I5 passing |

**What changed from the target bullets (SPEC §0), and why:**
- "10K+ jobs/sec" → **19,000+**: the measured number is higher, so the bullet uses it.
  It's the median-of-3 headline with every run above 19,000, not a best run.
- "throttling intake during traffic spikes" → "holds back or turns away new work when
  traffic exceeds capacity". That's what was tested: a sustained 1.5× overload, in both
  modes. No short spike was tested.
- "0 lost jobs and 0 duplicate results across 1M+ tasks" on every code change isn't
  true as a single claim: every code change runs **100K** jobs, and the 1M runs were
  started by hand (the nightly schedule hasn't fired yet). The bullet keeps the two
  apart: crash tests on every change, and a count of 1M runs. If the interviewer asks,
  "on every push, 100K jobs; 1M-job runs on demand, ~35 minutes each".
- "Duplicate results" is kept as "results were duplicated". A job *can* run twice
  (at-least-once delivery), but its result and its side effect are recorded once. If
  asked: "no job's effect happened twice, even though some jobs ran twice".
