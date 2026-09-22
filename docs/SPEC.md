# SPEC: Fault-Tolerant Distributed Task Queue

> This is the full project specification. Claude Code: read this file, `CLAUDE.md`, and `PROGRESS.md` at the start of every session. This file is the source of truth. If you believe something here is wrong or suboptimal, say so and record the decision in `docs/DECISIONS.md`. Do not silently deviate.

---

## 0. Context: who this is for and why it exists

- **Owner:** Mohammed, a UC Berkeley CS + Data Science student applying for SWE/MLE internships (summer 2027).
- **Purpose:** a portfolio project for his resume. It will be public on GitHub and discussed in interviews.
- **How it's built:** Claude Code is the primary implementer. Mohammed reviews each phase and studies the finished code afterward. So:
  - **Clarity beats cleverness.** Prefer well-known, explainable mechanisms over novel ones.
  - **Every non-obvious design decision gets written down** in `docs/DECISIONS.md` with the alternatives considered and why this one won.
  - **Every number must be real, measured, and reproducible.**

### Target resume bullets (placeholders, not facts)

```
Fault-Tolerant Task Queue | Python, Redis, Docker, AWS ECS, GitHub Actions
• Built a distributed task queue in Python and Redis that splits background jobs (emails, file
  processing) across 12 parallel workers on AWS, handling 10K+ jobs/sec and throttling intake
  during traffic spikes
• Designed automated crash tests that run on every code change via GitHub Actions, randomly killing
  workers and cutting connections mid-job, confirming 0 lost jobs and 0 duplicate results across
  1M+ tasks
```

The numbers (12 workers, 10K+ jobs/sec, 1M+ tasks) are **ambition targets**. Work hard to reach them with legitimate engineering. If reality differs, report the real numbers and propose reworded bullets at the end. **Never fabricate, extrapolate, or cherry-pick a number to match a bullet.** Section 9 maps every claim to the evidence that must back it.

---

## 1. What we're building (plain English)

Web apps constantly need work done "in the background": sending a welcome email, resizing an upload, generating a report. The web server drops a job into a queue and responds instantly, and a fleet of worker processes picks jobs up and does them.

The hard part is not the happy path. It's guaranteeing that **every job gets done and its effect happens once**, even when:

- a worker crashes halfway through a job,
- the network between a worker and Redis drops,
- a job keeps failing (a "poison" job),
- producers submit jobs faster than workers can finish them.

This project is a Celery/Sidekiq-style queue built from scratch on Redis Streams. It is load-tested on AWS and proven correct by an automated chaos test that deliberately breaks things.

---

## 2. Goals and non-goals

### Goals

1. A Python library + CLI: producers `enqueue()` jobs; workers run registered handlers.
2. **At-least-once delivery** with **effectively-once side effects** via idempotency keys.
3. Visibility timeouts (leases) with heartbeats, so crashed workers' jobs get reclaimed.
4. Retries with exponential backoff + jitter, a dead-letter queue (DLQ), and poison-job detection.
5. Backpressure: producers are throttled or rejected when the queue is too deep.
6. A chaos-testing harness that kills workers and injects network faults, then verifies invariants.
7. CI on GitHub Actions: lint, types, tests, and the chaos test on every push.
8. Horizontal scaling benchmark on AWS ECS (EC2 launch type), with charts.

### Non-goals (do not build these)

- Exactly-once *delivery* (impossible in general; we claim effectively-once *effects* and explain why).
- Kubernetes, multi-region, Redis Cluster sharding (a short design note on how sharding would work is welcome in DECISIONS.md).
- A web UI or dashboard (Grafana is a stretch goal only).
- Workflows/DAGs, cron scheduling, job priorities (priorities are a stretch goal).

---

## 3. Non-negotiable rules

1. **Honest numbers.** Every metric in README/RESULTS must come from a saved raw result file produced by a committed script, with the exact command to reproduce it. If a run fails or looks wrong, investigate. Never discard it silently or rerun until it looks good. When you report results, report medians over repeated runs where noted.
2. **Cost safety (AWS Free plan).** This account is on the AWS **Free plan** with credits (about $100 now, up to $200 total). **If the credits run out, the account is automatically closed** and resources are lost. Therefore:
   - Never create billable resources without first printing an itemized cost estimate and getting an explicit "yes" from Mohammed in chat.
   - Set up an AWS Budgets alert before deploying anything (Phase 7).
   - Tear down after every AWS session and verify the teardown.
   - Target total AWS spend: **under $15** for the whole project.
3. **Secrets.** Never commit credentials, `.env` files, Terraform state, or AWS account IDs. Use a named AWS CLI profile. Add `.gitignore` entries in Phase 0.
4. **Tests are the spec.** Never weaken, skip, or delete a test to make it pass. If a test reveals a real limitation, document it in DECISIONS.md and discuss it.
5. **No fake verification.** "It should work" is not verification. Run the command and report the actual output.
6. **Phase gates.** At the end of each phase: run the acceptance checks, update `PROGRESS.md`, append to `docs/DECISIONS.md`, commit, then **stop and report** (see §11).
7. **Don't mock the thing under test.** Integration and chaos tests use a real Redis (Docker). Mocks are fine only for pure-logic unit tests.
8. **Ask when blocked.** If something is missing (Docker not running, no AWS credentials, quota too low), stop and ask. Don't work around it by faking or silently reducing scope.

---

## 4. Semantics: the contract the system guarantees

Document these in the README under "Guarantees and non-guarantees". They are the heart of the interview story.

| Property | Guarantee |
|---|---|
| Delivery | **At-least-once.** A job may be delivered more than once (e.g., after a crash). |
| Side effects | **Effectively-once** for effects committed through the idempotency mechanism. |
| Ordering | **No global ordering guarantee.** Document why (parallel workers, retries). |
| Durability | Jobs survive worker crashes and network faults. Redis itself is a trust boundary. Redis data loss (no persistence, crash) is out of scope for the zero-loss claim; document AOF settings and the trade-off. |
| Loss | A job accepted by `enqueue()` (call returned successfully) must end in exactly one terminal state: `SUCCEEDED` or `DEAD` (in the DLQ). |

### Job lifecycle

```
enqueue ──► PENDING (in stream, undelivered)
              │ XREADGROUP
              ▼
           IN_FLIGHT (in consumer group's Pending Entries List; lease held by a worker)
     ┌────────┼──────────────────────────────┐
     │ success│ handler raised                │ worker crashed / lease expired
     ▼        ▼                               ▼
 SUCCEEDED  RETRY_SCHEDULED (delayed set)   reclaimed by another worker (XAUTOCLAIM)
            │ due time reached                → back to IN_FLIGHT (delivery count +1)
            ▼
         PENDING again ... after max attempts / max deliveries ──► DEAD (DLQ)
```

### Required mechanisms (recommended design; deviations need a DECISIONS.md entry)

- **Transport:** Redis Streams + one consumer group per queue. Workers use `XREADGROUP ... COUNT n BLOCK ms`.
- **Commit (the critical section):** a Lua script that atomically:
  1. checks `done:{job_id}`; if already set, this is a duplicate delivery → just ack, increment `duplicates_suppressed`, return;
  2. otherwise sets `done:{job_id}` (with a generous TTL), records the result, then `XACK` + `XDEL` the entry.
  Deleting after ack keeps `XLEN` equal to "undelivered + in-flight". **Never use `XTRIM MAXLEN`**, which can delete unacked entries, i.e. data loss. Explain this in DECISIONS.md.
- **Side-effect ledger:** example handlers perform "effects" (e.g., "send email") through an `EffectLedger`. It emulates a downstream API that accepts idempotency keys (like Stripe's) with one atomic Lua script: `if SET NX ledger:<key> then XADD effects_log <key>`. The **append-only effects log** is what the chaos verifier counts. A `SET NX` key alone can never show a duplicate, so it can't be the evidence. Likewise, the commit script appends the job_id to an **append-only results log** after its `done` check, which backs the "0 duplicate results" claim. Document honestly that effects on external systems are only effectively-once if those systems honor idempotency keys.
- **Ownership and terminal-state rules (critical; get these right and test them):**
  - `XACK` and `XCLAIM` do **not** check who owns an entry. So every non-commit state change (retry scheduling, both DLQ paths, heartbeat/lease extension) must be a Lua script that first checks, via `XPENDING <stream> <group> <id> <id> 1`, that **the caller is the current owner** and that the job has no terminal state yet. Otherwise it does nothing and returns `LEASE_LOST`.
  - This prevents:
    - a stale worker (paused, then reclaimed) from scheduling a retry for a job another worker already completed;
    - two workers' heartbeats from stealing the lease back and forth.
  - **Commit is first-wins and idempotent:** any holder may commit. A later commit sees `done` and becomes a suppressed duplicate.
  - Record the terminal state in one key per job (`SUCCEEDED` or `DEAD`) using compare-and-set. Decide and document the precedence when a slow owner's success arrives after the job was moved to the DLQ. Recommended: a late success replaces DEAD (the work was done); DEAD never replaces SUCCEEDED; count "late successes" as a metric.
  - Every transition out of the stream (commit, retry, DLQ) does `XACK` + `XDEL`, so `XLEN` stays meaningful.
  - If the reply to a commit is lost (connection reset), retry the **commit** (it's idempotent). Never treat a lost commit reply as a handler failure.
- **Redis client timeouts:** set `socket_timeout`, `socket_connect_timeout`, `health_check_interval`, and a bounded reconnect/retry policy. Without them, a black-holed connection (Toxiproxy `timeout` toxic) can hang a worker forever and stall the chaos drain.
- **Idempotent enqueue:** optional producer-supplied `idempotency_key`. A duplicate enqueue within a TTL returns the original `job_id` instead of creating a new job.
- **Visibility timeout / leases:** default 30s (configurable; tests use ~1–2s). Each worker runs a reaper loop using `XAUTOCLAIM` with `min-idle-time = visibility_timeout` to take over stale jobs.
- **Heartbeats:** long-running handlers extend their lease periodically (e.g., `XCLAIM` to self with `JUSTID`, which resets idle time without bumping the delivery counter; verify this against Redis docs).
- **Retries:** on handler exception, atomically (Lua, after the ownership check described below) ack and delete the entry and add the job with `attempt+1` to a delayed sorted set scored by due time. Use exponential backoff with full jitter: `sleep = random(0, min(cap, base * 2^attempt))`. A scheduler loop (in every worker, safe to run concurrently because the move is an atomic Lua script) moves due jobs back into the stream.
- **Poison detection:** two paths to the DLQ:
  - `attempt >= max_attempts` (the handler keeps raising), and
  - delivery count from `XPENDING` > `max_deliveries` (the job keeps crashing the worker).
- **DLQ:** a separate stream storing the job, the last error, the attempts, and a timestamp. CLI: `dlq list`, `dlq requeue <id|--all>`.
- **Backpressure:** `enqueue()` checks queue depth (`XLEN` + delayed set size) against a high watermark:
  - mode `reject`: raise `QueueFull` (optional HTTP API returns `429` + `Retry-After`);
  - mode `block`: await with timeout until depth < low watermark (hysteresis).
  Cache the depth check briefly (e.g., 50–100 ms) so it doesn't double Redis load. Document the staleness trade-off.
- **Graceful shutdown:** on SIGTERM, stop fetching, let in-flight jobs finish within a grace period, then exit. Unfinished jobs stay in the PEL and will be reclaimed.
- **Timestamps for latency:** use Redis `TIME` (a single clock) for enqueue/complete timestamps used in cross-machine latency math. Explain why (clock skew).
- **Key naming:** `ftq:{<queue>}:stream`, `:dead`, `:delayed`, `:done:<job_id>`, `:ledger:<key>`, etc. Hash-tag braces keep a queue's keys in one Redis Cluster slot; explain in DECISIONS.md even though we don't use Cluster.

---

## 5. Tech stack (pin versions in the lockfile)

- **Language:** Python 3.12, `uv` for env/deps, `src/` layout, package name `ftq`.
- **Libraries:** `redis` (redis-py, asyncio API), `pydantic` v2 + `pydantic-settings`, `typer` (CLI), stdlib `logging` with JSON formatting (or `structlog`), `httpx` (Toxiproxy API), `matplotlib` (charts). Optional: `fastapi` + `uvicorn` for the enqueue API, `prometheus-client`.
- **Testing:** `pytest`, `pytest-asyncio`, `pytest-timeout`; `hypothesis` is optional for property tests.
- **Quality:** `ruff` (lint + format), `mypy --strict` on `src/`.
- **Infra:** Docker + Docker Compose v2, Redis 7.4+ (pin the image tag), Toxiproxy (`ghcr.io/shopify/toxiproxy`, pinned), Terraform ≥ 1.6, AWS CLI v2.
- **Dev machine:** Mohammed is on macOS (likely Apple Silicon). Images for AWS x86 instances must be built for `linux/amd64` with `docker buildx`. Otherwise use arm64 (Graviton) instances consistently. Decide explicitly.
- **Verify fast-moving APIs** (redis-py async, Terraform AWS provider, ECS AMIs) against official docs or `--help` before relying on memory.

---

## 6. Repository layout (target)

```
.
├── CLAUDE.md
├── PROGRESS.md                 # Claude-maintained status log (see §11)
├── README.md
├── Makefile                    # single entry point for all commands
├── pyproject.toml / uv.lock
├── docker/Dockerfile           # multi-stage, non-root, small
├── docker-compose.yml          # redis, toxiproxy, workers (scalable), producer/loadgen
├── src/ftq/
│   ├── __init__.py
│   ├── config.py               # pydantic-settings; every knob documented
│   ├── models.py               # Job schema (job_id = ULID/UUIDv7, type, payload, attempt, idem key, enqueued_at)
│   ├── client.py               # enqueue / enqueue_many (pipelined) / backpressure
│   ├── worker.py               # fetch loop, concurrency cap, handler dispatch, heartbeats, shutdown
│   ├── reaper.py               # XAUTOCLAIM loop, max-deliveries → DLQ
│   ├── scheduler.py            # delayed-retry mover
│   ├── ledger.py               # EffectLedger
│   ├── dlq.py
│   ├── metrics.py              # counters (processed, retried, dead, reclaimed, duplicates_suppressed, rejected)
│   ├── scripts/*.lua           # atomic operations, commented line by line
│   ├── handlers/               # send_email (simulated I/O), cpu_task (hashing), flaky(p), poison, crashy
│   └── cli.py                  # ftq worker | enqueue | stats | dlq | bench
├── tests/{unit,integration}/
├── chaos/                      # orchestrator + verifier + fault schedule
├── bench/                      # load generator, analysis, charts
├── deploy/terraform/           # AWS infra
├── results/{local,ci,aws}/     # raw JSON + charts (committed)
└── docs/
    ├── SPEC.md
    ├── DECISIONS.md            # ADR-style log
    └── RESULTS.md              # methodology + every claimed number with evidence
```

---

## 7. Phased plan with acceptance criteria

Phases 0–6 are local and cost $0. Phases 7–8 cost money and require explicit approval.

### Phase 0: Bootstrap and plan
- Create the layout, `pyproject.toml`, `Makefile` (`setup`, `fmt`, `lint`, `typecheck`, `test`, `check` = all of those, `up`, `down`, `chaos`, `bench`, `aws-*` stubs), `.gitignore`, and a Compose file with Redis.
- Create `PROGRESS.md` and `docs/DECISIONS.md` (first entries: why Redis Streams over Lists; why Python; why ECS on EC2 rather than Fargate, see Phase 7).
- **Acceptance:** `make check` passes; `docker compose up -d redis` + a trivial ping test passes.
- **Report:** your implementation plan for Phases 1–3 (brief) and any questions. **STOP.**

### Phase 1: Core queue (single worker, happy path + idempotency)
- Job model, `enqueue`, worker fetch loop, handler registry, commit Lua script, idempotent enqueue, EffectLedger, graceful shutdown.
- **Tests (real Redis):** enqueue → processed → result stored; idempotent enqueue returns the same id; a forced re-delivery of an already-done job is suppressed (ledger count stays 1, `duplicates_suppressed` increments); SIGTERM lets in-flight work finish.
- **Acceptance:** `make check` green. **STOP.**

### Phase 2: Reliability
- Leases + reaper (`XAUTOCLAIM`), heartbeats, retries with backoff + jitter, delayed scheduler, DLQ (both paths), DLQ CLI.
- **Tests:**
  - A worker killed mid-job (simulate by abandoning a delivered entry) → another worker reclaims after the timeout and completes it exactly once.
  - A flaky handler eventually succeeds.
  - A poison job lands in the DLQ with the correct attempts.
  - A crash-looping job hits max deliveries and goes to the DLQ.
  - The backoff schedule is within bounds (unit test).
  - A long job with heartbeats is NOT reclaimed.
  - **Stale-worker test:** worker A takes a job and stalls past the lease; worker B reclaims and commits it; A then resumes and either raises (its retry must return `LEASE_LOST` and schedule nothing) or commits (it must be suppressed as a duplicate). Either way, the job ends with exactly one terminal state and one effect.
  - Two workers heartbeating the same job cannot steal the lease back and forth.
- **Acceptance:** `make check` green; the tests run in < 60s total (short timeouts in test config). **STOP.**

### Phase 3: Concurrency, backpressure, observability
- Multiple worker processes; per-worker in-flight cap (asyncio semaphore / prefetch); batching and pipelining where it matters (measure before and after, and note it in DECISIONS.md).
- Backpressure in both modes with hysteresis; optional FastAPI enqueue endpoint returning 429.
- `ftq stats` (depth, in-flight, delayed, DLQ size, counters); JSON logs carrying job_id, attempt, worker_id. The default log level must NOT log every job at INFO: it drowns CI output and, on AWS, CloudWatch ingestion would cost real money at 10K jobs/sec.
- **Tests:** backpressure engages above the high watermark and releases below the low watermark; multi-worker run completes N jobs exactly once.
- **Acceptance:** `make check` green; a local demo `make up WORKERS=4` + `ftq bench --jobs 50000` completes, and you show the stats output. **STOP.**

### Phase 4: Chaos testing harness (the centerpiece)
- Compose topology: **one Toxiproxy proxy per worker** (`worker_i ──► proxy_i ──► redis`), so network faults can be partial (one worker partitioned) rather than a global Redis outage. The producer and the verifier connect to Redis **directly**, so "accepted" is unambiguous. If you ever route the producer through a proxy, the verifier must handle enqueues whose reply was lost.
- The orchestrator (`chaos/run.py`, seeded and reproducible; print the seed):
  1. Enqueues N jobs:
     - **normal** jobs;
     - **flaky** jobs that fail *deterministically*: attempt < k, where k comes from a seeded hash of the job_id and k < max_attempts. Random failures would occasionally exhaust retries and create false DEADs at 1M scale;
     - a few **poison** jobs (always raise) and **crashy** jobs (kill their worker process);
     - some **non-heartbeating slow** jobs, which are guaranteed to have their lease expire.
  2. During processing, on a random schedule:
     - `docker kill -s KILL` random worker containers, then restart them;
     - **`docker pause` a worker for longer than the lease, then `docker unpause`** (the classic GC-pause "zombie"). This is what reliably produces a worker that commits *after* losing its lease, i.e. real suppressed duplicates;
     - Toxiproxy faults on individual proxies: `reset_peer`, `timeout`, `latency`, and proxy disabled (partition) for short windows.
  3. Waits for the drain (with a global timeout), then runs the verifier.
- Size `max_deliveries` against the fault schedule so normal jobs aren't pushed into the DLQ just by being in flight during kills. Write down the reasoning (and an estimated false-DEAD probability) in DECISIONS.md.
- **Verifier invariants** (non-zero exit on any violation; JSON report to `results/…/chaos_report.json`):
  - **I1 No loss:** every accepted job_id has exactly one terminal state, SUCCEEDED or DEAD, and DEAD only for poison/crashy jobs.
  - **I2 No duplicate effects:** in the **append-only effects log**, each SUCCEEDED job's idempotency key appears exactly once. Poison jobs appear zero times.
  - **I2b No duplicate results:** in the **append-only results log**, each SUCCEEDED job_id appears exactly once.
  - **I3 DLQ correctness:** each poison/crashy job is in the DLQ with the expected attempts/deliveries.
  - **I4 The faults actually happened:** kills ≥ K, pauses ≥ P, network-fault windows ≥ M, reclaims > 0, and duplicate deliveries suppressed > 0. **If redelivery never happened, the test proved nothing.** Fail the run. If I4 fails, fix the fault schedule. Never relax I4.
  - **I5 Drained:** stream empty, PEL empty, delayed set empty.
- **Mutation checks (prove the verifier catches bugs), as automated tests:**
  - bypass the ledger's NX check → I2 must fail;
  - bypass the commit's `done` check → I2b must fail;
  - bypass the ownership check in the retry script → I1 must fail (or I2/I2b), under the stale-worker scenario.
- **Long runs:** a chaos run can exceed Claude Code's ~10-minute command limit. Run it in the background (or detached), write progress to a log file, and poll it. Never let a long run get killed partway and report a partial result as a pass.
- **Acceptance:** a local chaos run at N=100,000 passes (show the report summary), and the mutation tests pass. **STOP.**

### Phase 5: CI on GitHub Actions
- `ci.yml` on push/PR: `uv` setup with cache, ruff, mypy, pytest (Redis service container), and a Docker build.
- `chaos` job on every push: runs the compose-based chaos test and uploads `chaos_report.json` as an artifact.
- **Scale decision:** measure how long N=1,000,000 takes on a GitHub-hosted runner.
  - If ≤ ~12 minutes: run 1M on every push.
  - Otherwise: run a smaller N (e.g., 100K) on every push, plus 1M on a nightly `schedule` and `workflow_dispatch`.
  - Record which option is true. The resume bullet's wording must match reality.
- Add a CI badge to the README. Mohammed will push to GitHub. If `gh` is authenticated, you may watch runs with it.
- **Acceptance:** a green run on GitHub, with the run links recorded in PROGRESS.md. **STOP.**

### Phase 6: Local benchmark harness
- `bench/loadgen.py`: multi-process producers at a target rate or max rate, with a configurable job type and payload size.
- Measure:
  - completion throughput over a **steady-state window** (exclude warmup/cooldown);
  - end-to-end latency p50/p95/p99 (enqueue → complete, using Redis TIME);
  - enqueue latency;
  - rejected/blocked counts under backpressure.
- Worker scaling curve: 1, 2, 4, 8, 12 workers (a local curve will flatten because of the laptop; that's fine, it validates the harness).
- `bench/plot.py` → charts in `results/local/`.
- **Acceptance:** charts + raw JSON committed; a short analysis of where the bottleneck is (CPU of the workers? Redis? the loadgen?) with evidence (e.g., CPU utilization). **STOP.**

### Phase 7: AWS deployment (BILLABLE: requires explicit approval)
**Pre-flight (read-only, $0):**
1. Confirm the AWS CLI profile works (`aws sts get-caller-identity`), and pick a region (default `us-west-2` or `us-east-1`).
2. **Free plan constraints.** Verify in that region:
   - which EC2 instance types are allowed. Free-plan accounts are reported to be limited to small types such as `c7i-flex.large`, `m7i-flex.large`, `t3.small`, `t4g.small`, `t3.micro`, `t4g.micro` (2 vCPU max). Check what the account actually allows.
   - the vCPU quota: `aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A`.
   - **Fargate is not listed as supported on the Free plan**, so use **ECS with the EC2 launch type**. If you find Fargate works, record that, but stay on EC2 unless there's a strong reason.
3. Size the cluster to the quota. Example: 1 Redis instance, N worker instances each running a few worker containers, and 1 loadgen instance. With 2 workers per 2-vCPU instance, 12 workers + Redis + loadgen is about **16 vCPUs**. If the quota is lower, present a written re-plan (request a quota increase; fewer workers; different packing) with vCPU totals and costs. **Never upgrade the account to the Paid plan without explicit consent**; that makes real charges possible.
   - Prefer `c7i-flex.large` / `m7i-flex.large`. Avoid the t-family for benchmarks: its default "unlimited" CPU-credit mode bills extra under sustained load. If you must use it, set `credit_specification = standard` and report it.
   - Note in RESULTS that flex instances advertise a 40% baseline with bursts to 100%. Keep benchmark windows short, and check for throttling (compare repeat runs).
   - **Redis sizing:** run Redis on `m7i-flex.large` (8 GiB) with `maxmemory` set and `maxmemory-policy noeviction`. An LRU policy would silently evict idempotency keys and break correctness. Give `done`/ledger keys a TTL in benchmark mode, and `FLUSHALL` between benchmark runs. Budget memory for AOF rewrite forks, or disable AOF for throughput runs and say so.
4. Create an **AWS Budgets** cost budget with email alerts at $5, $10, $20. **Exclude credits from the budget's cost aggregation** (Terraform: `cost_types { include_credit = false }`). Otherwise credits net usage to about $0 and the alerts never fire. Also report the remaining credit balance and the Free plan's end date. (Setting up a budget is also one of the Free plan activities that earns extra credits.)

**Terraform (`deploy/terraform/`):**
- Use the default VPC or a minimal VPC with public subnets and **no NAT Gateway** (NAT costs money every hour).
- Put **everything in one AZ/subnet** (cross-AZ data transfer is billed).
- ECR repo; ECS cluster; Auto Scaling group with the ECS-optimized AMI (from the SSM parameter) as a capacity provider; task definitions for `redis` (placement-constrained to its own instance, AOF config documented), `worker` (service, desiredCount configurable), and `loadgen` (run-task).
- Security groups: Redis reachable only from the cluster's SG, never public. SSH is closed (use SSM Session Manager if a shell is needed).
- CloudWatch log groups with 1-day retention; WARN-level logging during benchmarks.
- Tag every resource `project=ftq`.
- Makefile: `aws-plan`, `aws-up` (plan → print cost estimate → require typed confirmation → apply), `aws-bench`, `aws-down`, `aws-verify-clean` (check no running instances, no ENIs, no EIPs, ECR images optional, Terraform state empty).
- **Acceptance:** `terraform plan` output + cost estimate shown to Mohammed. **STOP and wait for approval before `apply`.**

### Phase 8: AWS benchmark runs (BILLABLE: time-boxed)
- Session plan, max ~2 hours of runtime:
  1. Scaling curve: 1 → 2 → 4 → 8 → 12 workers, ~3 min steady state each.
  2. Headline run at 12 workers for ≥ 5 min steady state, repeated 3× (report the median and spread).
  3. Backpressure demo: offered load above capacity; show bounded queue depth, rejected/blocked counts, and a chart.
  4. Optional: a chaos run on AWS (`aws ecs stop-task` on random workers during load), verified with the same verifier.
- The load generator runs **inside AWS** as an ECS task (same subnet), not on the laptop. Locally you only start runs and poll their status/logs in short commands; Claude Code's command limit is ~10 minutes. Put a hard wall-clock guard on every run (the task stops itself), and make `aws-down` safe to run at any time.
- Save the raw results to `results/aws/`, plus `aws ecs describe-services` output proving 12 running workers.
- **Then tear everything down immediately** and run `make aws-verify-clean`. Record the estimated spend in PROGRESS.md (Cost Explorer lags ~24h; note to re-check).
- **Acceptance:** results + charts committed; teardown verified. **STOP.**

### Phase 9: Polish and hand-off
- **README:**
  - one-paragraph what/why;
  - an architecture diagram (Mermaid);
  - guarantees and non-guarantees;
  - quickstart in ≤ 3 commands (`make up`, `make chaos`, `make bench`);
  - results with charts;
  - how the chaos test works;
  - limitations and future work.
- **`docs/RESULTS.md`:** the methodology and the evidence table (§9), filled in.
- **`docs/DECISIONS.md`:** complete.
- **Final resume bullets:** propose revised bullets using only real numbers, each number linked to its evidence. If a target wasn't hit, say so plainly and suggest honest wording.
- **Acceptance:** fresh clone → `make setup && make check && make chaos` works. **STOP.**

---

## 8. Coding standards

- Type hints everywhere; `mypy --strict` clean on `src/`.
- Small, focused modules. Docstrings explain *why*, not just what. Every Lua script gets line-by-line comments.
- All configuration comes from env vars via pydantic-settings, with a documented table in the README.
- No premature abstraction: no plugin frameworks, no pluggable backends. One queue backend: Redis.
- Tests:
  - unit tests for pure logic;
  - integration tests against real Redis;
  - chaos tests under Compose.
  - Tests must be deterministic. Avoid bare `sleep`s; poll with timeouts; use short test-config timeouts; seed randomness. Mark slow tests.
- Commits use conventional messages (`feat:`, `fix:`, `test:`, `docs:`, `chore:`), one logical change each where practical.

---

## 9. Claims → evidence (fill this in `docs/RESULTS.md`)

| Resume claim | Evidence required |
|---|---|
| "12 parallel workers on AWS" | `describe-services` output showing 12 running worker tasks during the headline run; instance types listed |
| "10K+ jobs/sec" | Steady-state completion throughput (median of 3 runs, ≥ 5 min windows), with job type, payload size, and in-flight cap stated |
| "throttling intake during traffic spikes" | Backpressure run: offered vs. accepted rate, bounded depth chart, rejected/blocked counts |
| "crash tests run on every code change via GitHub Actions" | The workflow file triggers on push; a link to a green run |
| "randomly killing workers and cutting connections mid-job" | Chaos report: kill count, fault windows, reclaims > 0, duplicates suppressed > 0 |
| "0 lost jobs and 0 duplicate results across 1M+ tasks" | Verifier report with N ≥ 1,000,000, I1–I5 (incl. I2b) passing, and where it ran (per-push CI vs. nightly); mutation tests passing |

Each row gets: the measured value, its definition, the environment, the reproduce command, the raw file path, and the date.

---

## 10. Interview-readiness artifacts (low effort, high value)

Maintain these as you go. Don't write a textbook; Mohammed will generate a long-form write-up later from these files.

- `docs/DECISIONS.md` entries: context → options → decision → consequences. Examples:
  - Streams vs. Lists;
  - Lua for atomicity;
  - XDEL after ack vs. MAXLEN;
  - lease length trade-offs;
  - backoff with jitter;
  - why at-least-once + idempotency instead of exactly-once;
  - Redis TIME for latency;
  - ECS on EC2 vs. Fargate;
  - what the bottleneck was and how you found it.
- A "Things that went wrong" section in PROGRESS.md: real bugs found (especially by the chaos test) and how they were fixed. These are the best interview stories. Keep them honest and specific.

---

## 11. How Claude Code should work on this project

- **Session start:** read `CLAUDE.md`, this SPEC, and `PROGRESS.md`. Then summarize the current state in 3–5 lines before doing anything.
- **Each phase:** post a short plan (bullets) → implement → run acceptance checks → fix → update docs → commit → report → **stop**.
- **`PROGRESS.md` format:**
  - `## Status`: current phase, done/next;
  - `## Phase log`: per phase: what was built, acceptance evidence (commands + key output lines), open issues;
  - `## Spend log`: AWS resources created/destroyed, time, estimated cost;
  - `## Things that went wrong`.
- **End-of-phase report to Mohammed (in chat):** what was built (3–6 bullets), evidence (key numbers/test results), decisions made, anything he should look at or learn, and what the next phase needs from him.
- If Mohammed says "continue through Phase N", you may proceed through local phases without stopping, but **always stop before any billable action**, and stop if an acceptance check fails.
- **Long-running commands:** Claude Code's shell commands time out after ~10 minutes. Anything longer (1M-job chaos runs, AWS benchmarks) must run in the background or detached, log to a file, and be polled with short commands. A run that was killed partway is a failed run, not a partial success.
- Keep the context window healthy: prefer reading specific files/sections over dumping large outputs; summarize long logs.

---

## 12. Stretch goals (only after Phase 9, and only if Mohammed asks)

- Prometheus metrics endpoint + a Grafana dashboard in Compose.
- Priority queues (multiple streams with weighted fetching).
- Durability test: Redis with `appendfsync always`, and kill/restart Redis during chaos (report the throughput cost).
- An ElastiCache variant of the AWS deployment (compare it with self-hosted Redis).
