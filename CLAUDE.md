# CLAUDE.md: Fault-Tolerant Task Queue (`ftq`)

A from-scratch, Celery-style distributed task queue on Redis Streams: at-least-once delivery, idempotent (effectively-once) side effects, leases/visibility timeouts, retries with backoff, a dead-letter queue, backpressure, a chaos-testing harness, GitHub Actions CI, and a benchmark on AWS ECS.

**Full spec: `docs/SPEC.md`. Status log: `PROGRESS.md`. Read both at the start of every session**, then summarize the current state in 3–5 lines before doing anything else.

## Non-negotiables (details and reasons in SPEC §3)

1. **Every number is real.** Metrics come from saved raw results produced by committed scripts, with reproduce commands. Never fabricate, extrapolate, or cherry-pick to match the resume targets in SPEC §0.
2. **Never spend money without an explicit "yes".** This AWS account is on the Free plan: **if the credits run out, the account is closed.** Print an itemized cost estimate, wait for approval, tear down after every session, and verify the teardown. Target total spend < $15.
3. **Never commit secrets**, `.env`, Terraform state, or AWS account IDs.
4. **Never weaken, skip, or delete a test to make it pass.** Document real limitations in `docs/DECISIONS.md`.
5. **Verify by running commands** and report the actual output. "Should work" doesn't count.
6. **Integration and chaos tests use real Redis** (Docker). Mocks are only for pure-logic unit tests.
7. **Phase gates:** at the end of each phase, run the acceptance checks, update `PROGRESS.md` and `docs/DECISIONS.md`, commit, report, and **STOP**. Exception: if Mohammed says "continue through Phase N", keep going through $0 phases, but still stop before anything billable and whenever an acceptance check fails.
8. **Ask when blocked** (Docker down, no credentials, quota too low). Don't fake or quietly shrink scope.
9. **Long-running commands:** shell commands time out after ~10 minutes. Run long jobs (1M-job chaos, AWS benchmarks) in the background or detached, log to a file, and poll. A run killed partway is a failure, not a pass.
10. **Correctness core (SPEC §4):**
    - every non-commit state change (retry, DLQ, heartbeat) checks lease ownership in Lua;
    - commit is first-wins and idempotent;
    - the chaos verifier counts the **append-only** effects/results logs.
    These are the places bugs hide, so test them hardest.

## Environment facts

- Mohammed's machine: macOS (likely Apple Silicon). Build AWS images with `docker buildx --platform linux/amd64` unless using arm64 instances throughout.
- AWS Free plan: use **ECS on the EC2 launch type** (Fargate isn't listed as supported). Instance types are likely limited to small 2-vCPU types, so check the account's allowed types and vCPU quota before sizing (12 workers is about 16 vCPUs in total).
- No NAT Gateway, a single AZ, CloudWatch retention of 1 day, and WARN-level logs during benchmarks (per-job logging at 10K/s is expensive).
- AWS Budgets alerts must **exclude credits**, or they never fire. Redis uses `noeviction`, never LRU; eviction would silently break idempotency.

## Commands (keep this list accurate as the Makefile evolves)

```
make setup        # uv sync --locked (no pre-commit hooks yet)
make fmt          # ruff format + safe autofixes
make check        # fmt-check + lint + typecheck + FAST tests (`-m "not slow"`, ~16 s)
make check-all    # fmt-check + lint + typecheck + EVERY test (~85 s); what CI must run (Phase 5 confirmed)
make test         # fast pytest set (needs Redis: `make up`; fails, never skips, without it)
make test-all     # every test, including `slow` (subprocess / process-pool / >1 s tests)
make up [WORKERS=N] / down   # redis (+ N worker containers from docker/Dockerfile) / down -v
uv run ftq worker                 # run one worker (FTQ_* env config; SIGTERM drains)
uv run ftq enqueue TYPE --payload JSON [--idempotency-key K]   # exit 2 on QueueFull
uv run ftq stats                  # JSON: depth, in_flight, delayed, dlq, consumers, full, counters
uv run ftq bench --jobs N [--type T --payload JSON --batch B]  # enqueue, wait, exactly-once check
uv run ftq dlq list [--limit N]   # DLQ entries as JSON lines
uv run ftq dlq requeue JOB_ID... | --all   # back on the stream, attempt 0, same job_id
uv run python bench/pipelining.py # enqueue batching + worker drain measurement -> results/local/
make chaos N=100000 [CHAOS_WORKERS=8] [SEED=s]  # chaos run + verifier -> results/local/chaos_report.json (~2 min; run in background)
uv run python -m chaos.run --help                # same, all knobs (--rate, --out, --keep, --skip-build ...)
make chaos-record [N=100000]  # the same run recorded (asciinema via uvx) -> results/local/chaos_demo.{cast,txt,json}
uv run python -m chaos.summary results/ci/chaos_report_*_N1M_*.json   # Markdown table of chaos reports (RESULTS.md)
gh workflow run chaos-scale.yml -f jobs=N [-f workers=W]   # chaos on a GitHub runner (1M nightly; ~35 min)
make bench [BENCH_ARGS=...]   # scaling + latency + backpressure + charts -> results/local/bench/ (~25 min; background)
uv run python -m bench.run {concurrency|scaling|latency|backpressure|iothreads|point} --help   # one suite (local Docker)
uv run python -m bench.loadgen --help   # the load generator alone: needs only FTQ_REDIS_URL + running workers (Phase 8 reuses it)
uv run python -m bench.loadgen --hosts 2 --run-id R ...   # coordinator of a 2-host run (ADR-047); on the other host:
uv run python -m bench.loadgen --producer-only --run-id R   # takes the coordinator's spec, offers its share
uv run python -m bench.plot             # charts + summary.md from the saved reports
uv run python -m bench.plot --aws       # AWS scaling.png + backpressure.png + summary.md from results/aws/
TF_VAR_alert_email=... make aws-base   # budget alarm + ECR repo (idle $0; applied once)
make aws-image                    # build linux/amd64, push to ECR as the git short SHA
make aws-plan [TF_VARS="-var worker_hosts=6 -var workers=12 -var loadgen_hosts=1"]  # plan + itemized estimate, $0
make aws-up                       # BILLABLE: apply the saved plan after typing "apply"
make aws-smoke                    # BILLABLE (stack up): ftq bench as an ECS task, exactly-once checked
uv run python -m deploy.aws task -- CMD ...   # BILLABLE (stack up): any one-off task, e.g. the loadgen
make aws-bench-plan               # $0: the Phase 8 session's points, minutes and cost
make aws-bench-local              # $0: the Phase 8 driver end to end on local Docker (port 6392)
make aws-bench [BENCH_ARGS=...]   # BILLABLE (stack up): the Phase 8 session -> results/aws/ (background + poll)
make aws-down                     # destroy stack + delete ECR images + verify-clean (safe any time)
make aws-verify-clean             # $0: fail unless nothing billable is left in us-west-2
```

## Style

- Python 3.12, `uv`, `src/ftq`, `mypy --strict`, `ruff`.
- Docstrings explain *why*. Lua scripts are commented line by line.
- Clarity over cleverness. No premature abstraction.
- Conventional commits.
