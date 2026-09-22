# Design decisions (ADR log)

Each entry: **Context → Options → Decision → Consequences.** Newest entries at the bottom.
Status is one of *accepted* (in force), *planned* (decided, implemented in a later phase),
or *superseded*.

---

## ADR-001: Redis Streams (not Lists) as the transport

*Status: accepted (Phase 0). Implemented in Phase 1.*

**Context.** Workers must be able to crash mid-job without losing it, and another worker must
be able to detect and take over abandoned work.

**Options.**
1. **Lists + `BLMOVE` to a per-worker "processing" list** (the classic "reliable queue" pattern).
   Crash recovery needs a custom scanner that knows which worker owned which processing list and
   when it took each item. Lists record neither delivery time nor delivery count, so we'd
   invent our own bookkeeping for leases and poison detection.
2. **Redis Streams + consumer groups.** The server tracks every delivered-but-unacked entry in
   the group's Pending Entries List (PEL), with owner, idle time, and delivery count.
   `XAUTOCLAIM` reclaims entries idle longer than a threshold; `XPENDING` exposes the delivery
   count we need for crash-loop (poison) detection.
3. A separate broker (RabbitMQ, SQS, Kafka): out of scope (SPEC §2 non-goals: one backend, Redis).

**Decision.** Streams. The PEL gives us leases (idle time), ownership, and delivery counts as
built-in, well-documented server features instead of hand-rolled bookkeeping.

**Consequences.**
- `XACK` and `XCLAIM` don't check who owns an entry, so every non-commit transition must check
  ownership in Lua first (SPEC §4; implemented in Phase 2).
- Acked entries stay in the stream unless deleted, so every exit path does `XACK` + `XDEL`
  (never `XTRIM MAXLEN`, which can drop unacked entries). A dedicated ADR follows in Phase 1.

---

## ADR-002: Python for the implementation

*Status: accepted (Phase 0).*

**Context.** A portfolio project that must be explainable in interviews, implemented mostly by
Claude Code and studied by Mohammed afterward. The workload is I/O-bound (Redis round trips),
with a CPU-bound handler type for benchmarks.

**Options.** Python (asyncio), Go, Rust.

**Decision.** Python 3.12 with `asyncio` + redis-py's asyncio client.
- The target roles (SWE/MLE) and the Celery/RQ ecosystem this project mirrors are Python.
- The correctness-critical logic lives in **Lua scripts inside Redis**, so the choice of
  host language barely affects correctness; it mainly affects throughput per worker.
- `mypy --strict` gives type safety that is close to a compiled language for this code size.

**Consequences.**
- One Python process runs on one core (GIL), so per-worker throughput is limited. We scale
  out with processes and containers, and batch/pipeline Redis calls (Phase 3).
- The 10K jobs/sec target (SPEC §0) is harder in Python than in Go. If we miss it, we report
  the real number (SPEC §3.1) and say where the bottleneck was.

---

## ADR-003: ECS on the EC2 launch type (not Fargate) for the AWS benchmark

*Status: accepted (Phase 0). Verified against the actual account in Phase 7.*

**Context.** The AWS account is on the **Free plan**: about $100 in credits, and the account
**closes** when the credits run out or 6 months after sign-up. The Free plan only exposes
"select" services.

**Options.**
1. **ECS on Fargate.** No instances to manage, but billed per vCPU-hour and GB-hour. The spec
   reports Fargate isn't listed as supported on the Free plan. The public Free plan docs we
   checked (2026-09-22) don't list services explicitly, so this is **unverified** until Phase 7.
2. **ECS on EC2** with an Auto Scaling group of ECS-optimized AMIs as a capacity provider. We
   choose instance types ourselves, so we can stay within whatever types and vCPU quota the
   Free plan allows.
3. Raw EC2 with Docker and no ECS: less to learn, but no service/desired-count abstraction and
   no clean `describe-services` evidence that 12 workers were running (SPEC §9).

**Decision.** ECS with the EC2 launch type. If Fargate turns out to work on this account, we
record that but stay on EC2 unless there's a strong reason to switch.

**Consequences.**
- We manage an ASG and the ECS agent (via the SSM-parameter AMI). That's a bit more Terraform.
- Instance sizing is limited by the account's allowed types and vCPU quota, which we must check
  in Phase 7 before sizing (`describe-instance-types --filters Name=free-tier-eligible,Values=true`;
  quota `L-1216C47A`).
- Without a NAT gateway, instances need **public IPv4 addresses** to reach ECS/ECR endpoints.
  They cost about $0.005/hr each and must be in the Phase 7 cost estimate (the alternative, VPC
  interface endpoints, costs more per hour).

---

## ADR-004: Redis 8.8.3 image, classic stream commands only

*Status: accepted (Phase 0).*

**Context.** The spec asked for Redis 7.4+. At bootstrap (2026-09-22) the current release is
8.8.3. It adds stream commands that overlap our design: `XACKDEL` (8.2: ack and delete in one
command), `XNACK` and `XREADGROUP … CLAIM` (8.8: release entries for immediate redelivery, and
claim idle entries while reading).

**Options.** (a) pin 7.4.x; (b) pin 8.8.3 and use the new commands; (c) pin 8.8.3 but use only
commands that exist in 7.x.

**Decision.** (c). Pin `redis:8.8.3` for current performance and fixes, but use only the classic
commands (`XREADGROUP`, `XACK`+`XDEL` inside Lua, `XAUTOCLAIM`, `XCLAIM … JUSTID`, `XPENDING`).
- The hand-built versions are the part worth explaining in an interview.
- The code stays portable to Redis/Valkey 7.x, which is what ElastiCache runs (a SPEC §12
  stretch goal).
- `XNACK SILENT` would let graceful shutdown hand back unfinished jobs immediately instead of
  waiting for the lease to expire. Noted as possible future work; not used.

**Consequences.** An integration test asserts the server version, so an accidental image change
fails loudly.

---

## ADR-005: Python 3.12 (as specified), not 3.13/3.14

*Status: accepted (Phase 0).*

**Context.** At bootstrap, 3.12 receives security fixes only (EOL Oct 2028). 3.13 and 3.14
are current, and 3.14 adds `uuid.uuid7()` to the standard library.

**Decision.** Stay on 3.12, per the spec and Mohammed's call. Every dependency supports it. The
runtime is set by our own container image (`python:3.12-*`), not by the ECS host AMI. The host
only runs Docker and the ECS agent, so the host's Python version doesn't matter to the workers.

**Consequences.** UUIDv7/ULID job IDs come from a small library (e.g. `uuid-utils`) rather than
the standard library (Phase 1).

---

## ADR-006: redis-py 8.x client configuration; job IDs generated by the client

*Status: planned (implemented in Phase 1).*

**Context.** redis-py 8.x now uses the RESP3 protocol on the wire (while keeping RESP2-shaped
replies), defaults to 5-second socket timeouts, and **automatically retries failed commands**
(10 attempts, exponential backoff with jitter). A retry re-sends a command whose reply was lost,
even though the command may already have run.

**Decision.**
- Set `socket_timeout`, `socket_connect_timeout`, `health_check_interval`, and an explicit,
  bounded `Retry` policy in `config.py`. We don't rely on library defaults that can change
  between major versions, and a partitioned worker must give up quickly enough for the chaos
  drain to finish.
- Generate `job_id` (UUIDv7) **on the client**, before `XADD`. If `XADD` is retried after a
  lost reply, the result is two stream entries with the **same** job_id. Commit is keyed by
  job_id, so the second one is suppressed as a duplicate, and "exactly one terminal state per
  job_id" still holds.
- Every Lua script must be safe to re-run after a lost reply. Commit and the ledger are already
  idempotent. For the ownership-checked scripts (retry, DLQ, heartbeat), a re-run returns
  `LEASE_LOST` because the first run already acked the entry. That's harmless.

**Consequences.** Every script's docstring must state what happens if it's re-sent.

---

## ADR-007: Slow non-heartbeating jobs must stay between 1× and about 2× the lease

*Status: planned (lease/reaper in Phase 2; chaos job mix in Phase 4).*

**Context.** The chaos mix includes slow jobs that don't heartbeat, so their lease is guaranteed
to expire (SPEC §7, Phase 4). Every reclaim by `XAUTOCLAIM` increments the delivery count. A job
that runs for k lease lengths can be reclaimed about k times, and once that exceeds
`max_deliveries` it goes to the DLQ. That would be a false DEAD and fail invariant I1, which
says only poison/crashy jobs may be DEAD.

**Decision.** Slow-job duration is drawn from roughly (1.2×, 1.8×) the lease. That guarantees at
least one reclaim, while the original holder usually finishes first and commits (commit is
first-wins, so the reclaimer's commit becomes a suppressed duplicate). The exact bounds and the
worst-case delivery count go into the `max_deliveries` sizing analysis (ADR-008).

**Consequences.** Slow jobs produce real redeliveries and suppressed duplicates (needed for
invariant I4) without risking false DEADs.

---

## ADR-008: Size `max_deliveries` for *all* sources of redelivery, including crashy jobs

*Status: planned (Phase 4 sizing, with an estimated false-DEAD probability).*

**Context.** A crashy job kills its whole worker process. Every *other* job that worker had
prefetched also gets redelivered, and its delivery count goes up even though it did nothing
wrong. So redeliveries come from: scheduled kills, pauses longer than the lease, network-fault
windows, slow jobs (ADR-007), **and crashy jobs taking down innocent jobs**.

**Decision.** In Phase 4, model the expected delivery count of a normal job from:
- the per-worker prefetch (in-flight cap);
- the rate of crashy jobs;
- the kill/pause/fault schedule.

Then set `max_deliveries` so a normal job's chance of a false DEAD is negligible over N = 1M,
and write down the estimate. Crashy jobs make up a small fraction of the mix, and prefetch per
worker is bounded.

**Consequences.** If the verifier ever reports a normal job as DEAD, first check this analysis
against the fault counts before calling it a bug. Never "fix" it by weakening I1 or I4.

---

## ADR-009: A late success deletes the job's DLQ entry

*Status: planned (DLQ in Phase 2).*

**Context.** SPEC §4 recommends that a late success (a slow owner commits after the job was
moved to the DLQ) replaces `DEAD` with `SUCCEEDED`, because the work was actually done. But the
job's entry would still sit in the DLQ stream, and `dlq requeue --all` would run it again.

**Options.** (a) the late-success commit deletes the DLQ entry in the same Lua script;
(b) `dlq requeue` checks the terminal state and skips jobs that already succeeded.

**Decision.** (a), per Mohammed. The commit script already runs atomically when it overrides
DEAD. It also removes the DLQ entry (it looks up the DLQ entry ID stored with the terminal
state) and increments a `late_successes` counter. DEAD never replaces SUCCEEDED.

**Consequences.** The DLQ always holds exactly the jobs whose terminal state is DEAD, which also
makes invariant I3 simpler to check. The commit script gets one extra branch, which needs its
own test.

---

## ADR-010: TTLs on `done` / terminal-state keys must outlast any possible redelivery

*Status: planned (Phase 1 commit; Phase 2 leases; Phase 4 chaos mode).*

**Context.** `done:{job_id}` is what makes a redelivered commit a suppressed duplicate. If it
expires while a copy of the job can still be delivered (a paused zombie worker, a long retry
backoff, a very old PEL entry), the late copy would commit **again**: a real duplicate. The
chaos verifier also reads terminal-state keys after the drain.

**Decision.**
- The default TTL (days) must far exceed the worst-case job lifetime: max attempts × max
  backoff + max deliveries × lease + margin. The config documents this relation and validates it.
- Chaos and test runs set **no TTL** (or one far beyond the run length), so the verifier sees
  every key.
- Benchmark mode may use shorter TTLs to bound memory, with `FLUSHALL` between runs (SPEC §7).

**Consequences.** Memory grows with the number of distinct jobs until TTLs expire. That's
acceptable locally (1M small keys is on the order of 100 MB) and is covered by the `maxmemory`
cap (ADR-013).

---

## ADR-011: Per-worker Toxiproxy topology is deferred to Phase 4

*Status: planned (Phase 4).*

**Context.** The chaos design needs one proxy per worker (`worker_i → proxy_i → redis`), so a
network fault can hit a single worker. `docker compose --scale worker=N` replicas can't each be
given a distinct proxy address.

**Decision.** In Phase 4: run one Toxiproxy container hosting N proxies (one listen port per
worker), and generate a Compose override with one named service per worker (`worker-1..N`),
each pointed at its own proxy port. Phase 0–3 Compose stays simple (Redis, then scalable
workers). Per Mohammed: don't build this before the chaos harness needs it.

**Consequences.** `make up WORKERS=4` (Phase 3) and the chaos topology (Phase 4) are different
Compose configurations. The chaos one is generated by `chaos/`.

---

## ADR-012: `make test` fails, never skips, when Redis is unreachable

*Status: accepted (Phase 0).*

**Context.** Integration tests need real Redis (SPEC §3.7). A common pattern is to skip them
when Redis is down. But then `make check` could go green without testing anything, which is
exactly the fake verification SPEC §3.5 forbids.

**Decision.** The `redis_client` fixture calls `pytest.fail` with the hint "Run `make up`
first." The workflow is `make up && make check`. CI provides Redis as a service container
(Phase 5).

**Consequences.** Running `make check` on a fresh machine without Docker fails. That's the
correct outcome.

---

## ADR-013: Local Redis configuration: `noeviction`, AOF `everysec`, 2 GB cap

*Status: accepted (Phase 0). AWS values revisited in Phase 7.*

**Context.** Idempotency depends on `done`/ledger keys never disappearing early.

**Decision.**
- `maxmemory-policy noeviction`: when memory is full, writes **fail loudly** (OOM errors)
  instead of silently evicting keys. An LRU/LFU policy would silently break idempotency. An
  integration test asserts this setting.
- `maxmemory 2gb` locally: a hard cap so a runaway test can't eat the laptop. We'll revisit it
  when sizing the 1M-job chaos run.
- `appendonly yes`, `appendfsync everysec`: a Redis crash can lose up to about 1 second of
  writes. Redis's own durability is a trust boundary and outside the zero-loss claim (SPEC §4).
  A stricter `appendfsync always` durability test is a stretch goal (SPEC §12).
- Redis port published on `127.0.0.1` only; there's no password on a local dev Redis.

**Consequences.** Hitting the cap during a run shows up as errors, not silent corruption, and
the chaos verifier will catch it.

---

## ADR-014: Keep the repo outside iCloud-synced folders

*Status: accepted (Phase 0).*

**Context.** The repo started under `~/Desktop`, which iCloud syncs ("Desktop & Documents
Folders"). iCloud set the macOS `UF_HIDDEN` flag on `.venv` and its files. CPython 3.12.13's
`site.py` skips hidden `.pth` files (a security hardening), so uv's editable-install `ftq.pth`
was ignored and `import ftq` failed with `ModuleNotFoundError`. We confirmed this with `ls -lO`,
and it matches a public report: https://github.com/alexbejan/jevkit/issues/1.

**Options.**
1. Move the repo out of iCloud-synced folders.
2. Put the venv in `.venv.nosync` (iCloud ignores `*.nosync`) with a `.venv` symlink. This was
   briefly used as a stopgap.
3. Add `pythonpath = ["src"]` to pytest. That fixes tests only, not the `ftq` CLI entry point.

**Decision.** Option 1: the repo now lives at `~/code/fault-tolerant-task-queue`, with a plain uv
`.venv/`. It fixes the root cause rather than one symptom. It also stops iCloud from syncing
`.git`, the virtualenv, and (later) large chaos logs and raw results. That syncing is slow, and
iCloud's conflict handling can corrupt a git repository.

**Consequences.** No machine-specific workaround in the repo. After the move, the recreated
`.venv` has no hidden flag and `import ftq` works. If the project is ever cloned back into
`~/Desktop` or `~/Documents` on a Mac with iCloud Desktop sync, this failure comes back.
