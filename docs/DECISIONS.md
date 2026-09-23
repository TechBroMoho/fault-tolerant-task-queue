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

*Status: accepted (implemented in Phase 1: `config.make_redis`, `models.new_job_id`).*

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

*Phase 1 values:* `socket_timeout` 5 s, `socket_connect_timeout` 2 s,
`health_check_interval` 10 s, `Retry(ExponentialWithJitterBackoff(base=0.05, cap=1.0), 3)`
on `ConnectionError`/`TimeoutError`. All are `FTQ_*` settings. `block_ms` (1 s) must be below
`socket_timeout`, or every idle `XREADGROUP BLOCK` would time out; the config validates that.
The client retry covers `XREADGROUP` too. If a fetch's reply is lost, the retried read returns
*different* entries, and the lost read's entries sit in this worker's PEL without being worked
on. That's the same state as a crashed worker, and the Phase 2 reaper reclaims them after the
lease. It isn't a loss, but it's one more source of redelivery for ADR-008's sizing.

---

## ADR-007: Slow non-heartbeating jobs must stay between 1× and about 2× the lease

*Status: partly implemented (Phase 2: the `slow` handler, registered with
`heartbeat=False`; a test shows it is reclaimed mid-run and its effect still happens once).
The duration distribution lands with the Phase 4 chaos mix.*

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

*Status: accepted (Phase 2: commit.lua step 1b returns LATE_SUCCESS;
`test_late_success_replaces_dead_and_removes_dlq_entry`).*

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

*Status: accepted (Phase 1: `FTQ_DONE_TTL_SECONDS`, default 7 days, `0` = never expire,
applied to done and ledger keys; tests run with 0. Phase 2: the relation below is
validated by `Settings`, see the Phase 2 note at the end of this entry).*

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

*Phase 2 note.* `Settings.job_lifetime_bound` = `max_attempts × (max_deliveries ×
visibility_timeout + job_backoff_cap)`: every attempt can be delivered `max_deliveries`
times, each after a full lease, then wait up to the backoff cap. A non-zero
`done_ttl_seconds` must be at least **10×** that, or the config is rejected. Defaults:
5 × (10 × 30 s + 300 s) = 3000 s, so the 7-day TTL passes easily. Two things this bound does
**not** cover, documented rather than validated: time a job waits undelivered in a
backlog, and a job that heartbeats for a very long time (both extend a job's life without
redeliveries). DEAD records never expire: they live as long as their DLQ entry (ADR-027).

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

---

## ADR-015: No HTTP enqueue endpoint; backpressure lives in the library

*Status: accepted (Phase 0 review; affects Phase 3).*

**Context.** SPEC §4 and Phase 3 list an *optional* FastAPI enqueue endpoint that turns
`QueueFull` into `429 Too Many Requests` with a `Retry-After` header.

**Options.** (a) build the endpoint (FastAPI + uvicorn, a container, and tests);
(b) keep backpressure in the library only: `enqueue()` raises `QueueFull` in `reject` mode and
waits in `block` mode.

**Decision.** (b), per Mohammed.
- The mechanism is the high/low watermarks, the cached depth check, and hysteresis. It sits in
  `client.py` either way. An HTTP layer would only translate an exception into a status code,
  and adds nothing to the correctness or throughput story.
- The "throttling intake during traffic spikes" claim (SPEC §9) is backed by the backpressure
  benchmark: offered vs. accepted rate, bounded queue depth, and rejected/blocked counts. The
  benchmark calls the library directly.
- It avoids two dependencies, a service to deploy on AWS, and an extra network hop that would
  make enqueue-latency numbers harder to interpret.

**Consequences.** Web apps would call `enqueue()` in-process, like Celery/RQ clients do. An HTTP
front end would be a thin addition later (catch `QueueFull`, return 429 + `Retry-After`) and is
listed as future work. The README must not imply an HTTP API exists.

---

## ADR-016: Every exit from the stream is `XACK` + `XDEL`; never `XTRIM MAXLEN`

*Status: accepted (Phase 1: commit.lua; Phase 2 retry/DLQ scripts follow the same rule).*

**Context.** In a consumer group, `XACK` only removes an entry from the Pending Entries List.
The entry itself stays in the stream forever, so memory grows with every job ever processed,
and `XLEN` stops meaning anything.

**Options.**
1. **`XTRIM MAXLEN ~N`** (or `XADD … MAXLEN`): cap the stream length. Trimming removes the
   *oldest* entries whether or not they've been delivered or acked. Under a backlog, or with
   an old entry parked in a PEL (a crashed worker's job waiting for reclaim), trimming deletes
   jobs nobody has finished. That's silent data loss, and exactly the case our zero-loss
   claim is about.
2. **`XTRIM MINID`** at the oldest pending ID: safe only if computed correctly under
   concurrency, and it still keeps acked entries behind any old pending one.
3. **`XACK` + `XDEL` of the exact entry, inside the same Lua script as the state change.**

**Decision.** (3). The commit script (and later the retry and DLQ scripts) acks and deletes
the one entry it's finishing, atomically with recording the outcome.
- `XLEN` = undelivered + in flight: a real depth metric, which backpressure (Phase 3) uses.
- Redis 8.2 added `XACKDEL`, which does both in one command. We keep the two classic commands
  (ADR-004); inside a script they're atomic anyway.
- A unit test fails if any Lua script mentions `XTRIM`.

**Consequences.** The duplicate path of commit.lua also acks and deletes its entry. Otherwise
a suppressed duplicate would sit in the PEL and be redelivered forever. A test covers it
(`test_second_holder_commit_is_suppressed_and_cleans_up`). Mutation check: with commit.lua's
`XDEL` removed, 5 integration tests fail.

---

## ADR-017: Lua scripts for every multi-step transition; `#!lua` shebang for OOM safety

*Status: accepted (Phase 1: enqueue, commit, ledger).*

**Context.** Commit is check-then-act: "if not done, record the result, append to the log,
ack, delete". Two workers committing the same job at once must not both see "not done".

**Options.**
1. Client-side `WATCH`/`MULTI`/`EXEC` (optimistic locking): retry loops under contention,
   more round trips, harder to read.
2. `MULTI`/`EXEC` alone: it can't branch on a value read inside the transaction.
3. **A Lua script**: Redis runs it to completion with no other command in between, so the
   check and the writes form one atomic step, in one round trip.

**Decision.** (3), with each script commented line by line in `src/ftq/scripts/`. Every
key a script touches is passed in `KEYS` (required for Cluster, and it documents the script's
footprint). Scripts start with a `#!lua` shebang and no flags, which tells Redis 7+ to check
for OOM **before** running the script. Without that check, a script that hit `maxmemory`
halfway (under `noeviction`, ADR-013) would fail on its first write only after earlier
commands had run. Redis scripts don't roll back.

**Consequences.**
- A write can still fail mid-script for a non-memory reason (e.g. a key of the wrong type).
  That would be a bug in our key layout, not a runtime condition. Every key is namespaced per
  queue (ADR-018) and written by one script type only.
- `TIME` is allowed inside scripts: Redis 7+ replicates a script's effects, not the script
  itself, so non-deterministic commands are fine (verified on 8.8.3).
- A unit test requires every script to start with `#!lua`.

---

## ADR-018: Key naming `ftq:{<queue>}:<kind>[:<id>]` with a hash tag

*Status: accepted (Phase 1: `keys.py`).*

**Context.** A commit touches the stream, the job's done key, the results log, and the stats
hash in one script. On Redis Cluster, a script can only touch keys in one hash slot.

**Decision.** Every key embeds `{<queue>}`. Cluster hashes only the text inside the first
`{…}`, so all of a queue's keys share a slot, and every script keeps working unchanged if we
ever shard queues across a cluster. Queue names are limited to `[A-Za-z0-9_.-]{1,64}` (no
braces), which the config validates. Keys: `:stream`, `:done:<job_id>`, `:idem:<key>`,
`:ledger:<effect_key>`, `:results`, `:effects`, `:stats`; Phase 2 adds `:delayed` and `:dead`.

**Consequences.** One queue can't be spread across a cluster. It's one slot, so one shard,
by design. Sharding (not a goal, SPEC §2) would mean many queues, or N sub-queues per logical
queue with producers hashing jobs across them. Tests use a random queue name each, so their
keys never collide, and delete `ftq:{<queue>}:*` afterwards.

---

## ADR-019: Two idempotency keys: one for enqueue, one for effects

*Status: accepted (Phase 1: enqueue.lua, ledger.lua, the send_email handler).*

**Context.** "Idempotency key" means two different things in SPEC §4: a producer's
enqueue dedup key, and the key a handler passes to a downstream API.

**Decision.**
- **Enqueue:** optional `idempotency_key`. enqueue.lua does `SET idem:<key> <job_id> NX EX ttl`
  and only then `XADD`s, both in one script, so two concurrent enqueues with the same key
  can't both add a job. A repeat within `FTQ_IDEMPOTENCY_TTL_SECONDS` (default 24 h) returns
  the **original** job_id and adds nothing, even if the payload differs. The key is the
  producer's statement that it's the same job, and we don't compare payloads. (Stripe is
  stricter: it rejects a reused key sent with different parameters. That would mean storing
  a payload hash with the key; possible future work.) After the TTL, the key is free again.
- **Effects:** handlers derive the ledger key from `job_id` (e.g. `send_email:<job_id>`),
  not from the enqueue key, which is optional. job_id is present on every job and shared by
  every copy of it: redeliveries, reclaims, and the duplicate entry a re-sent `XADD` creates
  (ADR-006). The ledger (`SET NX` marker + `XADD` to the append-only effects log, in one
  script) applies each key at most once.

**Consequences.** A producer that retries `enqueue()` without a key after a timeout may create
two jobs with different job_ids. Both run and both send an email. Only the key prevents
that. The README's guarantees section (Phase 9) must say so. Effects are only effectively-once where the real downstream
honors keys; the ledger emulates that (SPEC §4).

---

## ADR-020: Timestamps come from Redis `TIME`, taken inside the scripts

*Status: accepted (Phase 1: `enqueued_at_ms` in enqueue.lua; `finished_at_ms` in commit.lua).*

**Context.** End-to-end latency is `complete − enqueue`. Producers and workers run on
different machines (on AWS, different instances), and their clocks can disagree by
milliseconds or more. That's the same order as the latencies we want to measure.

**Decision.** Both timestamps are read from Redis's clock by the script that performs the
transition. Every latency is then a difference of two readings of one clock, and it costs no
extra round trip. The results log records `enqueued_at_ms` and `finished_at_ms` for each
first commit, which is the raw data for Phase 6 percentiles.

**Consequences.** Redis's clock stepping backwards (an NTP correction) could produce a
negative sample, which the analysis must report, not hide. Queue wait and handler time aren't
separated yet. If Phase 6 needs that split, commit can also record the delivery time.

---

## ADR-021: The done key is a terminal-state hash; commit checks `state == SUCCEEDED`

*Status: accepted (Phase 1: commit.lua).*

**Context.** SPEC §4 wants one terminal-state record per job, which a late success may upgrade
from DEAD to SUCCEEDED (ADR-009, Phase 2), plus a stored result.

**Decision.**
- `ftq:{q}:done:<job_id>` is a hash: `state`, `result` (JSON), `finished_at_ms`, `worker_id`.
- Commit's duplicate check is `HGET state == 'SUCCEEDED'`, not "key exists". So when Phase 2
  writes `state=DEAD`, a late success still goes through (ADR-009) without changing the check.
- `duplicates_suppressed` counts **every** commit that finds the job already SUCCEEDED. That
  includes a commit re-sent by the client after a lost reply. We considered telling the two
  apart by whether `XACK` returned 1, but that doesn't work: when a stale holder and a
  reclaimer share one entry ID (the Phase 2 stale-worker case), the first commit acks the
  shared entry, so the second always sees `XACK = 0`, even though it's a genuine duplicate
  delivery. The simple rule is also the honest one: each suppressed commit is work done twice.
- The **append-only results log** gets one entry per *first* commit. Unlike the done key, it
  can show a duplicate, so it's the evidence for "0 duplicate results" (invariant I2b).

**Consequences.** The results and effects logs grow by one entry per job and are never
trimmed by the queue. That's fine for tests and chaos runs (the verifier needs every entry).
For long benchmark runs, Phase 6/8 decides whether to disable them or flush between runs, and
says so in RESULTS. Trimming a *log* is safe; trimming the *job stream* is not (ADR-016).
Mutation check: with commit's `done` check bypassed, 3 tests fail; with the ledger's `NX`
removed, 2 fail.

---

## ADR-022: Worker fetch loop, in-flight cap, and graceful shutdown

*Status: accepted (Phase 1: `worker.py`, `cli.py`). Phase 2 resolved the three "Phase 1
limits" below (ADR-027, ADR-029, ADR-028). Concurrency is tuned in Phase 3.*

**Context.** A worker must bound its in-flight work, never leave fetched jobs unattended, and
on SIGTERM (what `docker stop` and ECS send) finish what it has without taking more.

**Decision.**
- **Fetch only what can start now.** Each `XREADGROUP` asks for `concurrency − in_flight`
  entries, and every fetched entry immediately becomes a task. Nothing sits "prefetched" in
  this worker's PEL while no one works on it. That matters for ADR-008: prefetched but idle
  jobs would pick up redeliveries when the worker dies.
- **Consumer group created at ID `0`**, with `MKSTREAM`, by the worker on startup. With `$`,
  jobs enqueued before the first worker ever started would be skipped: silent loss. A test
  enqueues before starting the worker.
- **The blocking read is never cancelled.** If a stop arrives during `XREADGROUP BLOCK`, we
  wait for it to return (≤ `block_ms`, default 1 s). Cancelling it could orphan entries Redis
  had already delivered. Entries returned after a stop request are still processed.
- **SIGTERM/SIGINT:** stop fetching; wait up to `FTQ_SHUTDOWN_GRACE` (default 30 s) for
  in-flight jobs; cancel whatever is left and exit 0. Abandoned jobs never committed, so
  their entries stay in the PEL for the Phase 2 reaper. There's no second-signal force-quit:
  the grace period already bounds shutdown, and `docker stop` follows with SIGKILL anyway.
- **Fetch errors** (after the client's own retries) are logged, followed by a 1 s pause and
  another try. A worker partitioned from Redis waits instead of crash-looping.
- **A failed commit round trip is not a handler failure** (SPEC §4). The job is logged and
  left in the PEL. On redelivery, either the commit had landed (the redelivery is suppressed)
  or it hadn't (the handler reruns, and the ledger suppresses the effect).

**Consequences / Phase 1 limits.**
- A handler exception, a malformed entry, or an unknown job type is **logged and left in
  the PEL** for now. Phase 2 replaces this with retry-with-backoff and the DLQ. Until then,
  such a job is stuck, not lost (there's no reaper yet).
- Each worker process uses a unique consumer name (`host-pid-random`), so restarted workers
  leave idle consumer records in the group. Phase 2/3 cleans up consumers that are idle and
  have no pending entries. Deleting a consumer that still has pending entries would drop
  those entries from the PEL, which is loss.
- `cpu_task` runs on the event loop and blocks it while hashing. Phase 2 must make sure a
  CPU-bound handler can't starve the heartbeat task (or document the limit).


---

## ADR-023: Leases are PEL idle time; the reaper runs inside the fetch loop

*Status: accepted (Phase 2: `reclaim.lua`, `reaper.py`, `Worker._reap`).*

**Context.** A crashed, stalled, or partitioned worker leaves its entries in the PEL. Some
other worker must notice and take them over without violating the in-flight cap.

**Options.**
1. **A separate reaper task** that claims on its own timer. It can claim while every slot is
   busy. Claimed entries then sit unworked in this worker's PEL, age past the lease again, and
   get reclaimed again, inflating delivery counts toward a false DEAD (ADR-008).
2. **The reaper inside the fetch loop**: each iteration, if a pass is due, `XAUTOCLAIM` at most
   `free` entries (the same free-slot count the `XREADGROUP` uses), start them, then read new
   entries into the slots that are left.
3. Redis 8.8's `XREADGROUP … CLAIM`: excluded by ADR-004 (classic commands only).

**Decision.** (2).
- The lease is the entry's idle time; `XAUTOCLAIM min-idle-time = visibility_timeout`
  (default 30 s, like SQS's default visibility timeout). Reaper passes run every
  `reap_interval` (5 s); if the scan cursor says more PEL is left, the next pass runs on the
  next loop iteration.
- `reclaim.lua` wraps `XAUTOCLAIM` so that each claimed entry's **delivery count** (from
  `XPENDING`) comes back in the same round trip, and the `reclaimed` counter is bumped
  atomically with the claim. The worker sends an entry whose count exceeds `max_deliveries`
  to the DLQ unrun (ADR-027).
- A busy worker (no free slots) doesn't reap: it couldn't run what it claimed. Workers with
  spare capacity reap instead.
- `XAUTOCLAIM`'s third reply element (pending ids whose stream entry is gone) is logged at
  ERROR. Our exits always ack before deleting, so it should never happen.

**Consequences.**
- Recovery latency after a crash is at most `visibility_timeout + reap_interval` plus
  however long it takes until some worker has a free slot. When every worker is saturated,
  stale jobs wait like any queued job.
- Lease length trade-off (SPEC §10): a short lease recovers crashed jobs faster but turns
  more stalls (GC pauses, network blips) into reclaims and suppressed duplicates. Heartbeats
  (ADR-025) decouple job duration from the lease, so the lease only has to cover a
  heartbeat interval plus Redis latency hiccups. 30 s with 10 s heartbeats tolerates two
  consecutive missed beats.

---

## ADR-024: Ownership-checked transitions: XPENDING first, then LEASE_LOST or TERMINAL

*Status: accepted (Phase 2: `heartbeat.lua`, `retry.lua`, `dead.lua`, `transitions.py`).*

**Context.** `XACK` and `XCLAIM` don't check who owns an entry (ADR-001). A worker that
stalled past its lease and then resumes still holds the entry id. Without a check it could
schedule a retry of a job another worker already committed, move it to the DLQ, or
heartbeat the lease back from the new owner (SPEC §4).

**Decision.** Every non-commit transition is one Lua script that starts with
`XPENDING <stream> <group> <id> <id> 1` and compares the owner with the caller:
- **Not the owner** (or no longer pending): change nothing except the `lease_lost` counter,
  and return `LEASE_LOST`.
- **Owner, but the job already has a terminal state** (retry and dead only): another copy of
  the same job_id finished first. That happens with the duplicate entries a re-sent `XADD`
  creates (ADR-006). Ack and delete this copy, count it in `duplicates_suppressed`, and
  return `TERMINAL`. Retrying would re-run a finished job, and a DLQ move would overwrite
  SUCCEEDED or create a second DLQ entry.
- **Owner, non-terminal**: do the transition.
- Commit is deliberately *not* ownership-checked: it is first-wins on the done key, so any
  holder may commit (SPEC §4, ADR-021).
- **Re-send after a lost reply:** the first run acked the entry, so a re-send returns
  `LEASE_LOST` and changes nothing (ADR-006).
- **Deviation from SPEC §4's wording:** the heartbeat has no terminal-state check. SPEC asks
  every non-commit change to check both. But a heartbeat only moves the lease clock: it
  can't overwrite an outcome, so there's nothing for that check to protect. Returning
  TERMINAL would stop heartbeats on a redundant copy, making the lease lapse and adding a
  reclaim; the copy's commit is suppressed either way.
- **A worker that loses its lease keeps running the handler.** It stops heartbeating. Its
  commit is first-wins, and its retry or DLQ move would be refused. We considered
  cancelling the handler: that's unsafe for arbitrary handler code, impossible for a
  process-pool job, and it saves only redundant work.

**Consequences.** The stale-worker test runs all three ways a stale worker can resume
(retry refused, DLQ move refused, commit suppressed) and ends with one terminal state, one
result, and one effect each time. A unit test checks that no write precedes the `XPENDING`
check in any of the three scripts. Mutation checks: see PROGRESS.md, Phase 2.

---

## ADR-025: Heartbeats: `XCLAIM … JUSTID` to self, one task per job, opt-out per handler

*Status: accepted (Phase 2: `heartbeat.lua`, `Worker._heartbeat_loop`).*

**Context.** A job longer than the lease must keep its lease, or it's reclaimed and run twice.

**Options.** (a) `XCLAIM <me> 0 <id> JUSTID`: resets idle; (b) `XCLAIM` without `JUSTID`:
also resets idle but **increments the delivery counter**, so each heartbeat would push the job
toward max_deliveries; (c) a separate lease key with a TTL: more state that could disagree
with the PEL, which is what XAUTOCLAIM actually reads.

**Decision.** (a), after the ownership check (ADR-024). Verified on Redis 8.8.3: idle goes to
~0 and the delivery count is unchanged. `test_heartbeat_resets_idle_without_bumping_delivery_count`
asserts both.
- Every job gets a heartbeat task running beside its handler, every `heartbeat_interval`
  (default 10 s). The config requires `2 × heartbeat_interval ≤ visibility_timeout`, so one
  lost beat never expires a healthy lease.
- `register(..., heartbeat=False)` opts a handler out. The `slow` chaos handler uses it so
  its lease is guaranteed to lapse (ADR-007).
- A heartbeat that fails with a Redis error is logged, and the loop tries again next
  interval. On `LEASE_LOST` it stops (ADR-024).

**Consequences.**
- The heartbeat runs on the event loop, so anything that blocks the loop starves it. That's
  why blocking handlers go to pools (ADR-028).
- **Limitation: a handler that hangs forever while its worker is healthy keeps its lease
  forever.** It is never reclaimed and never DEAD: stuck, not lost (it stays in the PEL and
  shows up in `XPENDING`). A per-handler timeout that counts as a failed attempt is future
  work. It's simple for async handlers (`asyncio.wait_for`), but needs a kill for
  process-pool jobs. No Phase 4 chaos job hangs forever.

---

## ADR-026: Retries: full-jitter backoff, a delayed sorted set, and an atomic scheduler

*Status: accepted (Phase 2: `backoff.py`, `retry.lua`, `schedule.lua`, `scheduler.py`).*

**Context.** A handler that raises should run again later, not immediately, and a burst of
failures shouldn't come back as a synchronized wave.

**Options.** (a) sleep in the worker, then rerun: holds a slot and the lease, and a crash
loses the timer; (b) re-`XADD` immediately with a "not before" field: workers would spin
re-reading jobs that aren't due; (c) a **sorted set scored by due time**, plus a mover.

**Decision.** (c).
- `retry.lua` (ownership-checked) copies the entry's own fields, bumps `attempt`, and `ZADD`s
  the JSON-encoded field list scored by `now + delay` on Redis's clock (ADR-020). Then it
  acks and deletes the entry. The retry is exactly the enqueued job, with the same job_id,
  idempotency key, and `enqueued_at_ms`, so end-to-end latency includes the retries.
- Delay: `random(0, min(cap, base × 2^attempt))`, AWS's "full jitter". It is computed in
  Python so the formula is unit-tested with a seeded RNG. Defaults: base 1 s, cap 300 s.
- `schedule.lua` moves up to `scheduler_batch` due members into the stream in one atomic
  step, so every worker can run the mover concurrently without double moves. A test runs
  ten racing schedulers over 200 retries. The mover runs every `scheduler_interval`
  (0.5 s), so a retry fires at most that late. A full batch triggers another pass
  immediately.
- `max_attempts` counts handler runs (first try + retries). A retry is a fresh stream
  entry, so the delivery count restarts at 1 for each attempt, and a job's total runs are
  bounded by `max_attempts × max_deliveries`.

**Consequences.** The delayed set is part of the queue's depth (SPEC §4 backpressure counts
`XLEN + ZCARD delayed`). Two copies of one job failing at the same attempt produce the
identical member, which `ZADD` collapses into one retry.

---

## ADR-027: The DLQ: a stream, a DEAD terminal state, four reasons, and requeue by job_id

*Status: accepted (Phase 2: `dead.lua`, `requeue.lua`, `dlq.py`, `ftq dlq list|requeue`).*

**Context.** SPEC §4 needs two poison paths (max attempts, max deliveries), a DLQ holding the
job with its last error, attempts, and time, and a CLI to list and requeue.

**Decision.**
- **One DLQ-writing script, `dead.lua`** (ownership-checked), for four reasons:
  `max_attempts` (the handler kept raising), `max_deliveries` (the entry kept being
  redelivered, i.e. the job keeps crashing its worker), `malformed` (unparseable entry),
  and `unknown_type` (no handler registered). The last two go straight to the DLQ:
  retrying can't help, it only burns attempts.
- **max_deliveries is checked by the worker** before it runs a reclaimed entry: count >
  `max_deliveries` means DLQ, unrun. It could live inside `reclaim.lua`, but then two
  scripts would write DLQ entries. Keeping one costs a round trip only on this rare path.
- **The DLQ entry** is the job's original fields verbatim plus `dlq_job_id`, `dlq_reason`,
  `dlq_error`, `dlq_attempts` (handler runs started), `dlq_deliveries`, `dlq_dead_at_ms`,
  `dlq_worker_id`, and `dlq_source_entry_id`. A malformed entry without a job_id gets
  `entry:<entry id>`.
- **The terminal state DEAD** goes in the job's done hash with `dead_entry_id`. It has **no
  TTL**: a DEAD record lives as long as its DLQ entry. DEAD never replaces SUCCEEDED
  (ADR-024). A late success replaces DEAD and deletes the DLQ entry (ADR-009).
- **`requeue <job_id>`** (`requeue.lua`, atomic): only if the state is DEAD, re-add the
  job's fields with `attempt = 0`, delete the DLQ entry, clear the DEAD record. The job
  keeps its job_id, so the ledger still suppresses effects that already happened: a
  requeued job that charged a card before failing won't charge it again (tested).
  `--all` pages through the DLQ with an exclusive start id, so an entry that can't be
  requeued isn't retried forever.

**Consequences.** The DLQ holds exactly the jobs whose state is DEAD, which keeps chaos
invariant I3 simple. `max_deliveries` defaults to 10 for now; Phase 4 sizes it against the
fault schedule (ADR-008).

---

## ADR-028: Blocking and CPU-bound handlers run in a thread or process pool

*Status: accepted (Phase 2: `Registry.register_sync`, `Worker._run_handler`, `cpu_task`).*

**Context.** Heartbeats (ADR-025), the fetch loop, and every Redis call share the worker's
event loop. Phase 1's `cpu_task` hashed on the loop: during a long run nothing else on the
loop ran, the heartbeat couldn't fire, the lease lapsed, and a reaper would take a healthy
job away (a duplicate run, and a step toward a false DEAD). Measured with a 1 s lease
(`bench/lease_starvation.py`, raw output in `results/local/lease_starvation.txt`): a 2.5 s
loop-blocking job's PEL idle time reached **~2490 ms**, the whole run, with no heartbeat in
between. The same kind of work as a process-pool `cpu_task` peaked at **199 to 206 ms**, one
heartbeat interval, in 3 of 3 runs.

**Options.**
1. **Keep it on the loop and make the lease longer than the longest job.** Crash recovery
   then takes as long as the longest job, which defeats heartbeats.
2. **Threads for everything.** Pure-Python CPU work holds the GIL; the loop gets it back at
   every switch interval (5 ms), so heartbeats would limp along. But CPU jobs gain no
   parallelism, and loop latency degrades for every other job.
3. **Heartbeats on their own OS thread with a sync Redis client.** That survives a blocked
   loop, but it keeps a lease alive for a loop that is stuck, i.e. it hides a hung worker.
   The lease should mean "the loop that will commit this job is alive".
4. **A process pool for CPU-bound work and a thread pool for blocking I/O**, with async
   handlers staying on the loop.

**Decision.** (4). `registry.register_sync(type, pool="process" | "thread")` for a plain
function `fn(job: Job) -> result`, run via `loop.run_in_executor`.
- Sync handlers get the `Job` only: it's picklable, and the ledger (an async Redis client)
  belongs to the loop. A job that computes and has an effect is an async handler that
  offloads the computation itself, or two jobs.
- **Process pool:** size `process_pool_size` (default 2), created on first use. It uses the
  **`spawn`** start method on every OS: forking a process that runs an event loop and
  Redis connections copies them in an undefined state, and spawn is the macOS default
  anyway, so laptop and Linux containers behave alike. Spawn re-imports the parent's main
  module, which is why `ftq/__main__.py` now has an `if __name__ == "__main__"` guard.
  Children ignore SIGINT, so Ctrl-C drains the worker instead of failing the jobs.
  Registration rejects nested functions, which can't be pickled by import path.
- **A dead pool child** (killed, or crashed the interpreter) raises `BrokenProcessPool`. The
  worker replaces the pool, and the jobs that were running in it count as failed attempts.
- **Shutdown past the grace period** terminates the pool's children. Otherwise the
  interpreter would wait for them at exit, and the grace period wouldn't bound shutdown
  (tested). Python 3.12 has no public API for this, so we use the pool's private process
  map; 3.14's `terminate_workers()` is the public equivalent.
- **Thread pool:** one thread per slot (`concurrency`), so a blocking handler never waits
  for a thread. A thread can't be killed, so an abandoned blocking handler keeps the
  process alive until it returns, and `docker stop` escalates to SIGKILL.
- `cpu_task` is now a process-pool handler.

**Consequences.**
- Process-pool jobs pay for pickling the job and result, plus IPC. That goes in the Phase 6
  benchmark. CPU throughput per worker now scales with `process_pool_size`.
- An async handler that blocks the loop is still possible (a user bug). The control test
  documents the symptom: the lease lapses under a healthy worker.
- Tests: a real `ftq worker` subprocess runs a `cpu_task` for over two leases while a
  second worker's reaper is armed. The entry's PEL idle time, sampled from the test process,
  never reaches the lease, and the job is never reclaimed. The same measurement on a
  loop-blocking handler shows the lease lapsing. A thread-pool blocking-I/O job also keeps
  its lease.

---

## ADR-029: Prune idle consumers only when they own zero pending entries

*Status: accepted (Phase 2: `prune_consumers.lua`, `Reaper.prune_consumers`, the worker's
maintenance loop).*

**Context.** Consumer names are unique per process (ADR-022), so every worker restart leaves
a consumer record in the group, and they accumulate. `XGROUP DELCONSUMER` removes one, but
it also **discards the consumer's pending entries**. Those entries leave the PEL, no
`XAUTOCLAIM` can ever find them, and the jobs are silently lost. A crashed worker's consumer
is precisely an idle consumer that still owns entries.

**Options.**
1. Never prune. Safe, but records grow without bound across restarts (small, but `XINFO` gets
   noisy, and chaos runs restart workers constantly).
2. Prune by idle time alone. That loses the jobs of any crashed consumer whose entries
   haven't been reclaimed yet (all workers busy, or a lease longer than the prune
   threshold).
3. **Prune only consumers that own no pending entries, with the check and the delete in one
   Lua script.**
4. Stable consumer names (e.g. the container name), so restarts reuse one record. But an
   old container still draining and its replacement would share a name. The ownership
   check (ADR-024) can't tell them apart, and one could heartbeat or retry the other's
   entries.

**Decision.** (3). `prune_consumers.lua` lists consumers (`XINFO CONSUMERS`). For each one
idle ≥ `consumer_prune_idle` (default 1 h) that isn't the caller, it asks the PEL directly
(`XPENDING <stream> <group> - + 1 <consumer>`) and calls `DELCONSUMER` only if that returns
nothing. The script is atomic, so no read or claim can hand the consumer an entry between
the check and the delete. Every worker runs it every `consumer_prune_interval` (60 s) and
at startup. The idle threshold only avoids churning live consumers; the pending check is
what makes deletion safe. On Redis 7.2+, `idle` means time since the last *attempted*
interaction, so a live worker blocked in `XREADGROUP` stays under `block_ms`. The config
requires the threshold to exceed `block_ms`.

**Consequences.** A crashed worker's record stays until a reaper has moved all its entries
away; then it's pruned at the next pass once its idle time passes the threshold. Tests: a
consumer with pending work survives repeated passes with its entry, owner, and delivery
count intact, and the entry is then reclaimed and completed. The same consumer is pruned
by a live worker once it owns nothing, and a running worker prunes empty consumers while
leaving a crashed one's job in the PEL. A unit test requires the PEL check to come before
the `DELCONSUMER` call. Mutation check: see PROGRESS.md, Phase 2.
