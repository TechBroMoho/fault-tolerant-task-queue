# ftq: Fault-Tolerant Task Queue

A from-scratch, Celery-style distributed task queue on Redis Streams: at-least-once delivery,
effectively-once side effects via idempotency keys, leases with heartbeats, retries with backoff,
a dead-letter queue, backpressure, and a chaos-testing harness that kills workers and cuts
connections mid-job, then verifies no job was lost or duplicated.

> **Status:** early development (Phase 3: multiple workers, backpressure, per-job timeouts,
> stats, JSON logs). See [PROGRESS.md](PROGRESS.md).
> No performance or correctness numbers are claimed yet. Every number that appears here later
> will link to a raw result file and the command that reproduces it.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
make setup      # install Python 3.12 deps from uv.lock
make up         # start Redis (Docker Compose); `make up WORKERS=4` adds 4 worker containers
make check      # format check + lint + mypy --strict + fast tests (~16 s)
make check-all  # the same with every test, including the `slow` ones (~55 s; what CI runs)
```

Try it by hand:

```bash
uv run ftq worker                      # Ctrl-C / SIGTERM drains in-flight jobs, then exits
uv run ftq enqueue send_email --payload '{"to": "ada@example.com"}' --idempotency-key signup-ada
uv run ftq enqueue flaky --payload '{"fail_times": 2}'     # fails twice, retried with backoff
uv run ftq enqueue poison                                   # ends in the dead-letter queue
uv run ftq dlq list                                         # DLQ entries as JSON lines
uv run ftq dlq requeue <job_id>                             # or --all
uv run ftq stats                                            # depth, in flight, delayed, DLQ, counters
uv run ftq bench --jobs 50000                               # enqueue N, wait, check exactly-once
```

Built-in handlers: `send_email` (I/O, effect via the ledger), `cpu_task` (CPU-bound, runs in a
process pool), and the deliberate failures `flaky`, `poison`, `crashy` (kills its worker), and
`slow` (no heartbeats). Handlers are `async def` functions on the event loop, or plain functions
registered with `register_sync(..., pool="thread" | "process")` for blocking or CPU-bound work
(ADR-028). Every run has a timeout (`FTQ_JOB_TIMEOUT`, or `timeout=` per handler type); a run
that exceeds it is a failed attempt (ADR-030).

The Python API is `await Client(redis, settings).enqueue("send_email", {...}, idempotency_key=...)`
or `enqueue_many([...])` (one pipelined round trip). It is an in-process library call; there is
no HTTP API (ADR-015). When the queue is at `FTQ_HIGH_WATERMARK`, enqueue raises `QueueFull`
(`reject` mode) or waits for room (`block` mode) until the depth is below `FTQ_LOW_WATERMARK`
(ADR-031).

### Configuration

Every knob is an `FTQ_*` environment variable (`src/ftq/config.py`):

| Env var | Default | Meaning |
|---|---|---|
| `FTQ_REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL. |
| `FTQ_SOCKET_TIMEOUT` | `5.0` | Seconds to wait for any single Redis reply before raising TimeoutError. |
| `FTQ_SOCKET_CONNECT_TIMEOUT` | `2.0` | Seconds to wait for a TCP connect to Redis. |
| `FTQ_HEALTH_CHECK_INTERVAL` | `10.0` | Ping idle pooled connections older than this (s) before reuse; 0 disables. |
| `FTQ_RETRY_ATTEMPTS` | `3` | Client-side retries of a command after a connection error or timeout. |
| `FTQ_RETRY_BACKOFF_BASE` | `0.05` | Base (s) of the jittered exponential retry backoff. |
| `FTQ_RETRY_BACKOFF_CAP` | `1.0` | Cap (s) on a single retry backoff sleep. |
| `FTQ_QUEUE` | `default` | Queue name (one stream + group each). |
| `FTQ_GROUP` | `workers` | Consumer group name within the queue. |
| `FTQ_CONCURRENCY` | `10` | Max jobs in flight per worker process (also the XREADGROUP COUNT cap). |
| `FTQ_BLOCK_MS` | `1000` | XREADGROUP BLOCK timeout (ms). Bounds how long a stop request waits. |
| `FTQ_SHUTDOWN_GRACE` | `30.0` | On SIGTERM, seconds to let in-flight jobs finish before abandoning them. |
| `FTQ_PROCESS_POOL_SIZE` | `2` | Processes for CPU-bound handlers registered with pool='process' (ADR-028). Thread-pool handlers get `concurrency` threads. |
| `FTQ_JOB_TIMEOUT` | `300.0` | Seconds one handler run may take before it counts as a failed attempt (ADR-030). A handler type can override it at registration. |
| `FTQ_VISIBILITY_TIMEOUT` | `30.0` | Lease length (s): an entry idle this long in the PEL is reclaimed by XAUTOCLAIM. |
| `FTQ_HEARTBEAT_INTERVAL` | `10.0` | Seconds between lease extensions of a running job; at most lease / 3. |
| `FTQ_REAP_INTERVAL` | `5.0` | Seconds between reaper passes (XAUTOCLAIM of expired leases). |
| `FTQ_MAX_ATTEMPTS` | `5` | Handler runs (first try + retries) before a failing job goes to the DLQ. |
| `FTQ_MAX_DELIVERIES` | `10` | Deliveries of one stream entry (XPENDING count) before it goes to the DLQ unrun: the job keeps crashing its worker. |
| `FTQ_JOB_BACKOFF_BASE` | `1.0` | Retry delay base (s): delay = random(0, min(cap, base * 2^attempt)). |
| `FTQ_JOB_BACKOFF_CAP` | `300.0` | Cap (s) on a single retry delay. |
| `FTQ_SCHEDULER_INTERVAL` | `0.5` | Seconds between moves of due retries from the delayed set to the stream. |
| `FTQ_SCHEDULER_BATCH` | `500` | Max due retries moved per scheduler pass. |
| `FTQ_CONSUMER_PRUNE_IDLE` | `3600.0` | Delete a group consumer idle this long (s), but ONLY if it owns zero pending entries (deleting one that does would drop its jobs from the PEL). |
| `FTQ_CONSUMER_PRUNE_INTERVAL` | `60.0` | Seconds between consumer-cleanup passes. |
| `FTQ_BACKPRESSURE_MODE` | `reject` | When the queue is full: 'reject' raises QueueFull at once; 'block' waits (up to block_timeout) for the depth to fall below the low watermark. |
| `FTQ_HIGH_WATERMARK` | `100000` | Queue depth (stream length + delayed retries) at which enqueue stops accepting jobs. |
| `FTQ_LOW_WATERMARK` | `80000` | Once full, enqueue accepts jobs again only when depth falls below this. |
| `FTQ_BLOCK_TIMEOUT` | `30.0` | In 'block' mode, seconds to wait for room before raising QueueFull. |
| `FTQ_BLOCK_POLL_INTERVAL` | `0.05` | In 'block' mode, seconds between admission retries while full. |
| `FTQ_LOG_LEVEL` | `INFO` | INFO logs lifecycle events only; per-job lines are DEBUG. |
| `FTQ_LOG_FORMAT` | `json` | 'json': one object per line with job_id/attempt/worker_id fields. |
| `FTQ_DONE_TTL_SECONDS` | `604800` | TTL of done/ledger keys; 0 = never expire (chaos and tests). Must outlast any possible redelivery of the job (ADR-010). |
| `FTQ_IDEMPOTENCY_TTL_SECONDS` | `86400` | How long an enqueue idempotency key maps to its original job_id. |

Design: [docs/SPEC.md](docs/SPEC.md) · Decisions: [docs/DECISIONS.md](docs/DECISIONS.md)

## License

MIT. See [LICENSE](LICENSE).
