# ftq: Fault-Tolerant Task Queue

A from-scratch, Celery-style distributed task queue on Redis Streams: at-least-once delivery,
effectively-once side effects via idempotency keys, leases with heartbeats, retries with backoff,
a dead-letter queue, backpressure, and a chaos-testing harness that kills workers and cuts
connections mid-job, then verifies no job was lost or duplicated.

> **Status:** early development (Phase 1: core queue, single worker). See [PROGRESS.md](PROGRESS.md).
> No performance or correctness numbers are claimed yet. Every number that appears here later
> will link to a raw result file and the command that reproduces it.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
make setup   # install Python 3.12 deps from uv.lock
make up      # start Redis (Docker Compose)
make check   # format check + lint + mypy --strict + tests
```

Try it by hand (Phase 1: one worker, happy path; retries/DLQ/reclaim arrive in Phase 2):

```bash
uv run ftq worker                      # Ctrl-C / SIGTERM drains in-flight jobs, then exits
uv run ftq enqueue send_email --payload '{"to": "ada@example.com"}' --idempotency-key signup-ada
```

The Python API is `await Client(redis, settings).enqueue("send_email", {...}, idempotency_key=...)`
(an in-process library call; there is no HTTP API, see ADR-015).

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
| `FTQ_LOG_LEVEL` | `INFO` | INFO logs lifecycle events only; per-job lines are DEBUG. |
| `FTQ_DONE_TTL_SECONDS` | `604800` | TTL of done/ledger keys; 0 = never expire (chaos and tests). Must outlast any possible redelivery of the job (ADR-010). |
| `FTQ_IDEMPOTENCY_TTL_SECONDS` | `86400` | How long an enqueue idempotency key maps to its original job_id. |

Design: [docs/SPEC.md](docs/SPEC.md) · Decisions: [docs/DECISIONS.md](docs/DECISIONS.md)

## License

MIT. See [LICENSE](LICENSE).
