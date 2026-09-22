# ftq: Fault-Tolerant Task Queue

A from-scratch, Celery-style distributed task queue on Redis Streams: at-least-once delivery,
effectively-once side effects via idempotency keys, leases with heartbeats, retries with backoff,
a dead-letter queue, backpressure, and a chaos-testing harness that kills workers and cuts
connections mid-job, then verifies no job was lost or duplicated.

> **Status:** early development (Phase 0: bootstrap). See [PROGRESS.md](PROGRESS.md).
> No performance or correctness numbers are claimed yet. Every number that appears here later
> will link to a raw result file and the command that reproduces it.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
make setup   # install Python 3.12 deps from uv.lock
make up      # start Redis (Docker Compose)
make check   # format check + lint + mypy --strict + tests
```

Design: [docs/SPEC.md](docs/SPEC.md) · Decisions: [docs/DECISIONS.md](docs/DECISIONS.md)

## License

MIT. See [LICENSE](LICENSE).
