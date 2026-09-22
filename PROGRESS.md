# PROGRESS

## Status

- **Current phase:** Phase 0 (bootstrap and plan): **complete**, at the gate awaiting review.
- **Next:** Phase 1 (core queue: single worker, happy path, idempotency). Starts on Mohammed's go-ahead.
- **Repo:** https://github.com/TechBroMoho/fault-tolerant-task-queue (public, default branch `main`, created 2026-09-22).
- **AWS:** nothing created. Spend to date: $0.

## Phase log

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
