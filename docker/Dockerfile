# ftq worker image: multi-stage, non-root, small (SPEC §6).
#
# Stage 1 installs the locked dependencies and the package into a virtualenv with uv.
# Stage 2 copies only that virtualenv onto a slim Python base, so the runtime image has
# no uv, no build tools, and no source tree beyond the installed package.
# Build for AWS x86 instances with: docker buildx build --platform linux/amd64 ...

FROM ghcr.io/astral-sh/uv:0.11.16 AS uv

FROM python:3.12.13-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /src
# Dependencies first (their own layer: cached until uv.lock changes), then the package.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.12.13-slim-bookworm
RUN useradd --system --uid 10001 --no-create-home ftq
COPY --from=build /opt/venv /opt/venv
# The benchmark harness rides along (a few KB): Phase 8 runs the same load generator as
# an ECS task from this image (`python -m bench.loadgen`, run from /opt/ftq). It imports
# only ftq and the stdlib; the charts (matplotlib) run on the host.
COPY bench /opt/ftq/bench
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1
USER ftq
# SIGTERM (docker stop / ECS) drains in-flight jobs within FTQ_SHUTDOWN_GRACE.
STOPSIGNAL SIGTERM
ENTRYPOINT ["ftq"]
CMD ["worker"]
