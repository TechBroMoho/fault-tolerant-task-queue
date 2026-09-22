# Single entry point for every project command (SPEC §6).
# Written for GNU Make 3.81 (the macOS default): no .ONESHELL, no newer features.

UV ?= uv
COMPOSE ?= docker compose

.DEFAULT_GOAL := help

.PHONY: help setup fmt fmt-check lint typecheck test check up down chaos bench \
        aws-plan aws-up aws-bench aws-down aws-verify-clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-18s %s\n", $$1, $$2}'

# ---------------------------------------------------------------- dev loop

setup: ## Install the pinned toolchain and deps from uv.lock
	$(UV) sync --locked

fmt: ## Auto-format and apply safe lint fixes
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

fmt-check: ## Fail if any file is not formatted
	$(UV) run ruff format --check .

lint: ## Lint with ruff
	$(UV) run ruff check .

typecheck: ## mypy --strict (config in pyproject.toml)
	$(UV) run mypy

test: ## Run pytest; integration tests need Redis (`make up`) and fail, not skip, without it
	$(UV) run pytest

check: fmt-check lint typecheck test ## Everything CI runs: fmt-check + lint + typecheck + tests

# ---------------------------------------------------------------- local stack

up: ## Start the local stack and wait until healthy (Phase 0: Redis only)
	$(COMPOSE) up -d --wait redis

down: ## Stop the local stack and delete its data volume (dev data is disposable)
	$(COMPOSE) down -v

# ---------------------------------------------------------------- later phases (stubs)
# Stubs exit non-zero so nothing can mistake "not built yet" for "passed".

chaos: ## Chaos run + verifier (Phase 4)
	@echo "make chaos: not implemented yet (Phase 4)" >&2; exit 1

bench: ## Local load test + charts (Phase 6)
	@echo "make bench: not implemented yet (Phase 6)" >&2; exit 1

# ---------------------------------------------------------------- AWS (BILLABLE except plan/verify)

aws-plan: ## terraform plan + cost estimate, $0 (Phase 7)
	@echo "make aws-plan: not implemented yet (Phase 7)" >&2; exit 1

aws-up: ## BILLABLE: apply after typed confirmation (Phase 7)
	@echo "make aws-up: not implemented yet (Phase 7)" >&2; exit 1

aws-bench: ## BILLABLE: run the benchmark suite in AWS (Phase 8)
	@echo "make aws-bench: not implemented yet (Phase 8)" >&2; exit 1

aws-down: ## Tear down all AWS resources (Phase 7)
	@echo "make aws-down: not implemented yet (Phase 7)" >&2; exit 1

aws-verify-clean: ## Verify nothing billable is left running, $0 (Phase 7)
	@echo "make aws-verify-clean: not implemented yet (Phase 7)" >&2; exit 1
