# Single entry point for every project command (SPEC §6).
# Written for GNU Make 3.81 (the macOS default): no .ONESHELL, no newer features.

UV ?= uv
COMPOSE ?= docker compose
WORKERS ?= 0
N ?= 10000
CHAOS_WORKERS ?= 8
BENCH_ARGS ?=

.DEFAULT_GOAL := help

.PHONY: help setup fmt fmt-check lint typecheck test test-all check check-all up down chaos bench \
        aws-base aws-image aws-plan aws-up aws-smoke aws-bench aws-bench-plan aws-bench-local aws-down aws-verify-clean

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

test: ## Fast tests (not `slow`); integration tests need Redis (`make up`) and fail, not skip, without it
	$(UV) run pytest -m "not slow"

test-all: ## Every test, including the `slow` subprocess/process-pool ones (what CI runs)
	$(UV) run pytest

check: fmt-check lint typecheck test ## Dev loop: fmt-check + lint + typecheck + fast tests

check-all: fmt-check lint typecheck test-all ## What CI runs: fmt-check + lint + typecheck + every test

# ---------------------------------------------------------------- local stack

up: ## Start Redis, plus WORKERS=N worker containers (default 0), and wait until healthy
ifeq ($(WORKERS),0)
	$(COMPOSE) up -d --wait redis
else
	$(COMPOSE) --profile workers up -d --wait --build --scale worker=$(WORKERS) redis worker
endif

down: ## Stop the local stack (workers too) and delete its data volume (dev data is disposable)
	$(COMPOSE) --profile workers down -v

# ---------------------------------------------------------------- chaos and benchmarks

chaos: ## Chaos run + verifier: make chaos N=100000 [CHAOS_WORKERS=8] [SEED=s] -> results/local/chaos_report.json
	$(UV) run python -m chaos.run --jobs $(N) --workers $(CHAOS_WORKERS) $(if $(SEED),--seed $(SEED),)

bench: ## Local benchmark: scaling, latency, backpressure + charts -> results/local/bench/ (~25 min; run in background)
	$(UV) run python -m bench.run scaling $(BENCH_ARGS)
	$(UV) run python -m bench.run latency --skip-build $(BENCH_ARGS)
	$(UV) run python -m bench.run backpressure --skip-build $(BENCH_ARGS)
	$(UV) run python -m bench.plot

# ---------------------------------------------------------------- AWS (BILLABLE except plan/verify)

# The account is on the Free plan: if the credits run out, it's closed (CLAUDE.md). Every
# billable step prints an itemized estimate and needs a typed confirmation first.
AWS_ENV := AWS_PROFILE=$(or $(AWS_PROFILE),ftq) AWS_REGION=us-west-2 AWS_PAGER=
TF_BASE := deploy/terraform/base
TF_STACK := deploy/terraform/stack
IMAGE_TAG ?= $(shell git rev-parse --short HEAD)
# Fleet size, e.g. TF_VARS="-var worker_hosts=6 -var workers=12 -var loadgen_hosts=1"
TF_VARS ?=

aws-base: ## Budget alarm + ECR repo (idle cost $0); needs TF_VAR_alert_email
	@test -n "$$TF_VAR_alert_email" || { echo "set TF_VAR_alert_email" >&2; exit 1; }
	$(AWS_ENV) terraform -chdir=$(TF_BASE) init -input=false
	$(AWS_ENV) terraform -chdir=$(TF_BASE) apply -input=false

aws-image: ## Build linux/amd64 and push to ECR as the git short SHA (ECR storage: cents)
	@git diff --quiet HEAD -- src bench docker pyproject.toml uv.lock || { echo "commit first: the tag must name the code" >&2; exit 1; }
	$(AWS_ENV) sh -c 'repo=$$(terraform -chdir=$(TF_BASE) output -raw repository_url) && \
	  aws ecr get-login-password | docker login --username AWS --password-stdin "$${repo%%/*}" && \
	  docker buildx build --platform linux/amd64 -f docker/Dockerfile -t "$$repo:$(IMAGE_TAG)" --push .'

aws-plan: ## terraform plan + itemized cost estimate, $0
	$(AWS_ENV) terraform -chdir=$(TF_STACK) init -input=false
	$(AWS_ENV) terraform -chdir=$(TF_STACK) plan -input=false -out=tfplan -var image_tag=$(IMAGE_TAG) $(TF_VARS)
	$(AWS_ENV) terraform -chdir=$(TF_STACK) show -json tfplan > $(TF_STACK)/tfplan.json
	$(AWS_ENV) $(UV) run python -m deploy.aws estimate $(TF_STACK)/tfplan.json

aws-up: ## BILLABLE: apply the saved plan after a typed confirmation
	@test -f $(TF_STACK)/tfplan || { echo "run make aws-plan first" >&2; exit 1; }
	@$(AWS_ENV) $(UV) run python -m deploy.aws estimate $(TF_STACK)/tfplan.json
	@printf 'This starts billable resources. Type "apply" to continue: '; read ans; test "$$ans" = apply
	$(AWS_ENV) terraform -chdir=$(TF_STACK) apply -input=false tfplan
	rm -f $(TF_STACK)/tfplan $(TF_STACK)/tfplan.json

aws-smoke: ## BILLABLE (stack must be up): ftq bench as an ECS task, exactly-once checked
	$(AWS_ENV) $(UV) run python -m deploy.aws smoke $(SMOKE_ARGS)

aws-bench: ## BILLABLE (stack up): the Phase 8 session -> results/aws/ (run in background; BENCH_ARGS=...)
	$(AWS_ENV) $(UV) run python -m deploy.bench run $(BENCH_ARGS)

aws-bench-plan: ## The Phase 8 session's points, time and cost, $0
	$(UV) run python -m deploy.bench plan --per-hour 0.8487 $(BENCH_ARGS)

aws-bench-local: ## The same driver on local Docker (tiny points) -> bench/runs/awsbench-local/
	docker build -q -f docker/Dockerfile -t ftq-worker:local . >/dev/null
	$(UV) run python -m deploy.bench run --backend local --out bench/runs/awsbench-local --redo \
	  --worker-counts 1 2 --scaling-measure 5 --headline-repeats 2 --headline-measure 5 \
	  --backpressure-measure 5 --headline-workers 2 --warmup 2 --cooldown 1 --processes 2 $(BENCH_ARGS)

aws-down: ## Destroy the stack, delete ECR images, then verify-clean (safe to run any time)
	$(AWS_ENV) terraform -chdir=$(TF_STACK) init -input=false >/dev/null
	$(AWS_ENV) terraform -chdir=$(TF_STACK) destroy -input=false -auto-approve -var image_tag=$(IMAGE_TAG) $(TF_VARS)
	@# Several passes: buildx pushes a manifest list plus child manifests, and a child
	@# can't be deleted in the same call as the list that references it (Phase 7). A
	@# batch-delete's failures come back in its JSON with exit 0, so count what's left.
	$(AWS_ENV) sh -c 'for pass in 1 2 3; do \
	  ids=$$(aws ecr list-images --repository-name ftq --query imageIds --output json 2>/dev/null) || exit 0; \
	  [ "$$ids" = "[]" ] && exit 0; \
	  aws ecr batch-delete-image --repository-name ftq --image-ids "$$ids" --query "length(failures)" --output text; \
	 done; echo "ECR images left after 3 passes" >&2; exit 1'
	$(MAKE) aws-verify-clean

aws-verify-clean: ## Fail unless nothing billable is left in us-west-2, $0
	$(AWS_ENV) $(UV) run python -m deploy.aws verify-clean
