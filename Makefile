.DEFAULT_GOAL := help

PYTEST  := uv run pytest
MYPY    := uv run mypy
RUFF    := uv run ruff

.PHONY: help install test test-integration test-all lint fmt typecheck \
        pre-commit docs docs-serve demo demo-down clean build

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

install: ## Install all workspace dependencies
	uv sync --frozen

test: ## Run unit tests only
	$(PYTEST) tests/unit packages/*/tests/unit

test-integration: ## Run integration tests (requires Postgres + Redis)
	$(PYTEST) tests/integration packages/*/tests/integration -m integration

test-all: ## Full test suite with coverage report
	$(PYTEST) --cov --cov-report=term-missing --cov-report=html

lint: ## Check style and quality
	$(RUFF) check .

fmt: ## Auto-fix and format
	$(RUFF) format .
	$(RUFF) check --fix .

typecheck: ## mypy + pyright
	$(MYPY) packages/
	uv run pyright packages/

pre-commit: ## Run all pre-commit hooks against all files
	uv run pre-commit run --all-files

docs: ## Build documentation site
	uv run mkdocs build --strict

docs-serve: ## Serve docs locally with live reload
	uv run mkdocs serve

demo: ## Spin up full local stack with seeded failing pipelines
	docker compose up --build -d
	@echo "Waiting for services..."
	@bash scripts/seed-demo-data.sh
	@echo ""
	@echo "  Airflow UI:    http://localhost:8888  (admin / admin)"
	@echo "  Sentinel API:  http://localhost:8080"
	@echo "  Grafana:       http://localhost:3000  (admin / admin)"
	@echo "  Prometheus:    http://localhost:9091"
	@echo "  Marquez API:   http://localhost:5000"

demo-down: ## Tear down local stack (keeps volumes)
	docker compose down

clean: ## Remove all build artifacts and local state
	docker compose down -v --remove-orphans 2>/dev/null || true
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .coverage htmlcov dist site

build: ## Build Docker image
	docker build -t dataguard/pipeline-sentinel:dev .
