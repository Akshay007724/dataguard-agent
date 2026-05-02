# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make install          # uv sync --frozen (installs all workspace packages)
make test             # unit tests only — no infrastructure required
make test-integration # needs Postgres + Redis (run make demo first)
make test-all         # full suite with coverage report
make lint             # ruff check
make fmt              # ruff format + fix
make typecheck        # mypy --strict + pyright (both must pass)
make pre-commit       # all pre-commit hooks against all files
make demo             # docker compose up + seed failing pipelines
make demo-down        # stop stack, keep volumes
make clean            # stop stack + remove volumes + build artifacts
make docs-serve       # mkdocs live reload at http://localhost:8000
make build            # docker build dataguard/pipeline-sentinel:dev
```

Single test: `uv run pytest packages/sentinel/tests/unit/test_foo.py::test_bar -v`

## Architecture

Monorepo using `uv workspaces`. Four packages with a strict unidirectional dependency graph:

```
dataguard-agents  →  pipeline-sentinel  →  dataguard-adapters  →  dataguard-core
```

- **`packages/core/`** (`dataguard-core`) — shared primitives: LLM client (via `litellm`), async state store (SQLAlchemy 2.0 + asyncpg for Postgres, redis-py for Redis), observability (structlog, prometheus-client, OpenTelemetry), Pydantic v2 settings
- **`packages/adapters/`** (`dataguard-adapters`) — orchestrator clients: `AirflowAdapter` (apache-airflow-client), `ArgoAdapter` (hera-workflows). Dagster/Prefect are stubs. All implement `OrchestratorAdapter` abstract base in `base.py`
- **`packages/sentinel/`** (`pipeline-sentinel`) — runnable agent: MCP server (`mcp` SDK), quality detectors, lineage tracer (OpenLineage/Marquez), FastAPI HTTP wrapper, Typer CLI
- **`packages/agents/`** (`dataguard-agents`) — autonomous agents: `TriageAgent` (agentic litellm tool-use loop, max 30 turns), `WatchdogAgent` (asyncio polling with Redis debounce), `TriageReport` output model

Entry point: `pipeline-sentinel serve` → `packages/sentinel/src/pipeline_sentinel/cli/main.py`
MCP server: `packages/sentinel/src/pipeline_sentinel/mcp_server/server.py`
Each MCP tool is one file in `mcp_server/tools/`.

Agents entry: `dataguard_agents.TriageAgent` / `dataguard_agents.WatchdogAgent` — both consume `AgentContext` and call MCP tools directly via `ToolRegistry → _dispatch()`.

## Key Design Decisions

- **LLM abstraction**: `litellm` handles all providers (Anthropic, OpenAI, Azure, Ollama). No per-provider files.
- **`diagnose_failure` is hybrid**: deterministic pattern matchers run first; LLM only called for novel failures. Priors skip the LLM call entirely for known patterns (OOM, connection timeout, etc).
- **Read-only by default**: `execute_remediation` requires `auto_remediation_enabled: true` in pipeline config + `confirm: true` in call + `approver_id` + risk level check. All four must pass.
- **Redis for locking**: prevents concurrent `diagnose_failure` calls on the same pipeline from racing.
- **Structured output enforcement**: Pydantic model JSON schema passed as `response_format` to LLM — never parse free-text LLM output.

## Type Checking

`mypy --strict` AND `pyright` both must pass. CI enforces both. The `litellm`, `hera`, and `openlineage` stubs are in `[[tool.mypy.overrides]]` with `ignore_missing_imports = true`.

## Adding an Orchestrator Adapter

1. Implement all abstract methods in `packages/adapters/src/dataguard_adapters/base.py`
2. Add integration tests using `testcontainers` where possible
3. Register in `dataguard_adapters/__init__.py`
4. Document in `docs/adapters.md`

## Local Demo Stack

`make demo` starts: Airflow (`:8888`), Sentinel (`:8080`), Postgres (`:5432`), Redis (`:6379`), Marquez (`:5000`), Prometheus (`:9091`), Grafana (`:3000`). Demo DAGs are seeded to fail in three distinct ways (schema drift, source unavailable, SLA violation).

MCP config for Claude Desktop after `make demo`:
```json
{"mcpServers": {"pipeline-sentinel": {"command": "docker", "args": ["exec", "-i", "dataguard-sentinel", "pipeline-sentinel", "mcp"]}}}
```
