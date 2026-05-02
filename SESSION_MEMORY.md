# SESSION_MEMORY.md

Last updated: 2026-05-01

## Project

**dataguard-agent** — MCP-native agentic observability + incident triage for data pipelines.
Apache 2.0. uv workspaces monorepo.

---

## Architecture (agreed)

```
dataguard-agents  →  pipeline-sentinel  →  dataguard-adapters  →  dataguard-core
```

### Packages

| Package | Key contents |
|---|---|
| `packages/core/` (`dataguard-core`) | `LLMClient` (litellm), SQLAlchemy 2.0 async store, redis-py store, structlog + OTel + Prometheus, Pydantic v2 `Settings` |
| `packages/adapters/` (`dataguard-adapters`) | `AirflowAdapter`, `ArgoAdapter` (both httpx-based). `OrchestratorAdapter` ABC. `_trim_log` static method |
| `packages/sentinel/` (`pipeline-sentinel`) | MCP server (`mcp` SDK), 7 regex pattern matchers + LLM fallback diagnosis, `SchemaDriftDetector`, `VolumeAnomalyDetector`, `FreshnessDetector`, `LineageTracer` (Marquez), FastAPI HTTP wrapper, Typer CLI |
| `packages/agents/` (`dataguard-agents`) | `TriageAgent` (litellm tool-use loop, max 30 turns, Anthropic prompt caching), `WatchdogAgent` (asyncio polling, Redis debounce 1h TTL), `TriageReport` |

### Key design decisions
- **Hybrid diagnosis**: 7 deterministic regex matchers run first → LLM only on novel failures
- **Distributed lock**: Redis lock on `diagnose:{pipeline_id}` prevents concurrent diagnosis races
- **5-gate remediation**: `confirm` + `approver_id` + plan exists + `auto_remediation_enabled` + risk check
- **Prompt caching**: Anthropic `cache_control: ephemeral` on system prompt in agentic loop
- **Watchdog debounce**: `watchdog:triaged:{pipeline_id}` Redis key (1h TTL) prevents duplicate incidents

---

## Completed

### Scaffolding
- [x] Root `pyproject.toml` — uv workspace, ruff, mypy strict, pytest
- [x] `Makefile`, `Dockerfile` (multi-stage), `docker-compose.yml` (8 services)
- [x] `.github/workflows/ci.yml` — lint, typecheck, test-unit, test-integration, docker-build, helm-lint
- [x] Demo DAGs: 4 Airflow DAGs (3 fail in distinct ways: KeyError, ECONNREFUSED, timeout)

### dataguard-core
- [x] `config.py` — `Settings(BaseSettings)`
- [x] `llm/client.py` — `LLMClient.complete()` + `complete_structured()`, tenacity retry (3 attempts)
- [x] `llm/base.py` — `LLMResponse`, `LLMUsage`
- [x] `store/postgres.py` — ORM: `IncidentRow`, `RemediationPlanRow`, `RemediationAuditRow`
- [x] `store/redis.py` — `cache_get/set/delete`, `acquire_lock`, `release_lock`
- [x] `logging.py`, `metrics.py`, `tracing.py`
- [x] `__init__.py` filled

### dataguard-adapters
- [x] `base.py` — `OrchestratorAdapter` ABC, `RunStatus`, `RunDetails`, `PipelineSummary`, `_trim_log`
- [x] `airflow.py` — httpx against Airflow REST API v1
- [x] `argo.py` — httpx against Argo Workflows API
- [x] `__init__.py` filled (exports all public classes)

### pipeline-sentinel
- [x] `mcp_server/server.py` — `run_stdio_server()`, `_dispatch()` match for all 10 tools
- [x] `mcp_server/tools/diagnosis.py` — 7 patterns + `handle_diagnose_failure` + Redis lock
- [x] `mcp_server/tools/remediation.py` — 5-gate `handle_execute_remediation`
- [x] `mcp_server/tools/incidents.py`, `pipelines.py`, `quality.py`
- [x] `mcp_server/prompts/diagnosis.py` — `DIAGNOSIS_SYSTEM_PROMPT`, `build_diagnosis_prompt`
- [x] `detectors/base.py`, `schema_drift.py`, `volume_anomaly.py`, `freshness.py`, `custom_sql.py`
- [x] `lineage/tracer.py`, `lineage/openlineage.py`
- [x] `api/app.py`, `api/routes.py`
- [x] `cli/main.py`
- [x] All `__init__.py` files filled

### dataguard-agents
- [x] `base.py` — `AgentContext`, `ToolRegistry`, `AGENT_TOOLS` (7 tool definitions)
- [x] `triage.py` — `TriageAgent.run()` agentic loop
- [x] `watchdog.py` — `WatchdogAgent.run_forever()` / `run_once()`
- [x] `report.py` — `TriageReport`, `build_report_from_conversation()`, `_extract_incident_ids()`
- [x] `__init__.py` filled (exports `AgentContext`, `TriageAgent`, `TriageReport`, `WatchdogAgent`)

### Tests (unit, no infra required)
- [x] `packages/sentinel/tests/unit/test_pattern_matchers.py` — 17 parametrized pattern hit cases + miss/edge cases
- [x] `packages/sentinel/tests/unit/test_detectors.py` — `SchemaDriftDetector._compare_schemas` + `VolumeAnomalyDetector._severity`
- [x] `packages/adapters/tests/unit/test_log_trim.py` — `OrchestratorAdapter._trim_log`
- [x] `packages/core/tests/unit/test_llm_client.py` — `LLMClient` init, `complete()` with mocked litellm
- [x] `packages/agents/tests/unit/test_triage_report.py` — `build_report_from_conversation` + `_extract_incident_ids`
- [x] `packages/agents/tests/unit/test_watchdog.py` — 7 tests: debounce skip, new triage, mixed, no failures, adapter error, triage error continues, custom TTL
- [x] `packages/agents/tests/unit/test_triage_agent.py` — 7 tests: stop turn, tool dispatch, scope prompt, max turns, tool error, end_turn reason, timestamps

### Infra fixes
- [x] `conftest.py` (root) — sets `DATABASE_URL`/`REDIS_URL`/`ANTHROPIC_API_KEY` before module import
- [x] `pyproject.toml` — added `--import-mode=importlib` to fix multi-package `tests.unit` namespace collision

---

## Remaining TODOs

### High priority
- [ ] Integration tests (`@pytest.mark.integration`) for adapters with testcontainers
- [ ] E2E smoke test: `make demo` → call MCP tool → verify incident filed
- [x] `packages/agents/tests/unit/test_watchdog.py` — done
- [x] `packages/agents/tests/unit/test_triage_agent.py` — done

### Medium priority
- [ ] `docs/adapters.md` — how to add a new orchestrator adapter
- [ ] `docs/detectors.md` — how to add a custom detector
- [ ] `docs/deployment.md` — Helm chart + ArgoCD walkthrough
- [ ] Helm chart (`deploy/helm/`) — currently scaffolded but empty
- [ ] `packages/sentinel/src/pipeline_sentinel/config.py` — verify all env vars documented

### Low priority
- [ ] Dagster / Prefect adapter stubs (currently empty placeholders)
- [ ] Historical incident search in `diagnose_failure` (v0.2 stub in code)
- [ ] `mkdocs.yml` site structure + `docs/index.md`

---

## Run commands

```bash
make install          # install all workspace packages
make test             # unit tests (no infra)
make test-integration # needs Postgres + Redis
make demo             # full docker compose stack
make lint && make typecheck
```

Single test: `uv run pytest packages/agents/tests/unit/test_triage_report.py -v`

---

## How to use this file

Run `/compress` or `/clear` to free message tokens, then paste this into the next session as context. Or tell Claude: "Read SESSION_MEMORY.md and continue."

Update this file at the end of each session with: "Update SESSION_MEMORY.md with today's progress."
