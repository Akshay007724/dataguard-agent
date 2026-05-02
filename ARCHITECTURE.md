# Architecture

## Design Principles

**Agent-first, not dashboard-first.** Every capability is exposed as an MCP tool with a clean JSON schema. The HTTP API and CLI are thin wrappers. There is no UI — the LLM client is the UI.

**Read-only by default.** The system requires only read access to orchestrators, lineage stores, and data sources. `execute_remediation` is opt-in per pipeline, gated behind explicit configuration and a multi-condition check at call time.

**Adapter isolation.** Orchestrator-specific code never leaks beyond its adapter module. The MCP tools operate against `OrchestratorAdapter`, an abstract base class, making new orchestrator support a matter of implementing ~10 async methods.

---

## Monorepo Layout (`uv workspaces`)

Three installable packages with a strict unidirectional dependency graph:

```
pipeline-sentinel
    └── dataguard-adapters
            └── dataguard-core
```

**`dataguard-core`** (`packages/core/`) — shared primitives:
- LLM client abstraction via `litellm` (Anthropic, OpenAI, Azure OpenAI, Ollama, any litellm-supported provider)
- Async state store: SQLAlchemy 2.0 + asyncpg for Postgres, redis-py async for Redis
- Observability: `structlog` with trace context, `prometheus-client`, OpenTelemetry SDK
- Pydantic v2 settings with environment-variable binding

**`dataguard-adapters`** (`packages/adapters/`) — orchestrator clients:
- `AirflowAdapter` — Airflow REST API v2 via `apache-airflow-client`
- `ArgoAdapter` — Argo Workflows API via `hera-workflows` (Intuit's official SDK)
- `DagsterAdapter`, `PrefectAdapter` — stubs, v0.2

**`pipeline-sentinel`** (`packages/sentinel/`) — the runnable agent:
- MCP server (Anthropic `mcp` SDK)
- Quality detectors: schema drift, volume anomaly, freshness
- Lineage tracer: OpenLineage / Marquez
- FastAPI HTTP wrapper
- Typer CLI

---

## Request Lifecycle: `diagnose_failure`

```
MCP Client
  └─ diagnose_failure(pipeline_id, run_id)
       │
       ├─ 1. OrchestratorAdapter.get_run_details()     → RunDetails
       ├─ 2. OrchestratorAdapter.get_run_logs()        → compressed log excerpt
       ├─ 3. LineageTracer.trace_upstream(dataset_id)  → LineageGraph
       ├─ 4. DetectorRegistry.run_checks(dataset_id)   → List[DetectorResult]
       ├─ 5. IncidentStore.find_similar(embeddings)    → List[HistoricalIncident]
       │
       └─ 6. LLMProvider.complete(structured_prompt)
                └─ DiagnosisResult(
                     root_cause_category: RootCauseCategory,
                     confidence: float,          # 0.0–1.0
                     evidence: list[str],
                     similar_incidents: list[IncidentRef],
                     recommended_action: str
                   )
```

Step 6 uses a hybrid approach:
- **Deterministic pattern matchers** run first (OOM, connection timeout, schema drift markers in logs). When a matcher fires with high confidence, the LLM call is skipped — saving tokens and latency.
- **LLM reasoning** handles novel failure modes: it receives the compressed log, lineage context, detector results, and top-3 historical incidents as RAG context, then returns a `DiagnosisResult` as structured JSON output.

---

## LLM Integration

Provider abstraction uses `litellm`, which normalizes the completion API across providers. Structured output is enforced by passing a Pydantic model's JSON schema as the `response_format`. This means:

- Anthropic: uses `tool_use` with schema enforcement
- OpenAI/Azure: uses `response_format={"type": "json_schema", ...}`
- Ollama: uses grammar-constrained generation where supported

The `LLMClient` wrapper in `dataguard-core` handles provider routing, retry with exponential backoff, token usage logging, and span attribution for traces.

---

## State Store

**Postgres 16** (via SQLAlchemy 2.0 async + asyncpg) is the system of record:
- `incidents` — all filed incidents with status, severity, resolution
- `incident_patterns` — known failure patterns with matcher logic
- `remediation_playbooks` — ordered steps per pattern, risk level, rollback
- `pipeline_registry` — pipeline metadata cache (TTL-refreshed)
- `remediation_audit` — immutable audit log for every `execute_remediation` call

**Redis 7** (via redis-py async) is used for:
- Short-lived adapter cache: pipeline status, run details (TTL 60s)
- Distributed lock: prevents concurrent `diagnose_failure` calls on the same pipeline from racing to the LLM

Migrations are managed with Alembic. The migration history lives in `packages/core/src/dataguard_core/store/migrations/`.

---

## Observability

The system instruments its own operation at three layers:

**Prometheus metrics** (exposed on `:9090/metrics`):
- `sentinel_mcp_tool_duration_seconds` — histogram per tool
- `sentinel_mcp_tool_errors_total` — counter per tool × error type
- `sentinel_llm_tokens_total` — counter per provider × model × direction
- `sentinel_adapter_request_duration_seconds` — histogram per adapter × operation

**OpenTelemetry traces** propagated across:
- MCP tool invocation
- Each adapter call
- Each detector run
- LLM completion (with token counts as span attributes)

Traces let you reconstruct exactly what evidence the agent gathered and why it reached a conclusion.

**structlog** structured JSON logs with `trace_id` and `span_id` injected from OTel context, making log → trace correlation trivial in any log aggregator.

---

## Security Model

- All orchestrator credentials come from environment variables or mounted Kubernetes secrets. No credentials in code or default configs.
- Airflow and Argo connections require only read permissions. The system never writes to orchestrators.
- `execute_remediation` requires all four conditions to be true simultaneously:
  1. `auto_remediation_enabled: true` in pipeline config
  2. `confirm: true` in the tool call arguments
  3. A non-empty `approver_id`
  4. The remediation plan's `risk_level` ≤ `auto_remediation_max_risk` in config
- Every `execute_remediation` call writes an immutable record to the `remediation_audit` table before any action is taken.

---

## Deployment

**Local development:** `docker compose up` — see [docs/deployment.md](docs/deployment.md).

**Production:** Helm chart at `deploy/helm/dataguard-sentinel/`. The chart includes:
- Deployment with configurable replicas and resource limits
- Non-root security context (UID 65532)
- Prometheus scrape annotations
- Optional ingress
- Dependencies: Bitnami Postgres and Redis subcharts (swap for existing cluster instances via `postgresql.enabled: false`)

**GitOps:** ArgoCD Application manifest at `deploy/argocd/application.yaml`. Configured for automated sync with self-heal and prune.
