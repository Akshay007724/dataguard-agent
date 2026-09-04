# Architecture

## Design Principles

**Agent-first, not dashboard-first.** Every capability is exposed as a Model Context Protocol (MCP) tool with a strict JSON schema. The HTTP REST API and CLI are thin wrappers over the same tool definitions. There is no custom web UI — the LLM client (Claude Desktop, Claude Code, Cursor, LangChain/LlamaIndex agents) is the UI.

**Read-only by default.** The system requires only read access to orchestrators, lineage stores, and data sources. `execute_remediation` is opt-in per pipeline, gated behind explicit configuration, approval tokens, and strict risk thresholds.

**Adapter isolation.** Orchestrator-specific code never leaks beyond its adapter module. The MCP tools operate against `OrchestratorAdapter`, an abstract base class, making new orchestrator support a matter of implementing async REST or SDK client methods.

**Layered intelligence.** Deterministic regex pattern matching executes before invoking LLM reasoning. High-frequency known failures (OOM, database connection timeouts, upstream failures, common key errors) resolve in < 10ms with 100% confidence, saving LLM tokens and eliminating latency.

---

## Monorepo Layout (`uv workspaces`)

Four installable packages with a strict unidirectional dependency graph:

```
dataguard-agents
    └── pipeline-sentinel
            └── dataguard-adapters
                    └── dataguard-core
```

### 1. `dataguard-core` (`packages/core/`) — Shared Primitives
- **LLM Client**: `LLMClient` wrapping `litellm` (Anthropic, OpenAI, Azure OpenAI, Ollama, AWS Bedrock, vLLM) with automatic retry, exponential backoff, structured output enforcement (`Pydantic` schema validation), and Prometheus token tracking.
- **Async State Store**: SQLAlchemy 2.0 async engine + `asyncpg` for PostgreSQL, `redis.asyncio` for Redis caching and distributed locking.
- **Repository Layer**: `IncidentRepository`, `RemediationRepository`, and `AuditRepository` abstracting DB access away from tool handlers.
- **Distributed Locking**: Atomic UUID-tokenized locks in Redis via Lua scripts to prevent concurrent triage races.
- **Observability**: `structlog` with automated OpenTelemetry trace/span context injection, `prometheus-client` counters/histograms, and OTel SDK configuration.
- **Configuration**: Pydantic v2 `BaseSettings` with clean default values and environment-variable binding.

### 2. `dataguard-adapters` (`packages/adapters/`) — Orchestrator Clients
- **`AirflowAdapter`**: Asynchronous HTTP client (`httpx.AsyncClient`) interacting directly with Airflow 2.0+ REST API (`/api/v1/dags/...`). Supports basic/bearer auth, run filtering, log retrieval, and task execution details.
- **`ArgoAdapter`**: Asynchronous HTTP client (`httpx.AsyncClient`) for Argo Workflows REST API (`/api/v1/workflows/...`).
- **`DagsterAdapter` & `PrefectAdapter`**: Pluggable adapter stubs ready for GraphQL and REST integration in v0.2.
- **`AdapterRegistry`**: Multi-orchestrator registry enabling concurrent pipeline queries, dynamic adapter routing, and unified discovery across heterogeneous stacks.

### 3. `pipeline-sentinel` (`packages/sentinel/`) — The Observability Engine & MCP Server
- **MCP Server**: Built on FastMCP, serving 10 standard tools for pipeline status, failure details, lineage tracing, automated diagnosis, remediation planning, data quality checks, and incident logging.
- **Tool Registry**: Central single source of truth generating both FastMCP `Tool` specs and LiteLLM function calling schemas with zero duplication.
- **Lineage Tracer**: OpenLineage / Marquez client (`MarquezClient`) navigating dataset input/output graphs and upstream dependency chains.
- **Quality Detectors**:
  - `SchemaDriftDetector`: Identifies added, removed, or type-modified columns against Marquez schema baselines.
  - `VolumeAnomalyDetector`: Detects anomalous row count variations using moving standard deviation thresholds.
  - `FreshnessDetector`: Flags stale datasets exceeding configured SLA windows.
  - `CustomSQLDetector`: Validates SQL invariant assertions directly against target warehouses.
- **FastAPI HTTP Service**: Exposes `/health`, `/metrics` (Prometheus), and `/api/mcp/call` (HTTP fallback for webhooks/non-MCP clients) with full lifespan resource management.
- **Typer CLI**: Command-line interface for diagnosing pipelines, inspecting lineage, and running local tests.

### 4. `dataguard-agents` (`packages/agents/`) — Autonomous Agent Runtime
- **`TriageAgent`**: Autonomous multi-turn agent that inspects failed runs, gathers diagnostic evidence through MCP tools, synthesizes root cause analyses, and files incidents.
- **`WatchdogAgent`**: Asynchronous scheduler that polls orchestrators for pipeline failures, debounces notifications using Redis TTL locks, and triggers automated triage.
- **`TriageReport`**: Multi-format reporting engine formatting agent outputs into Slack blocks, Markdown summaries, PagerDuty alerts, or raw JSON.

---

## Request Lifecycle: `diagnose_failure`

```
MCP Client (Claude Desktop / Cursor / TriageAgent)
  └─ diagnose_failure(pipeline_id, run_id)
       │
       ├─ 1. Acquire Redis distributed lock ("lock:diagnose:{pipeline_id}:{run_id}")
       │     └─ If locked: return "DIAGNOSIS_IN_PROGRESS"
       │
       ├─ 2. OrchestratorAdapter.get_failure_details()
       │     └─ Fetch run metadata, error logs, and failing task IDs
       │
       ├─ 3. Deterministic Pattern Matchers (_PATTERN_MATCHERS)
       │     ├─ Regex scans logs for OOM, ConnectionTimeout, HTTP 503, KeyError,
       │     │  ColumnNotFound, SchemaMismatch, UpstreamFailed
       │     └─ [MATCH FOUND] → Return immediate DiagnosisResult (Confidence: 0.95, LLM bypassed)
       │
       ├─ 4. [NO MATCH] Concurrent Evidence Gathering (asyncio.gather)
       │     ├─ LineageTracer.trace_upstream(dataset_id)
       │     ├─ QualityDetectors (SchemaDrift, Freshness, VolumeAnomaly)
       │     └─ IncidentRepository.find_similar(pipeline_id)
       │
       ├─ 5. Structured LLM Completion (LLMClient.complete_structured)
       │     ├─ Prompt enriched with log excerpt, lineage context, detector findings, past incidents
       │     └─ Pydantic schema validation returns DiagnosisResult:
       │          - root_cause_category: RootCauseCategory
       │          - confidence: float (0.0 - 1.0)
       │          - evidence: list[str]
       │          - similar_incidents: list[IncidentRef]
       │          - recommended_action: str
       │
       └─ 6. Release Redis distributed lock (Lua script token verification)
```

---

## State Store & Data Models

### PostgreSQL 16 (Relational System of Record)
Interacted via SQLAlchemy 2.0 async + `asyncpg` with dedicated repositories:
- **`incidents`** (`IncidentRepository`):
  - Fields: `id`, `title`, `pipeline_id`, `severity`, `status`, `description`, `diagnosis_id`, `created_at`, `updated_at`, `resolved_at`.
- **`remediation_plans`** (`RemediationRepository`):
  - Fields: `id`, `pipeline_id`, `diagnosis_id`, `steps` (JSONB), `risk_level`, `estimated_resolution_time`, `rollback_plan`, `requires_human_approval`, `created_at`.
- **`remediation_audit`** (`AuditRepository`):
  - Immutable audit trail capturing `id`, `remediation_id`, `approver_id`, `action`, `status`, `executed_at`, `details` (JSONB).

### Redis 7 (Ephemeral Cache & Synchronization)
- **Log and Run Cache**: Short-lived TTL (60s) caching of orchestrator responses to prevent thundering herds.
- **Distributed Mutex**: Safe locking via UUID tokens and atomic Lua release scripts preventing concurrent LLM triage runs.
- **Watchdog Debounce**: Prevents alert storms by recording active incident triage windows.

---

## Observability & Telemetry

### 1. Prometheus Metrics (Exposed on `:8080/metrics`)
- `sentinel_mcp_tool_duration_seconds` — Histogram of tool execution latency labeled by tool name.
- `sentinel_mcp_tool_errors_total` — Counter of tool failures labeled by tool name and exception class.
- `sentinel_llm_tokens_total` — Counter of prompt and completion tokens labeled by model, provider, and direction.
- `sentinel_adapter_request_duration_seconds` — Histogram of orchestrator API call latency.

### 2. OpenTelemetry Tracing
Every MCP tool invocation, adapter HTTP request, detector check, and LLM call creates a span in an OpenTelemetry trace hierarchy. Span attributes include:
- `mcp.tool_name`, `pipeline.id`, `run.id`
- `llm.provider`, `llm.model`, `llm.prompt_tokens`, `llm.completion_tokens`
- Trace IDs are propagated across asynchronous subtasks via `dataguard_core.tracing`.

### 3. Structured Logging (`structlog`)
- Structured JSON output in production, colorized console output in development.
- OTel context injector automatically embeds `trace_id` and `span_id` into every log event.

---

## Security Model

1. **Credential Segregation**: Credentials (API tokens, database passwords, basic auth) are loaded exclusively through environment variables or Kubernetes Secret volume mounts.
2. **Read-Only Orchestration**: Orchestrator adapters require only viewer/reader privileges in Airflow or Argo.
3. **Four-Eye Remediation Gating**:
   `execute_remediation` will refuse execution unless all four conditions are met:
   - `auto_remediation_enabled` is set to `true` in configuration.
   - `confirm: true` is explicitly passed in the MCP tool parameters.
   - A non-empty `approver_id` is supplied.
   - The plan's `risk_level` does not exceed `auto_remediation_max_risk`.
4. **Non-Root Runtime**: Docker containers and Kubernetes Pods execute under UID 65532 with read-only root filesystems and dropped capabilities.
