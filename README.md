# DataGuard Agent

**Pipeline Sentinel** — an MCP-native AI agent that monitors data pipelines, traces failures to root cause, and files structured incidents. Built for data engineers who want LLM-driven triage without vendor lock-in.

---

```
$ claude "Check all pipeline statuses and diagnose any failures in the last 6 hours"

● list_pipelines → 12 pipelines, 2 degraded, 1 failed
● get_failure_details → customer_ltv_daily: KeyError on column 'account_type'
● trace_lineage → upstream: crm_accounts last succeeded 14h ago
● diagnose_failure →
    root_cause: schema_drift (confidence: 0.94)
    evidence: crm_accounts dropped column 'account_type' at 2024-01-15T03:12Z
    similar_incidents: [INC-047 resolved by schema backfill, 2023-11-02]
    recommended_action: backfill column or update downstream SELECT list
● file_incident → INC-089 created
```

*[full asciinema recording — placeholder URL]*

---

## Quick Start

**Requirements:** Docker, Docker Compose v2, and an MCP client (Claude Desktop, Cursor, or the CLI).

```bash
git clone https://github.com/dataguard-agent/dataguard-agent
cd dataguard-agent
make demo
```

This starts Airflow (with intentionally failing DAGs), Postgres, Redis, Marquez, Prometheus, and the Sentinel MCP server. Demo data is seeded automatically.

**Claude Desktop config** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "pipeline-sentinel": {
      "command": "docker",
      "args": ["exec", "-i", "dataguard-sentinel", "pipeline-sentinel", "mcp"]
    }
  }
}
```

See [examples/claude-desktop-config.json](examples/claude-desktop-config.json) for all client configs.

---

## MCP Tools

| Tool | Description |
|------|-------------|
| `list_pipelines` | All pipelines with status, SLA compliance, orchestrator |
| `get_pipeline_status` | Run history, duration trends, upstream/downstream graph |
| `get_failure_details` | Full error context: stack trace, log excerpt, retry history |
| `trace_lineage` | Traverse OpenLineage graph up/downstream to configurable depth |
| `diagnose_failure` | Root cause with confidence score, evidence, and historical matches |
| `propose_remediation` | Ordered remediation plan with risk level and rollback |
| `check_data_quality` | Schema drift, volume anomaly, freshness checks per dataset |
| `get_recent_incidents` | Incident history filtered by time window, severity, status |
| `file_incident` | Create incident record; optionally push to Jira/PagerDuty/GitHub |
| `execute_remediation` | **Opt-in only.** Executes approved plans; full audit trail |

Full schema reference: [docs/mcp-tools.md](docs/mcp-tools.md)

---

## Architecture

```
LLM Client (Claude, Cursor, GPT, etc.)
        │  MCP Protocol
        ▼
┌─────────────────────────────────────┐
│     dataguard-agents (Runtime)      │
│  (TriageAgent, Watchdog, Reports)   │
└──────────────────┬──────────────────┘
                   ▼
┌─────────────────────────────────────┐
│     Pipeline Sentinel MCP Server    │
│  (packages/sentinel: Tools, FastMCP)│
└──────────┬──────────────────────────┘
           │
  ┌────────┼──────────────┐
  ▼        ▼              ▼
Adapters  Detectors    Lineage
(Airflow  (Schema,     (OpenLineage
 Argo,     Volume,      / Marquez)
 Registry) Freshness)
  │
  └─── dataguard-core (LLM via litellm, Postgres Repositories, Redis Locks, OTel)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for design rationale and request lifecycle.

---

## Live Action Flow: How It Works

The following sequence diagram shows the real-time execution flow when an agent or user invokes `diagnose_failure`:

```mermaid
sequenceDiagram
    autonumber
    actor Client as LLM Client / Agent (Claude, Cursor, TriageAgent)
    participant Sentinel as Pipeline Sentinel (:8080)
    participant Redis as Redis 7 (Lock & Cache)
    participant Orchestrator as Airflow / Argo REST API
    participant Matcher as Regex Pattern Matcher
    participant Lineage as Marquez / OpenLineage
    participant Detectors as Quality Detectors (Schema, Freshness, Vol)
    participant Postgres as PostgreSQL 16 (Repositories)
    participant LLM as LiteLLM (Claude / GPT / Ollama / Groq)

    Note over Client, Sentinel: 1. Trigger Autonomous Triage
    Client->>Sentinel: diagnose_failure(pipeline_id, run_id)

    Note over Sentinel, Redis: 2. Distributed Mutex Locking
    Sentinel->>Redis: acquire_lock("lock:diagnose:{pipeline_id}:{run_id}", ttl=60s)
    Redis-->>Sentinel: Lock Acquired (token="uuid4")

    Note over Sentinel, Orchestrator: 3. Retrieve Failure Context
    Sentinel->>Orchestrator: GET /api/v1/dags/{pipeline_id}/dagRuns/{run_id}/taskInstances
    Orchestrator-->>Sentinel: Failing task metadata & head/tail truncated logs

    Note over Sentinel, Matcher: 4. Deterministic Fast-Path (< 10ms)
    Sentinel->>Matcher: Scan logs for OOM, ConnectionTimeout, HTTP 503, KeyError, SchemaMismatch
    alt Deterministic Pattern Hit (Known High-Frequency Failure)
        Matcher-->>Sentinel: Match Found! (Category: schema_drift, Confidence: 0.95)
        Note over Sentinel: Short-Circuit: LLM reasoning bypassed (0 tokens, <10ms latency)
    else No Pattern Hit (Novel Failure Mode)
        Note over Sentinel, Detectors: 5. Concurrent Evidence Gathering (asyncio.gather)
        par Upstream Lineage
            Sentinel->>Lineage: trace_upstream(dataset_id)
            Lineage-->>Sentinel: Upstream lineage graph & last successful runs
        and Quality Assertions
            Sentinel->>Detectors: check(dataset_id)
            Detectors-->>Sentinel: SchemaDrift / Volume / Freshness metrics
        and Historical Similarity
            Sentinel->>Postgres: IncidentRepository.find_similar(pipeline_id)
            Postgres-->>Sentinel: Historical incidents & past resolutions
        end

        Note over Sentinel, LLM: 6. Structured LLM Reasoning
        Sentinel->>LLM: complete_structured(enriched prompt, schema=DiagnosisResult)
        LLM-->>Sentinel: Validated DiagnosisResult(root_cause, confidence, recommendation)
    end

    Note over Sentinel, Postgres: 7. State Persistence
    Sentinel->>Postgres: IncidentRepository.create(incident_details)
    Postgres-->>Sentinel: Created (INC-089)

    Note over Sentinel, Redis: 8. Release Distributed Lock
    Sentinel->>Redis: release_lock(token="uuid4") via atomic Lua script
    Redis-->>Sentinel: Mutex released

    Sentinel-->>Client: Return DiagnosisResult + Incident Reference (INC-089)
```

---

## Deployment

DataGuard Agent supports multiple open-source deployment topologies:
- **Local Evaluation:** `make demo` (Airflow, Postgres, Redis, Marquez, Prometheus, Grafana)
- **Production Kubernetes:** Helm chart at `deploy/helm/dataguard-sentinel/`
- **GitOps Continuous Delivery:** ArgoCD Application manifest at `deploy/argocd/application.yaml`

See the complete [Open-Source Deployment Guide](docs/deployment.md).

---

## vs. Existing Tools

| | Pipeline Sentinel | Monte Carlo | Datafold | Bigeye |
|---|---|---|---|---|
| Open source | ✓ | ✗ | ✗ | ✗ |
| MCP-native | ✓ | ✗ | ✗ | ✗ |
| Self-hosted | ✓ | ✗ | Partial | ✗ |
| Agent-driven triage | ✓ | Partial | ✗ | ✗ |
| Lineage integration | OpenLineage | Proprietary | dbt | Limited |
| Orchestrator support | Airflow, Argo | Airflow, dbt | dbt | Airflow |

---

## Why We Built This

Data engineers spend most of their week firefighting broken pipelines. Existing observability tools are expensive, closed-source, and built for dashboards — not for AI agents. Pipeline Sentinel closes that gap: MCP-native from day one, fully self-hosted, and designed to plug into any LLM workflow that supports the Model Context Protocol.

---

## Status

**v0.1 — active development.** Airflow and Argo adapters functional. Dagster and Prefect stubs only. See [ROADMAP.md](ROADMAP.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
