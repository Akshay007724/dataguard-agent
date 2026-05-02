# Roadmap

## v0.1 — Current

**Core agent:**
- MCP server with all 10 tools
- Airflow adapter (REST API v2, full read support)
- Argo Workflows adapter (hera-workflows, full read support)
- Detectors: schema drift, volume anomaly, freshness
- OpenLineage / Marquez lineage integration
- LLM providers: Anthropic Claude, OpenAI (via litellm)
- State store: Postgres + Redis
- Observability: Prometheus, OTel, structlog

**Infrastructure:**
- Docker Compose local stack with seeded failing pipelines
- Helm chart (production-grade)
- ArgoCD Application manifest
- GitHub Actions CI: lint, typecheck, unit tests, integration tests, Docker build

**Documentation:**
- Quickstart (5-minute demo)
- Architecture rationale
- MCP tools reference with example inputs/outputs
- Adapter implementation guide
- One end-to-end case study

---

## v0.2

- Dagster adapter (full)
- Prefect adapter (full)
- `execute_remediation` — auto-remediation execution with audit trail
- Slack notification integration
- PagerDuty incident integration
- Jira/Linear ticket filing
- Azure OpenAI + Ollama LLM providers
- Embedding-based incident similarity (replace BM25 with vector search)

---

## v0.3

- Web UI (read-only incident browser — the MCP interface remains primary)
- Multi-tenant support (namespace isolation per team)
- Custom SQL quality detector with parameterized rules
- SLA prediction: flag pipelines trending toward SLA breach before failure
- `dbt` lineage adapter

---

## Not Planned

- Proprietary cloud service
- Write access to orchestrators beyond opt-in remediation
- Replacing OpenLineage as the lineage standard
