# Open-Source Deployment Guide

This guide covers deploying DataGuard Agent across development, staging, and production Kubernetes environments.

---

## 1. System Architecture & Topology

DataGuard Agent is an MCP-native observability and triage platform for data engineering pipelines. In production, the system operates as a stateless service backed by persistent state stores and connected to your orchestrators and telemetry systems.

```mermaid
flowchart TD
    subgraph Clients["LLM & Agent Clients"]
        CD["Claude Desktop"]
        CC["Claude Code CLI"]
        CU["Cursor / Windsurf"]
        AA["Autonomous Agents (TriageAgent/Watchdog)"]
    end

    subgraph Sentinel["Pipeline Sentinel Service"]
        MCP["MCP Server (FastMCP / stdio / SSE)"]
        API["FastAPI (Health, Metrics, REST API)"]
        ENG["Diagnosis & Remediation Engine"]
        DET["Detectors (Schema, Freshness, Volume)"]
    end

    subgraph State["State & Cache"]
        PG[("PostgreSQL 16\nIncidents & Audit Trail")]
        RD[("Redis 7\nDistributed Locks & Cache")]
    end

    subgraph DataPlatform["Data Platform Services"]
        AF["Apache Airflow"]
        AR["Argo Workflows"]
        MQ["Marquez / OpenLineage"]
    end

    subgraph Telemetry["Observability Stack"]
        PR["Prometheus (:8080/metrics)"]
        GF["Grafana Dashboards"]
        OT["OTel Collector (Traces)"]
    end

    Clients -->|MCP Protocol / JSON-RPC| MCP
    Clients -->|REST Fallback| API
    MCP --> ENG
    API --> ENG
    ENG --> DET
    ENG --> PG
    ENG --> RD
    ENG --> AF
    ENG --> AR
    ENG --> MQ
    API -.->|Scrapes /metrics| PR
    PR --> GF
    ENG -.->|OTel Spans| OT
```

---

## 2. Prerequisites

| Component | Minimum Version | Recommended | Notes |
|-----------|-----------------|-------------|-------|
| **Docker** | 24.0+ | Docker Engine 26+ | Required for local container builds |
| **Docker Compose** | v2.20+ | v2.27+ | Compose specification v2 |
| **Kubernetes** | 1.26+ | 1.28+ | For Helm & ArgoCD deployments |
| **Helm** | 3.12+ | 3.14+ | For Kubernetes packaging |
| **PostgreSQL** | 14+ | 16+ | With `asyncpg` compatibility |
| **Redis** | 6.2+ | 7.2+ | In-memory cache & distributed mutex |
| **Python** | 3.11+ | 3.12+ / 3.14 | If running natively from source |
| **LLM Provider API Key** | — | Anthropic / OpenAI / Bedrock | Required for generative triage & root cause reasoning |

---

## 3. Deployment Modes

### Option A: Local Quickstart with Docker Compose

The quickest way to evaluate DataGuard Agent with an end-to-end stack including Airflow, Marquez, PostgreSQL, Redis, Prometheus, and Grafana:

```bash
# 1. Clone repository
git clone https://github.com/dataguard-agent/dataguard-agent.git
cd dataguard-agent

# 2. Configure your LLM API key
export ANTHROPIC_API_KEY="sk-ant-..."
# or export OPENAI_API_KEY="sk-..."

# 3. Spin up full demo stack
make demo
```

Once started, the following services are reachable:

- **Sentinel API & Metrics**: `http://localhost:8080` (Health: `/health`, Metrics: `/metrics`)
- **Airflow Webserver**: `http://localhost:8888` (Credentials: `admin` / `admin`)
- **Marquez OpenLineage UI**: `http://localhost:5002`
- **Prometheus UI**: `http://localhost:9091`
- **Grafana Dashboards**: `http://localhost:3000` (Credentials: `admin` / `admin`)

To stop the stack:
```bash
make demo-down    # Preserves volumes
make clean        # Destroys all containers and data volumes
```

---

### Option B: Production Kubernetes Deployment with Helm

A production-ready Helm chart is provided in [`deploy/helm/dataguard-sentinel`](file:///Users/ad6/Desktop/Projects/Auditability&observability_for_agents/dataguard-agent/deploy/helm/dataguard-sentinel).

#### 1. Create the Secret for Credentials

Create a Kubernetes Secret containing database credentials, orchestrator tokens, and LLM API keys:

```bash
kubectl create namespace dataguard

kubectl create secret generic dataguard-sentinel-secrets \
  --namespace dataguard \
  --from-literal=DATABASE_URL="postgresql+asyncpg://sentinel:SECURE_PASSWORD@postgres.internal:5432/sentinel" \
  --from-literal=REDIS_URL="redis://redis.internal:6379/0" \
  --from-literal=ANTHROPIC_API_KEY="sk-ant-api03-..." \
  --from-literal=AIRFLOW_USERNAME="sentinel-reader" \
  --from-literal=AIRFLOW_PASSWORD="ORCHESTRATOR_PASSWORD"
```

#### 2. Configure `values-production.yaml`

Create an environment-specific values override file:

```yaml
# values-production.yaml
replicaCount: 2

image:
  repository: dataguard/pipeline-sentinel
  tag: "0.1.0"
  pullPolicy: IfNotPresent

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/metrics"

podSecurityContext:
  runAsNonRoot: true
  fsGroup: 65532

securityContext:
  runAsUser: 65532
  runAsGroup: 65532
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: [ALL]

resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 1Gi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  targetCPUUtilizationPercentage: 75

config:
  logLevel: INFO
  logFormat: json
  autoRemediationEnabled: false
  autoRemediationMaxRisk: low

existingSecret: "dataguard-sentinel-secrets"

# External database and cache configurations (disable bundled subcharts)
postgresql:
  enabled: false

redis:
  enabled: false

ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
  hosts:
    - host: sentinel.data.company.internal
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: sentinel-tls-cert
      hosts:
        - sentinel.data.company.internal
```

#### 3. Install via Helm

```bash
helm upgrade --install pipeline-sentinel ./deploy/helm/dataguard-sentinel \
  --namespace dataguard \
  --values values-production.yaml
```

#### 4. Verify Pod Health

```bash
kubectl get pods -n dataguard -l app.kubernetes.io/name=dataguard-sentinel
kubectl logs -n dataguard -l app.kubernetes.io/name=dataguard-sentinel -f
```

---

### Option C: GitOps Continuous Delivery with ArgoCD

DataGuard Agent provides an ArgoCD Application manifest in [`deploy/argocd/application.yaml`](file:///Users/ad6/Desktop/Projects/Auditability&observability_for_agents/dataguard-agent/deploy/argocd/application.yaml).

```bash
kubectl apply -f deploy/argocd/application.yaml
```

The Application manifest is configured with:
- Automated sync with `selfHeal: true` and `prune: true`
- Server-side apply
- Exponential backoff retry policies
- Ignoring differences on `.spec.replicas` so HorizontalPodAutoscaler can manage pod scaling dynamically.

---

### Option D: Direct Container / Standalone Docker

To run the standalone container without Docker Compose:

```bash
docker run -d \
  --name pipeline-sentinel \
  --restart unless-stopped \
  -p 8080:8080 \
  -e DATABASE_URL="postgresql+asyncpg://sentinel:password@postgres:5432/sentinel" \
  -e REDIS_URL="redis://redis:6379/0" \
  -e AIRFLOW_BASE_URL="http://airflow:8080" \
  -e AIRFLOW_USERNAME="admin" \
  -e AIRFLOW_PASSWORD="password" \
  -e OPENLINEAGE_URL="http://marquez:5000" \
  -e ANTHROPIC_API_KEY="sk-ant-..." \
  dataguard/pipeline-sentinel:latest
```

---

## 4. Configuration Reference

All settings are configured via environment variables and validated through Pydantic v2.

### Core Settings (`dataguard-core`)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DATABASE_URL` | String | `postgresql+asyncpg://sentinel:sentinel@localhost:5432/sentinel` | Async PostgreSQL connection string |
| `REDIS_URL` | String | `redis://localhost:6379/0` | Redis connection URL |
| `LOG_LEVEL` | String | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_FORMAT` | String | `json` | Logging format (`json` for production, `console` for dev) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | String | `None` | OpenTelemetry OTLP collector gRPC/HTTP endpoint |
| `DEFAULT_LLM_MODEL` | String | `anthropic/claude-3-5-sonnet-20241022` | Default model identifier for LiteLLM |

### Sentinel Settings (`pipeline-sentinel`)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SENTINEL_API_HOST` | String | `0.0.0.0` | API bind address |
| `SENTINEL_API_PORT` | Integer | `8080` | API and metrics HTTP port |
| `OPENLINEAGE_URL` | String | `http://localhost:5000` | Marquez or OpenLineage API base URL |
| `AUTO_REMEDIATION_ENABLED` | Boolean | `false` | Master toggle for remediation tool |
| `AUTO_REMEDIATION_MAX_RISK` | String | `low` | Maximum risk allowed (`low`, `medium`, `high`) |

### Orchestrator Adapter Settings (`dataguard-adapters`)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `AIRFLOW_BASE_URL` | String | `http://localhost:8080` | Airflow webserver root URL |
| `AIRFLOW_USERNAME` | String | `admin` | Airflow REST API username |
| `AIRFLOW_PASSWORD` | String | `admin` | Airflow REST API password |
| `ARGO_BASE_URL` | String | `http://localhost:2746` | Argo Workflows Server URL |
| `ARGO_TOKEN` | String | `""` | Kubernetes ServiceAccount Bearer token for Argo |
| `ARGO_NAMESPACE` | String | `argo` | Default Kubernetes namespace for workflows |

---

## 5. LLM Provider Setup

DataGuard Agent relies on `litellm` in `dataguard-core` to normalize calls across providers. Set the corresponding environment variables:

### Anthropic Claude (Recommended)
```bash
export ANTHROPIC_API_KEY="sk-ant-api03-..."
export DEFAULT_LLM_MODEL="anthropic/claude-3-5-sonnet-20241022"
```

### OpenAI / Azure OpenAI
```bash
# Direct OpenAI:
export OPENAI_API_KEY="sk-proj-..."
export DEFAULT_LLM_MODEL="openai/gpt-4o"

# Azure OpenAI:
export AZURE_API_KEY="..."
export AZURE_API_BASE="https://your-resource.openai.azure.com/"
export AZURE_API_VERSION="2024-02-01"
export DEFAULT_LLM_MODEL="azure/gpt-4o-deployment"
```

### Local Models (Ollama / vLLM)
For air-gapped or private environments:
```bash
# Ollama:
export DEFAULT_LLM_MODEL="ollama/qwen2.5:32b"
export OLLAMA_API_BASE="http://localhost:11434"

# vLLM (OpenAI-compatible server):
export OPENAI_API_BASE="http://vllm-service:8000/v1"
export OPENAI_API_KEY="none"
export DEFAULT_LLM_MODEL="openai/meta-llama/Llama-3.3-70B-Instruct"
```

---

## 6. Connecting MCP Clients

Once Sentinel is deployed, connect any MCP-compatible client.

### Claude Desktop
Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

#### Via Docker Exec (Local Demo):
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

#### Via Native CLI (Installed via UV):
```json
{
  "mcpServers": {
    "pipeline-sentinel": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/dataguard-agent",
        "run",
        "pipeline-sentinel",
        "mcp"
      ]
    }
  }
}
```

### Cursor IDE
In Cursor **Settings > Features > MCP**, click **Add New MCP Server**:
- **Name**: `pipeline-sentinel`
- **Type**: `command`
- **Command**: `docker exec -i dataguard-sentinel pipeline-sentinel mcp`

### Claude Code CLI
Add Sentinel directly to Claude Code:
```bash
claude mcp add pipeline-sentinel -- docker exec -i dataguard-sentinel pipeline-sentinel mcp
```

### REST API Fallback
For custom automation, webhooks, or agents lacking native MCP transport, Sentinel exposes an HTTP execution route:

```bash
curl -X POST http://localhost:8080/api/mcp/call \
  -H "Content-Type: application/json" \
  -d '{
    "tool": "diagnose_failure",
    "arguments": {
      "pipeline_id": "customer_ltv_daily"
    }
  }'
```

---

## 7. Observability & Monitoring

### Prometheus Metrics
Prometheus scrapes the `/metrics` endpoint on `:8080`.
Key production metrics to monitor:

```promql
# Rate of MCP tool calls per second
sum by (tool) (rate(sentinel_mcp_tool_duration_seconds_count[5m]))

# 95th percentile latency per tool
histogram_quantile(0.95, sum by (le, tool) (rate(sentinel_mcp_tool_duration_seconds_bucket[5m])))

# LLM token consumption rate
sum by (model, type) (rate(sentinel_llm_tokens_total[1h]))

# Error rate
sum by (tool) (rate(sentinel_mcp_tool_errors_total[5m]))
```

### Grafana Dashboard
Import the bundled dashboard from [`deploy/grafana/dashboards/pipeline-sentinel.json`](file:///Users/ad6/Desktop/Projects/Auditability&observability_for_agents/dataguard-agent/deploy/grafana/dashboards/pipeline-sentinel.json) into your Grafana instance to view real-time tool latency, LLM token burn rates, error classifications, and orchestrator health.

---

## 8. Database Migrations

PostgreSQL tables are initialized automatically by the Docker Compose demo script via `deploy/postgres/init.sql`.

For standalone and production deployments:
```bash
# Run migrations using Alembic
uv run alembic upgrade head
```

The database schema manages:
- `incidents`: Central registry of all triaged pipeline failures
- `remediation_plans`: Structured execution playbooks generated during triage
- `remediation_audit`: Append-only, tamper-evident audit log of approved actions

---

## 9. Security Best Practices

1. **Least-Privilege Orchestrator Accounts**: Configure dedicated service accounts in Airflow and Argo with read-only permissions (`Viewer` role in Airflow; `get`, `list`, `watch` verbs on `workflows.argoproj.io` in Kubernetes RBAC).
2. **Four-Eye Remediation Safeguards**: Keep `AUTO_REMEDIATION_ENABLED=false` by default. Even when enabled, low-risk automated remediations require explicit confirmation tokens. High-risk operations (e.g., table drops or backfills) are rejected by policy.
3. **Container Hardening**: The official Sentinel image runs as an unprivileged user (`UID 65532`), with root filesystem mounted read-only, `allowPrivilegeEscalation: false`, and all Linux capabilities dropped.
