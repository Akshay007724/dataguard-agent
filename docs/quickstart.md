# Quickstart

## Prerequisites

- Docker and Docker Compose v2
- An MCP client: [Claude Desktop](https://claude.ai/download), Cursor, or the `mcp` CLI

## Start the demo stack

```bash
git clone https://github.com/dataguard-agent/dataguard-agent
cd dataguard-agent
make demo
```

This starts: Airflow (with intentionally failing DAGs), Postgres, Redis, Marquez (OpenLineage), Prometheus, Grafana, and the Sentinel MCP server. Demo data is seeded automatically.

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8888 | admin / admin |
| Sentinel API | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Marquez | http://localhost:5000 | — |

## Connect your MCP client

**Claude Desktop** — add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

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

Restart Claude Desktop, then try:

> "Check all pipeline statuses and diagnose any failures in the last 6 hours."

## What to expect

The demo stack has three DAGs seeded to fail in different ways:

- `customer_ltv_daily` — schema drift: upstream `crm_accounts` dropped a column
- `inventory_sync_hourly` — source unavailability: external API returning 503
- `product_features_weekly` — SLA violation: runtime exceeds configured threshold

`diagnose_failure` will identify the root cause, surface historical incidents, and propose a remediation for each.

## Tear down

```bash
make demo-down   # keeps volumes
make clean       # removes everything including volumes
```
