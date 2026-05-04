#!/usr/bin/env bash
# Seed the local demo stack with pipeline metadata and trigger intentional failures.
# Run automatically by 'make demo' after 'docker compose up'.
set -euo pipefail

SENTINEL_URL="${SENTINEL_URL:-http://localhost:8080}"
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8888}"
MARQUEZ_URL="${MARQUEZ_URL:-http://localhost:5002}"

wait_for() {
  local url="$1"
  local name="$2"
  local attempts=0
  echo "Waiting for $name at $url..."
  until curl -sf "$url" > /dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      echo "ERROR: $name did not become ready after 30 attempts"
      exit 1
    fi
    sleep 3
  done
  echo "$name is ready."
}

wait_for "$AIRFLOW_URL/health" "Airflow"
wait_for "$SENTINEL_URL/health" "Pipeline Sentinel"
wait_for "$MARQUEZ_URL/api/v1/namespaces" "Marquez"

echo ""
echo "Seeding Airflow demo DAGs..."
# Unpause all demo DAGs so the scheduler picks them up
for dag in customer_ltv_daily inventory_sync_hourly product_features_weekly crm_accounts_export; do
  curl -sf -X PATCH "$AIRFLOW_URL/api/v1/dags/$dag" \
    -H "Content-Type: application/json" \
    -u admin:admin \
    -d '{"is_paused": false}' > /dev/null || true
done

echo "Seeding OpenLineage events in Marquez..."
curl -sf -X POST "$MARQUEZ_URL/api/v1/lineage" \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "START",
    "eventTime": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'",
    "run": {"runId": "demo-seed-001"},
    "job": {"namespace": "demo", "name": "crm_accounts_export"},
    "inputs": [],
    "outputs": [{"namespace": "demo", "name": "crm_accounts", "facets": {}}],
    "producer": "seed-script"
  }' > /dev/null || true

echo ""
echo "Demo data seeded. The following DAGs are active and will produce failures:"
echo "  customer_ltv_daily     — fails on schema drift (crm_accounts missing column)"
echo "  inventory_sync_hourly  — fails on source unavailability (external API down)"
echo "  product_features_weekly — SLA violation (runtime > threshold)"
echo ""
echo "Ask your MCP client: 'Diagnose all failing pipelines and file incidents for critical ones.'"
