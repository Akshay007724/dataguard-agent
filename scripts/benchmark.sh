#!/usr/bin/env bash
# Benchmark MCP tool latencies against the local demo stack.
# Requires: make demo (stack running), jq, curl
set -euo pipefail

SENTINEL_URL="${SENTINEL_URL:-http://localhost:8080}"
RUNS="${RUNS:-10}"

echo "Benchmarking Pipeline Sentinel MCP tools ($RUNS runs each)"
echo "Target: $SENTINEL_URL"
echo ""

benchmark_tool() {
  local tool="$1"
  local payload="$2"
  local total_ms=0

  for i in $(seq 1 "$RUNS"); do
    start=$(date +%s%3N)
    curl -sf -X POST "$SENTINEL_URL/mcp/call" \
      -H "Content-Type: application/json" \
      -d "{\"tool\": \"$tool\", \"arguments\": $payload}" > /dev/null
    end=$(date +%s%3N)
    total_ms=$((total_ms + end - start))
  done

  avg=$((total_ms / RUNS))
  printf "  %-30s avg %4dms over %d runs\n" "$tool" "$avg" "$RUNS"
}

benchmark_tool "list_pipelines"       '{}'
benchmark_tool "get_pipeline_status"  '{"pipeline_id": "customer_ltv_daily"}'
benchmark_tool "get_failure_details"  '{"pipeline_id": "customer_ltv_daily"}'
benchmark_tool "check_data_quality"   '{"dataset_id": "crm_accounts"}'
benchmark_tool "diagnose_failure"     '{"pipeline_id": "customer_ltv_daily"}'

echo ""
echo "Note: diagnose_failure latency includes LLM completion time."
echo "Set LLM_PROVIDER=none to benchmark the non-LLM path only."
