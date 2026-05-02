# MCP Tools Reference

All tools are read-only except `execute_remediation`, which requires explicit opt-in.

---

## `list_pipelines`

List pipelines with current status and SLA compliance.

**Input:**
```json
{
  "orchestrator": "airflow | argo | null",
  "tag": "string | null",
  "status": "healthy | degraded | failed | null"
}
```

**Output:** Array of pipeline summaries with `id`, `name`, `orchestrator`, `owner`, `last_run_status`, `last_run_at`, `sla_status`.

---

## `get_pipeline_status`

Detailed status for a single pipeline including run history and dependency graph.

**Input:** `{ "pipeline_id": "string" }`

**Output:** Current state, last 10 run history, average duration, SLA compliance rate, upstream dependencies, downstream consumers.

---

## `get_failure_details`

Full error context for a failed run with smart log truncation.

**Input:** `{ "pipeline_id": "string", "run_id": "string | null" }`

**Output:** Stack trace, log excerpt (first + last N lines), failing task, retry history, environment metadata, correlated upstream failures in the same time window.

---

## `trace_lineage`

Traverse the OpenLineage graph from a dataset or pipeline.

**Input:**
```json
{
  "dataset_id": "string | null",
  "pipeline_id": "string | null",
  "direction": "upstream | downstream | both",
  "depth": 3
}
```

**Output:** Graph with nodes (datasets, pipelines) and edges. Each node includes last successful run timestamp.

---

## `diagnose_failure`

Root cause analysis combining deterministic pattern matchers and LLM reasoning.

**Input:** `{ "pipeline_id": "string", "run_id": "string | null" }`

**Output:**
```json
{
  "diagnosis_id": "string",
  "root_cause_category": "schema_drift | source_unavailable | oom | dependency_failure | code_error | data_quality | sla_violation | unknown",
  "confidence": 0.94,
  "evidence": ["crm_accounts dropped column 'account_type' at 2024-01-15T03:12Z"],
  "similar_incidents": [{"id": "INC-047", "resolution": "schema backfill", "resolved_at": "..."}],
  "recommended_action": "string"
}
```

---

## `propose_remediation`

Generate a structured remediation plan from a diagnosis.

**Input:** `{ "pipeline_id": "string", "diagnosis_id": "string" }`

**Output:** Ordered steps (each typed as `retry | restart | scale | code_change | manual`), `risk_level`, `estimated_resolution_time`, `rollback_plan`, `requires_human_approval`.

---

## `check_data_quality`

Run quality checks against a dataset.

**Input:** `{ "dataset_id": "string", "checks": ["schema_drift", "volume_anomaly", "freshness"] }`

**Output:** Per-check results: pass/fail, actual vs expected values, severity, sample failing rows (truncated to 5).

---

## `get_recent_incidents`

Query incident history.

**Input:** `{ "time_window": "24h", "severity": "critical | high | medium | low | null", "status": "open | resolved | null" }`

**Output:** Array of incidents with summary, status, assignee, resolution.

---

## `file_incident`

Create an incident record and optionally push to configured integrations.

**Input:**
```json
{
  "title": "string",
  "pipeline_id": "string",
  "severity": "critical | high | medium | low",
  "description": "string",
  "diagnosis_id": "string | null"
}
```

**Output:** Incident ID, ticket URL (if Jira/PagerDuty/GitHub configured), Slack notification status.

---

## `execute_remediation`

**Opt-in only.** Executes an approved remediation plan. Requires all of:

1. `auto_remediation_enabled: true` in pipeline config
2. `confirm: true` in arguments
3. A non-empty `approver_id`
4. Plan `risk_level` ≤ `AUTO_REMEDIATION_MAX_RISK` env var

**Input:** `{ "remediation_id": "string", "confirm": true, "approver_id": "string" }`

**Output:** Execution status, audit trail ID, actions taken.
