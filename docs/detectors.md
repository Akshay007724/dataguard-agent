# Quality Detectors

DataGuard Agent includes modular detectors to verify dataset integrity, lineage freshness, and schema evolution during pipeline diagnosis and data quality evaluations.

---

## Core Contract

All detectors inherit from `BaseDetector` in `packages/sentinel/src/pipeline_sentinel/detectors/base.py`:

```python
class BaseDetector(ABC):
    @abstractmethod
    async def check(self, dataset_id: str) -> DetectorResult:
        """Evaluate quality rules for dataset and return structured check results."""
        ...
```

Each check returns a `DetectorResult` containing:
- `passed`: `bool` indicating whether the dataset passed all assertions
- `check_type`: `schema_drift | volume_anomaly | freshness | custom_sql`
- `severity`: `critical | high | medium | low | info`
- `message`: Diagnostic message explaining what was evaluated
- `details`: Structured payload containing expected vs. actual metrics

---

## Built-In Detectors

### 1. Schema Drift Detector (`SchemaDriftDetector`)
Monitors OpenLineage dataset facets to identify structural schema changes:
- **Column Dropped**: Flagged as `CRITICAL` severity (prevents downstream query breaks)
- **Data Type Changed**: Flagged as `HIGH` severity (e.g. `VARCHAR` to `INT`)
- **Column Added**: Flagged as `MEDIUM` severity (informative/non-breaking schema evolution)

```python
detector = SchemaDriftDetector(marquez_client=marquez)
result = await detector.check("postgres://warehouse/public.customer_orders")
```

### 2. Volume Anomaly Detector (`VolumeAnomalyDetector`)
Analyzes row count variations against historical ingestion trends using standard score deviation:
- Calculates $\mu$ (mean) and $\sigma$ (standard deviation) over a configurable sliding window.
- **Severity Tiers**:
  - $|z| \ge 6$: `CRITICAL`
  - $4 \le |z| < 6$: `HIGH`
  - $3 \le |z| < 4$: `MEDIUM`
  - $|z| < 3$: `LOW` / Pass

### 3. Freshness Detector (`FreshnessDetector`)
Compares dataset modification timestamps against business SLA thresholds:
- Interrogates OpenLineage job execution history.
- Flags datasets whose last output run exceeds maximum acceptable staleness.
- Emits `CRITICAL` warnings for pipelines with downstream dependencies waiting on stale inputs.

### 4. Custom SQL Detector (`CustomSQLDetector`)
Executes user-defined SQL assertions directly against data warehouses (PostgreSQL, Snowflake, BigQuery):
- Evaluates invariant constraints:
  - Null checks on primary keys: `SELECT COUNT(*) FROM table WHERE id IS NULL`
  - Range and outlier boundaries: `SELECT COUNT(*) FROM transactions WHERE amount < 0`
  - Referential integrity validation.

---

## Using Detectors via MCP

Quality checks are exposed to agentic LLMs via the `check_data_quality` MCP tool:

```json
{
  "dataset_id": "postgres://dw/analytics.monthly_revenue",
  "checks": ["schema_drift", "volume_anomaly", "freshness"]
}
```

The MCP server dispatches checks concurrently using `asyncio.gather` and compiles a unified report with remediation hints.
