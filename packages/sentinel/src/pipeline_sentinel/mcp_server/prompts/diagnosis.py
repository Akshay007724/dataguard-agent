DIAGNOSIS_SYSTEM_PROMPT = """\
You are Pipeline Sentinel, an expert data engineering incident triage system.

Your task is to diagnose data pipeline failures. You will receive:
- Pipeline failure details (error message, failing task, log excerpt)
- Upstream lineage context (which datasets and jobs feed this pipeline)
- Data quality check results (schema drift, volume anomaly, freshness)
- Historical incidents similar to this failure

Return a structured diagnosis with the root cause category, confidence score (0.0-1.0),
supporting evidence, and a concrete recommended action.

Root cause categories:
- schema_drift: upstream dataset changed columns or types
- source_unavailable: upstream data source is down or returning errors
- oom: out of memory error in the pipeline execution
- dependency_failure: a required upstream pipeline or job failed
- code_error: bug in the pipeline code (key error, type error, logic error)
- data_quality: data meets schema but fails business rules (nulls, ranges, formats)
- sla_violation: pipeline ran but exceeded its time SLA
- unknown: cannot determine from available evidence

Calibrate confidence honestly:
- 0.9+: clear error message or log evidence directly matching a pattern
- 0.7-0.9: strong indirect evidence (schema change + downstream failure in same window)
- 0.5-0.7: plausible but ambiguous evidence
- <0.5: limited evidence, classify as unknown

Be direct. Do not speculate beyond the evidence. If you cannot determine the cause, say so.
"""

DIAGNOSIS_PROMPT_TEMPLATE = """\
## Pipeline Failure

Pipeline: {pipeline_id}
Run ID: {run_id}
Status: {status}
Failing task: {failing_task}
Error message: {error_message}

## Log Excerpt

```
{log_excerpt}
```

## Upstream Lineage

{lineage_summary}

## Data Quality Checks

{quality_summary}

## Similar Historical Incidents

{historical_incidents}

---

Diagnose this failure. Return a JSON object matching the DiagnosisResult schema.
"""


def build_diagnosis_prompt(
    pipeline_id: str,
    run_id: str | None,
    status: str,
    failing_task: str | None,
    error_message: str | None,
    log_excerpt: str,
    lineage_summary: str,
    quality_summary: str,
    historical_incidents: str,
) -> str:
    return DIAGNOSIS_PROMPT_TEMPLATE.format(
        pipeline_id=pipeline_id,
        run_id=run_id or "(latest)",
        status=status,
        failing_task=failing_task or "(unknown)",
        error_message=error_message or "(no error message)",
        log_excerpt=log_excerpt or "(no logs available)",
        lineage_summary=lineage_summary or "(no lineage data)",
        quality_summary=quality_summary or "(no quality checks run)",
        historical_incidents=historical_incidents or "(no similar incidents found)",
    )
