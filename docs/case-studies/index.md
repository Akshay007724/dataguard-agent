# Case Studies

Real failure scenarios and how Pipeline Sentinel handles them end-to-end.

## Planned

- **Schema drift cascade** — upstream table drops a column; three downstream pipelines fail; agent traces lineage, identifies root cause, and proposes a targeted schema backfill
- **Source API degradation** — external vendor API returns intermittent 503s; agent distinguishes transient from persistent failure and recommends retry vs. incident escalation
- **Silent volume drop** — row count drops 40% with no error; agent detects via volume anomaly detector and correlates with an upstream DAG that silently skipped a partition
