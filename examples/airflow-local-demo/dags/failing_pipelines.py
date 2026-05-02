"""Demo DAGs with intentional failures for Pipeline Sentinel end-to-end testing.

Three failure modes:
  1. customer_ltv_daily    — schema drift (KeyError on dropped column)
  2. inventory_sync_hourly — source unavailability (connection refused to mock API)
  3. product_features_weekly — SLA violation (artificially slow run)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ── DAG 1: Schema drift ──────────────────────────────────────────────────────

def _simulate_schema_drift(**context: object) -> None:
    """Simulates a downstream pipeline reading a column that was dropped upstream."""
    # Upstream crm_accounts table dropped 'account_type' in a recent migration.
    # This pipeline still references it, causing a KeyError.
    row = {"customer_id": 1001, "revenue": 5000.0, "segment": "enterprise"}
    # Missing 'account_type' — this is the intentional failure
    ltv_score = row["revenue"] * ({"premium": 2.0, "enterprise": 1.8}[row["account_type"]])  # type: ignore[index]
    print(f"LTV score: {ltv_score}")


with DAG(
    dag_id="customer_ltv_daily",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["ltv", "customer", "demo-failing"],
) as customer_ltv_dag:
    PythonOperator(
        task_id="compute_ltv_scores",
        python_callable=_simulate_schema_drift,
    )


# ── DAG 2: Source unavailability ─────────────────────────────────────────────

def _simulate_source_unavailable(**context: object) -> None:
    """Simulates an external API returning connection refused."""
    import socket

    # Intentionally connects to a port that is not listening
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    try:
        sock.connect(("localhost", 19999))  # nothing listening here
    except (ConnectionRefusedError, OSError) as exc:
        raise RuntimeError(
            f"connection refused to inventory-api:19999: {exc}. "
            "Source system may be down."
        ) from exc
    finally:
        sock.close()


with DAG(
    dag_id="inventory_sync_hourly",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["inventory", "sync", "demo-failing"],
) as inventory_sync_dag:
    PythonOperator(
        task_id="fetch_inventory_api",
        python_callable=_simulate_source_unavailable,
    )


# ── DAG 3: SLA violation ─────────────────────────────────────────────────────

def _simulate_slow_run(**context: object) -> None:
    """Simulates a pipeline that runs but breaches its SLA."""
    import time

    print("Starting feature computation — this will exceed SLA...")
    # In a real scenario this would be a slow query or data skew
    time.sleep(45)  # Runs 45s; DAG SLA is 30s
    print("Feature computation complete (but too slow)")


with DAG(
    dag_id="product_features_weekly",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    sla_miss_callback=lambda dag, task_list, blocking_task_list, slas, blocking_tis: None,
    tags=["features", "ml", "demo-failing"],
) as product_features_dag:
    PythonOperator(
        task_id="compute_product_features",
        python_callable=_simulate_slow_run,
        execution_timeout=timedelta(seconds=30),  # will raise AirflowTaskTimeout
    )


# ── DAG 4: Healthy reference pipeline ───────────────────────────────────────

def _healthy_run(**context: object) -> None:
    print("Pipeline completed successfully.")


with DAG(
    dag_id="crm_accounts_export",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["crm", "export", "demo-healthy"],
) as crm_accounts_dag:
    PythonOperator(
        task_id="export_accounts",
        python_callable=_healthy_run,
    )
