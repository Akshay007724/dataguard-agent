from dataguard_core.config import Settings as CoreSettings


class SentinelSettings(CoreSettings):
    """Sentinel-specific settings, extending core."""

    # Airflow connection
    airflow_base_url: str = "http://localhost:8080"
    airflow_username: str = "admin"
    airflow_password: str = "admin"

    # Argo connection
    argo_host: str = "http://localhost:2746"
    argo_namespace: str = "argo"
    argo_token: str | None = None
    argo_verify_ssl: bool = True

    # Server
    server_host: str = "0.0.0.0"
    server_port: int = 8080
    metrics_port: int = 9090

    # Diagnosis
    diagnosis_deterministic_confidence_threshold: float = 0.85
    diagnosis_max_log_head_lines: int = 50
    diagnosis_max_log_tail_lines: int = 100

    # Incident ID prefix
    incident_id_prefix: str = "INC"


settings: SentinelSettings = SentinelSettings()  # type: ignore[call-arg]
