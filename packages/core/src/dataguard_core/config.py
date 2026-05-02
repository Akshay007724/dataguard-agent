from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # State store
    database_url: str = Field(
        ...,
        description="Async SQLAlchemy DSN, e.g. postgresql+asyncpg://user:pass@host/db",
    )
    redis_url: str = Field("redis://localhost:6379/0")

    # LLM — routed through litellm; prefix determines provider
    # Examples: "anthropic/claude-opus-4-7", "openai/gpt-4o", "ollama/llama3"
    llm_model: str = Field("anthropic/claude-opus-4-7")
    anthropic_api_key: SecretStr | None = Field(None)
    openai_api_key: SecretStr | None = Field(None)
    azure_openai_api_key: SecretStr | None = Field(None)
    azure_openai_endpoint: str | None = Field(None)

    # Observability
    log_level: str = Field("INFO")
    log_format: str = Field("json", description="json | console")
    otel_exporter_otlp_endpoint: str | None = Field(None)

    # Remediation guards
    auto_remediation_enabled: bool = Field(False)
    auto_remediation_max_risk: str = Field("low", description="low | medium | high")

    # Cache TTL
    pipeline_status_cache_ttl: int = Field(60, description="Redis TTL in seconds")

    # Lineage
    openlineage_url: str = Field("http://localhost:5000")
    openlineage_namespace: str = Field("default")


settings: Settings = Settings()  # type: ignore[call-arg]
