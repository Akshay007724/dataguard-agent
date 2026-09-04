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
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/dataguard",
        description="Async SQLAlchemy DSN, e.g. postgresql+asyncpg://user:pass@host/db",
    )
    redis_url: str = Field(default="redis://localhost:6379/0")

    # LLM — routed through litellm; prefix determines provider
    # Examples: "anthropic/claude-opus-4-7", "openai/gpt-4o", "ollama/llama3"
    llm_model: str = Field(default="anthropic/claude-opus-4-7")
    llm_api_base: str | None = Field(default=None, description="Optional custom API base for Ollama/vLLM/LocalAI")
    anthropic_api_key: SecretStr | None = Field(default=None)
    openai_api_key: SecretStr | None = Field(default=None)
    azure_openai_api_key: SecretStr | None = Field(default=None)
    azure_openai_endpoint: str | None = Field(default=None)
    groq_api_key: SecretStr | None = Field(default=None)

    # Observability
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json", description="json | console")
    otel_exporter_otlp_endpoint: str | None = Field(default=None)

    # Remediation guards
    auto_remediation_enabled: bool = Field(default=False)
    auto_remediation_max_risk: str = Field(default="low", description="low | medium | high")

    # Cache TTL
    pipeline_status_cache_ttl: int = Field(default=60, description="Redis TTL in seconds")

    # Lineage
    openlineage_url: str = Field(default="http://localhost:5000")
    openlineage_namespace: str = Field(default="default")


settings: Settings = Settings()
