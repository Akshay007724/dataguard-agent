# syntax=docker/dockerfile:1.7
ARG UV_VERSION=0.4
ARG PYTHON_VERSION=3.12

# ── Stage 1: resolve and install dependencies ────────────────────────────────
FROM ghcr.io/astral-sh/uv:${UV_VERSION}-python${PYTHON_VERSION}-bookworm-slim AS deps

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/ packages/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package pipeline-sentinel

# ── Stage 2: minimal runtime image ──────────────────────────────────────────
FROM python:${PYTHON_VERSION}-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/dataguard-agent/dataguard-agent"
LABEL org.opencontainers.image.description="Pipeline Sentinel — MCP-native agentic data pipeline triage"
LABEL org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app

# Dedicated non-root user
RUN groupadd --gid 65532 sentinel \
 && useradd --uid 65532 --gid 65532 --shell /sbin/nologin --no-create-home sentinel

COPY --from=deps --chown=sentinel:sentinel /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER sentinel

# 8080 = MCP/HTTP server, 9090 = Prometheus metrics
EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" \
        || exit 1

ENTRYPOINT ["pipeline-sentinel"]
CMD ["serve"]
