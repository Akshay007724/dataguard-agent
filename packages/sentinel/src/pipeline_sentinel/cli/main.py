from __future__ import annotations

import asyncio
import sys

import typer
import uvicorn

from dataguard_core.logging import configure_logging
from dataguard_core.tracing import configure_tracing
from pipeline_sentinel.config import settings

app = typer.Typer(
    name="pipeline-sentinel",
    help="Pipeline Sentinel — MCP-native agentic triage for data pipeline failures.",
    no_args_is_help=True,
)


@app.command()
def mcp() -> None:
    """Start the MCP server over stdio (for Claude Desktop, Cursor, mcp CLI)."""
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    configure_tracing(otlp_endpoint=settings.otel_exporter_otlp_endpoint)

    from pipeline_sentinel.mcp_server.server import run_stdio_server

    asyncio.run(run_stdio_server())


@app.command()
def serve(
    host: str = typer.Option(None, help="Override server host"),
    port: int = typer.Option(None, help="Override server port"),
) -> None:
    """Start the HTTP server (FastAPI wrapper over MCP tools)."""
    configure_logging(level=settings.log_level, fmt=settings.log_format)
    configure_tracing(otlp_endpoint=settings.otel_exporter_otlp_endpoint)

    from pipeline_sentinel.api.app import create_app

    application = create_app()
    uvicorn.run(
        application,
        host=host or settings.server_host,
        port=port or settings.server_port,
        log_config=None,  # structlog handles logging
    )


@app.command()
def diagnose(
    pipeline_id: str = typer.Argument(..., help="Pipeline ID to diagnose"),
    run_id: str = typer.Option(None, help="Specific run ID. Defaults to latest failure."),
) -> None:
    """Diagnose a pipeline failure and print the result to stdout."""
    configure_logging(level=settings.log_level, fmt="console")

    async def _run() -> None:
        from dataguard_core.store import postgres, redis
        from pipeline_sentinel.mcp_server.server import _build_dependencies
        from pipeline_sentinel.mcp_server.tools.diagnosis import handle_diagnose_failure

        postgres.init_engine(settings.database_url)
        redis.init_redis(settings.redis_url)
        await postgres.create_tables()

        adapters, tracer, detectors, llm = _build_dependencies()
        result = await handle_diagnose_failure(
            adapters, tracer, detectors, llm,
            {"pipeline_id": pipeline_id, "run_id": run_id},
        )
        typer.echo(result)

    asyncio.run(_run())


@app.command()
def version() -> None:
    """Print version and exit."""
    from importlib.metadata import version as pkg_version

    typer.echo(f"pipeline-sentinel {pkg_version('pipeline-sentinel')}")


if __name__ == "__main__":
    app()
