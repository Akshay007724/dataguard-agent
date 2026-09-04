from __future__ import annotations

from dataguard_core.logging import _add_opentelemetry_context, configure_logging, get_logger
from dataguard_core.tracing import configure_tracing, get_tracer


def test_configure_logging_json() -> None:
    configure_logging(level="DEBUG", fmt="json")
    log = get_logger("test_json")
    assert log is not None


def test_configure_logging_console() -> None:
    configure_logging(level="INFO", fmt="console")
    log = get_logger("test_console")
    assert log is not None


def test_opentelemetry_context_injector() -> None:
    event_dict = {"event": "test"}
    res = _add_opentelemetry_context(None, "info", event_dict)
    assert "event" in res


def test_configure_tracing() -> None:
    configure_tracing(service_name="test-service")
    tracer = get_tracer("test-tracer")
    assert tracer is not None
