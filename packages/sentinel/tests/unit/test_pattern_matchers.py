from __future__ import annotations

import pytest

from pipeline_sentinel.mcp_server.tools.diagnosis import _run_pattern_matchers


@pytest.mark.parametrize(
    "log_text,expected_cause,expected_pattern",
    [
        ("Process OOMKilled: memory limit exceeded", "oom", "oom_killed"),
        ("out of memory — increase heap", "oom", "oom_killed"),
        ("Killed    python worker.py", "oom", "oom_killed"),
        ("connection timed out after 30s", "source_unavailable", "connection_timeout"),
        ("ECONNREFUSED 127.0.0.1:5432", "source_unavailable", "connection_timeout"),
        ("could not connect to server: Connection refused", "source_unavailable", "connection_timeout"),
        ("socket timeout waiting for response", "source_unavailable", "connection_timeout"),
        ("503 Service Unavailable from upstream", "source_unavailable", "http_503"),
        ("upstream connect error 503 error detected", "source_unavailable", "http_503"),
        ("KeyError: 'account_type'", "code_error", "key_error"),
        ('KeyError: "missing_field"', "code_error", "key_error"),
        ('column "revenue" does not exist', "schema_drift", "column_not_found"),
        ("column user_id not found in table", "schema_drift", "column_not_found"),
        ("schema mismatch: expected INT got STRING", "schema_drift", "schema_mismatch"),
        ("incompatible schema on merge", "schema_drift", "schema_mismatch"),
        ("upstream task customer_ltv_daily failed", "dependency_failure", "upstream_failed"),
        ("Task ingest_raw in state upstream_failed", "dependency_failure", "upstream_failed"),
    ],
)
def test_pattern_hit(log_text: str, expected_cause: str, expected_pattern: str) -> None:
    result = _run_pattern_matchers(log_text)
    assert result is not None, f"Expected pattern match for: {log_text!r}"
    root_cause, confidence, pattern_name = result
    assert root_cause == expected_cause
    assert pattern_name == expected_pattern
    assert 0.0 < confidence <= 1.0


def test_pattern_miss_on_clean_log() -> None:
    assert _run_pattern_matchers("Pipeline completed. 1000 rows written to warehouse.") is None


def test_pattern_miss_on_empty_string() -> None:
    assert _run_pattern_matchers("") is None


def test_pattern_case_insensitive_oom() -> None:
    result = _run_pattern_matchers("OUT OF MEMORY: process killed")
    assert result is not None
    assert result[0] == "oom"


def test_pattern_case_insensitive_connection() -> None:
    result = _run_pattern_matchers("CONNECTION TIMED OUT")
    assert result is not None
    assert result[0] == "source_unavailable"


def test_first_match_wins_oom_before_connection() -> None:
    # OOM pattern is listed first — should win when both could match
    log = "out of memory; connection timed out"
    result = _run_pattern_matchers(log)
    assert result is not None
    assert result[2] == "oom_killed"
