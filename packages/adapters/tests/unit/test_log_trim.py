from __future__ import annotations

import pytest

from dataguard_adapters.base import OrchestratorAdapter


def _lines(n: int) -> str:
    return "\n".join(f"line {i}" for i in range(n))


class TestTrimLog:
    def test_short_log_returned_unchanged(self) -> None:
        raw = _lines(10)
        assert OrchestratorAdapter._trim_log(raw, head=50, tail=100) == raw

    def test_exact_boundary_not_trimmed(self) -> None:
        raw = _lines(150)  # head=50 + tail=100 == 150
        assert OrchestratorAdapter._trim_log(raw, head=50, tail=100) == raw

    def test_one_over_boundary_is_trimmed(self) -> None:
        raw = _lines(151)
        result = OrchestratorAdapter._trim_log(raw, head=50, tail=100)
        assert "omitted" in result

    def test_head_lines_preserved(self) -> None:
        raw = _lines(200)
        result = OrchestratorAdapter._trim_log(raw, head=50, tail=100)
        result_lines = result.splitlines()
        assert result_lines[0] == "line 0"
        assert result_lines[49] == "line 49"

    def test_tail_lines_preserved(self) -> None:
        raw = _lines(200)
        result = OrchestratorAdapter._trim_log(raw, head=50, tail=100)
        result_lines = result.splitlines()
        assert result_lines[-1] == "line 199"
        assert result_lines[-100] == "line 100"

    def test_omission_marker_accurate_count(self) -> None:
        raw = _lines(200)
        result = OrchestratorAdapter._trim_log(raw, head=50, tail=100)
        omission_line = result.splitlines()[50]
        assert "50" in omission_line  # 200 - 50 - 100 = 50 omitted

    def test_large_log_omission_count(self) -> None:
        raw = _lines(300)
        result = OrchestratorAdapter._trim_log(raw, head=10, tail=10)
        omission_line = result.splitlines()[10]
        assert "280" in omission_line  # 300 - 10 - 10 = 280

    def test_empty_log(self) -> None:
        assert OrchestratorAdapter._trim_log("", head=50, tail=100) == ""

    def test_single_line_not_trimmed(self) -> None:
        assert OrchestratorAdapter._trim_log("one line", head=1, tail=1) == "one line"
