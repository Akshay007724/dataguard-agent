from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataguard_agents.triage import TriageAgent
from dataguard_agents.base import AgentContext


def _make_ctx() -> MagicMock:
    ctx = MagicMock(spec=AgentContext)
    ctx.adapters = []
    ctx.tracer = MagicMock()
    ctx.detectors = []
    ctx.llm = MagicMock()
    ctx.llm._model = "claude-sonnet-4-6"
    return ctx


def _make_litellm_response(
    content: str | None = None,
    tool_calls: list[Any] | None = None,
    finish_reason: str = "stop",
) -> MagicMock:
    choice = MagicMock()
    choice.finish_reason = finish_reason
    choice.message.content = content
    choice.message.tool_calls = tool_calls or []
    choice.message.model_dump.return_value = {
        "role": "assistant",
        "content": content,
        **({"tool_calls": [tc.__dict__ for tc in tool_calls]} if tool_calls else {}),
    }
    response = MagicMock()
    response.choices = [choice]
    return response


def _make_tool_call(name: str, args: dict[str, Any], call_id: str = "tc-1") -> MagicMock:
    tc = MagicMock()
    tc.id = call_id
    tc.function.name = name
    tc.function.arguments = json.dumps(args)
    return tc


_VALID_REPORT = {
    "triage_completed_at": "2024-06-01T10:05:00+00:00",
    "pipelines_checked": 1,
    "failures_found": 1,
    "incidents_filed": ["INC-1"],
    "diagnoses": [
        {
            "pipeline_id": "pipe-a",
            "root_cause": "oom",
            "confidence": 0.95,
            "severity": "critical",
            "remediation_id": "REM-1",
            "incident_id": "INC-1",
        }
    ],
    "summary": "OOM in pipe-a.",
}


class TestTriageAgentRun:
    @pytest.mark.asyncio
    async def test_single_stop_turn_returns_report(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx)

        stop_response = _make_litellm_response(content=json.dumps(_VALID_REPORT), finish_reason="stop")

        with patch("dataguard_agents.triage.litellm.acompletion", new=AsyncMock(return_value=stop_response)):
            report = await agent.run()

        assert report.pipelines_checked == 1
        assert report.incidents_filed == ["INC-1"]

    @pytest.mark.asyncio
    async def test_tool_call_dispatched_then_stop(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx)

        tc = _make_tool_call("list_pipelines", {"status": "failed"})
        tool_response = _make_litellm_response(tool_calls=[tc], finish_reason="tool_calls")
        stop_response = _make_litellm_response(content=json.dumps(_VALID_REPORT), finish_reason="stop")

        mock_dispatch = AsyncMock(return_value=json.dumps({"pipelines": []}))

        with (
            patch("dataguard_agents.triage.litellm.acompletion", new=AsyncMock(side_effect=[tool_response, stop_response])),
            patch("dataguard_agents.base._dispatch", new=mock_dispatch),
        ):
            report = await agent.run()

        mock_dispatch.assert_awaited_once()
        call_kwargs = mock_dispatch.call_args
        assert call_kwargs[1]["name"] == "list_pipelines" or call_kwargs[0][0] == "list_pipelines" or "list_pipelines" in str(call_kwargs)

    @pytest.mark.asyncio
    async def test_scope_sets_single_pipeline_prompt(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx)

        stop_response = _make_litellm_response(content=json.dumps(_VALID_REPORT), finish_reason="stop")
        captured_messages: list[Any] = []

        async def fake_completion(**kwargs: Any) -> Any:
            captured_messages.extend(kwargs["messages"])
            return stop_response

        with patch("dataguard_agents.triage.litellm.acompletion", new=fake_completion):
            await agent.run(scope="pipe-a")

        user_msg = next(m for m in captured_messages if m.get("role") == "user")
        assert "pipe-a" in user_msg["content"]

    @pytest.mark.asyncio
    async def test_max_turns_returns_partial_report(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx, max_turns=2)

        tc = _make_tool_call("list_pipelines", {})
        tool_response = _make_litellm_response(tool_calls=[tc], finish_reason="tool_calls")

        mock_dispatch = AsyncMock(return_value=json.dumps({}))

        with (
            patch("dataguard_agents.triage.litellm.acompletion", new=AsyncMock(return_value=tool_response)),
            patch("dataguard_agents.base._dispatch", new=mock_dispatch),
        ):
            report = await agent.run()

        # Should return a report (not raise) even after hitting max_turns
        assert report is not None

    @pytest.mark.asyncio
    async def test_tool_error_recorded_as_json_error(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx)

        tc = _make_tool_call("diagnose_failure", {"pipeline_id": "pipe-x"})
        tool_response = _make_litellm_response(tool_calls=[tc], finish_reason="tool_calls")
        stop_response = _make_litellm_response(content=json.dumps(_VALID_REPORT), finish_reason="stop")

        captured_tool_results: list[str] = []

        async def fake_completion(**kwargs: Any) -> Any:
            for msg in kwargs["messages"]:
                if msg.get("role") == "tool":
                    captured_tool_results.append(msg["content"])
            return stop_response if captured_tool_results else tool_response

        with (
            patch("dataguard_agents.triage.litellm.acompletion", new=fake_completion),
            patch("dataguard_agents.base._dispatch", new=AsyncMock(side_effect=RuntimeError("DB down"))),
        ):
            report = await agent.run()

        assert any("error" in r for r in captured_tool_results)
        assert any("DB down" in r for r in captured_tool_results)

    @pytest.mark.asyncio
    async def test_end_turn_finish_reason_treated_as_stop(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx)

        stop_response = _make_litellm_response(content=json.dumps(_VALID_REPORT), finish_reason="end_turn")

        with patch("dataguard_agents.triage.litellm.acompletion", new=AsyncMock(return_value=stop_response)):
            report = await agent.run()

        assert report.summary == _VALID_REPORT["summary"]

    @pytest.mark.asyncio
    async def test_report_timestamps_set(self) -> None:
        ctx = _make_ctx()
        agent = TriageAgent(ctx)

        stop_response = _make_litellm_response(content=json.dumps(_VALID_REPORT), finish_reason="stop")

        before = datetime.now(timezone.utc)
        with patch("dataguard_agents.triage.litellm.acompletion", new=AsyncMock(return_value=stop_response)):
            report = await agent.run()
        after = datetime.now(timezone.utc)

        assert before <= report.triage_started_at <= after
        assert before <= report.triage_completed_at <= after
        assert report.duration_seconds >= 0
