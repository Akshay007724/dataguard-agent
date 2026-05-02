from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataguard_core.llm.client import LLMClient


def _mock_litellm_response(content: str, prompt_tokens: int = 10, completion_tokens: int = 5) -> MagicMock:
    usage = MagicMock()
    usage.prompt_tokens = prompt_tokens
    usage.completion_tokens = completion_tokens

    choice = MagicMock()
    choice.message.content = content

    response = MagicMock()
    response.choices = [choice]
    response.usage = usage
    response.model_dump.return_value = {"choices": []}
    return response


class TestLLMClientInit:
    def test_provider_parsed_from_slash_model(self) -> None:
        client = LLMClient(model="anthropic/claude-opus-4-7")
        assert client._provider == "anthropic"

    def test_provider_openai(self) -> None:
        assert LLMClient(model="openai/gpt-4o")._provider == "openai"

    def test_provider_unknown_without_slash(self) -> None:
        assert LLMClient(model="gpt-4o")._provider == "unknown"

    def test_model_stored(self) -> None:
        client = LLMClient(model="openai/gpt-4o", timeout=60)
        assert client._model == "openai/gpt-4o"
        assert client._timeout == 60


class TestLLMClientComplete:
    @pytest.mark.asyncio
    async def test_complete_returns_content(self) -> None:
        client = LLMClient(model="openai/gpt-4o")
        mock_resp = _mock_litellm_response("Hello world")
        with patch("dataguard_core.llm.client.litellm.acompletion", new=AsyncMock(return_value=mock_resp)):
            result = await client.complete("Say hello")
        assert result.content == "Hello world"

    @pytest.mark.asyncio
    async def test_complete_records_token_usage(self) -> None:
        client = LLMClient(model="openai/gpt-4o")
        mock_resp = _mock_litellm_response("ok", prompt_tokens=20, completion_tokens=8)
        with patch("dataguard_core.llm.client.litellm.acompletion", new=AsyncMock(return_value=mock_resp)):
            result = await client.complete("prompt")
        assert result.usage.input_tokens == 20
        assert result.usage.output_tokens == 8

    @pytest.mark.asyncio
    async def test_complete_sends_system_message(self) -> None:
        client = LLMClient(model="openai/gpt-4o")
        mock_resp = _mock_litellm_response("ok")
        with patch(
            "dataguard_core.llm.client.litellm.acompletion", new=AsyncMock(return_value=mock_resp)
        ) as mock_call:
            await client.complete("prompt", system="Be concise")
        messages = mock_call.call_args.kwargs["messages"]
        roles = [m["role"] for m in messages]
        assert roles == ["system", "user"]
        assert messages[0]["content"] == "Be concise"

    @pytest.mark.asyncio
    async def test_complete_without_system_sends_user_only(self) -> None:
        client = LLMClient(model="openai/gpt-4o")
        mock_resp = _mock_litellm_response("ok")
        with patch(
            "dataguard_core.llm.client.litellm.acompletion", new=AsyncMock(return_value=mock_resp)
        ) as mock_call:
            await client.complete("user prompt")
        messages = mock_call.call_args.kwargs["messages"]
        assert len(messages) == 1
        assert messages[0]["role"] == "user"

    @pytest.mark.asyncio
    async def test_complete_passes_api_key_when_set(self) -> None:
        client = LLMClient(model="openai/gpt-4o", api_key="sk-test")
        mock_resp = _mock_litellm_response("ok")
        with patch(
            "dataguard_core.llm.client.litellm.acompletion", new=AsyncMock(return_value=mock_resp)
        ) as mock_call:
            await client.complete("prompt")
        assert mock_call.call_args.kwargs.get("api_key") == "sk-test"

    @pytest.mark.asyncio
    async def test_complete_omits_api_key_when_none(self) -> None:
        client = LLMClient(model="openai/gpt-4o", api_key=None)
        mock_resp = _mock_litellm_response("ok")
        with patch(
            "dataguard_core.llm.client.litellm.acompletion", new=AsyncMock(return_value=mock_resp)
        ) as mock_call:
            await client.complete("prompt")
        assert "api_key" not in mock_call.call_args.kwargs
