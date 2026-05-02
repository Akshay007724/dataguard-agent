from __future__ import annotations

from typing import Any, TypeVar

import litellm
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from dataguard_core.llm.base import LLMResponse, LLMUsage
from dataguard_core.logging import get_logger
from dataguard_core.metrics import llm_tokens

T = TypeVar("T", bound=BaseModel)

log = get_logger(__name__)
litellm.set_verbose = False  # type: ignore[attr-defined]


class LLMClient:
    """Async LLM client backed by litellm.

    Args:
        model: litellm model string, e.g. "anthropic/claude-opus-4-7" or "openai/gpt-4o".
        api_key: Provider API key. If None, litellm reads from environment variables.
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, model: str, api_key: str | None = None, timeout: int = 120) -> None:
        self._model = model
        self._api_key = api_key
        self._timeout = timeout
        self._provider = model.split("/")[0] if "/" in model else "unknown"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        reraise=True,
    )
    async def complete(
        self,
        prompt: str,
        system: str | None = None,
        temperature: float = 0.0,
    ) -> LLMResponse:
        """Free-form completion. Returns raw text response."""
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "temperature": temperature,
            "timeout": self._timeout,
        }
        if self._api_key:
            kwargs["api_key"] = self._api_key

        response = await litellm.acompletion(**kwargs)
        return self._build_response(response)

    async def complete_structured(
        self,
        prompt: str,
        schema: type[T],
        system: str | None = None,
    ) -> T:
        """Structured completion. Response is validated against a Pydantic schema.

        Passes json_schema response_format to providers that support it.
        """
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": messages,
            "temperature": 0.0,
            "timeout": self._timeout,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": schema.__name__,
                    "strict": True,
                    "schema": schema.model_json_schema(),
                },
            },
        }
        if self._api_key:
            kwargs["api_key"] = self._api_key

        response = await litellm.acompletion(**kwargs)
        llm_response = self._build_response(response)
        return schema.model_validate_json(llm_response.content)

    def _build_response(self, response: Any) -> LLMResponse:
        content: str = response.choices[0].message.content or ""
        usage = response.usage
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0

        llm_tokens.labels(provider=self._provider, model=self._model, direction="input").inc(input_tokens)
        llm_tokens.labels(provider=self._provider, model=self._model, direction="output").inc(output_tokens)

        log.debug("llm_completion", model=self._model, in_tok=input_tokens, out_tok=output_tokens)

        return LLMResponse(
            content=content,
            usage=LLMUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                model=self._model,
                provider=self._provider,
            ),
            raw=response.model_dump(),
        )
