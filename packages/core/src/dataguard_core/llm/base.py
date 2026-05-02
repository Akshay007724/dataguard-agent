from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LLMUsage:
    input_tokens: int
    output_tokens: int
    model: str
    provider: str


@dataclass(frozen=True)
class LLMResponse:
    content: str
    usage: LLMUsage
    raw: dict[str, Any]
