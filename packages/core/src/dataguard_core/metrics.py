from prometheus_client import Counter, Histogram

mcp_tool_duration = Histogram(
    "sentinel_mcp_tool_duration_seconds",
    "MCP tool call latency",
    ["tool"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

mcp_tool_errors = Counter(
    "sentinel_mcp_tool_errors_total",
    "MCP tool call errors",
    ["tool", "error_type"],
)

llm_tokens = Counter(
    "sentinel_llm_tokens_total",
    "LLM token usage",
    ["provider", "model", "direction"],  # direction: input | output
)

llm_deterministic_hits = Counter(
    "sentinel_llm_deterministic_hits_total",
    "Failures resolved by deterministic matcher — LLM call skipped",
    ["pattern"],
)

adapter_request_duration = Histogram(
    "sentinel_adapter_request_duration_seconds",
    "Orchestrator adapter request latency",
    ["adapter", "operation"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
)

detector_duration = Histogram(
    "sentinel_detector_duration_seconds",
    "Quality detector run latency",
    ["detector"],
)
