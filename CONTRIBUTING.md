# Contributing

## Development Setup

```bash
# Clone and install
git clone https://github.com/dataguard-agent/dataguard-agent
cd dataguard-agent
uv sync --frozen
uv run pre-commit install

# Run the full local stack
make demo
```

## Before Submitting

All of these must pass:

```bash
make fmt        # ruff format + fix
make lint       # ruff check
make typecheck  # mypy --strict + pyright
make test       # unit tests
```

Integration tests require a running Postgres and Redis (the Compose stack works):

```bash
make test-integration
```

## Adding an Orchestrator Adapter

1. Create `packages/adapters/src/dataguard_adapters/<name>.py`
2. Implement every abstract method in `OrchestratorAdapter` (see `base.py`)
3. Add integration tests under `packages/adapters/tests/integration/test_<name>.py` using `testcontainers` where possible
4. Register the adapter in `dataguard_adapters/__init__.py`
5. Add a section to `docs/adapters.md`

## Adding a Detector

1. Create `packages/sentinel/src/pipeline_sentinel/detectors/<name>.py`
2. Subclass `BaseDetector` and implement `run(dataset_id) -> DetectorResult`
3. Register in `DetectorRegistry`
4. Document expected config schema in `docs/detectors.md`

## Commit Style

```
<type>(<scope>): <subject>

feat(adapters): add Dagster adapter with run history support
fix(sentinel): handle missing lineage graph in diagnose_failure
docs(mcp-tools): add example input/output for trace_lineage
refactor(core): extract LLM retry logic into LLMClient
```

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

## Pull Requests

- One logical change per PR
- Reference an issue if one exists
- All CI checks must pass before review is requested
- New adapters and detectors require integration tests

## Questions

Open a GitHub Discussion or file an issue tagged `question`.
