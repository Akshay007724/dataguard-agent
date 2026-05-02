from dataguard_adapters.base import OrchestratorAdapter, PipelineSummary, RunDetails, RunStatus


class PrefectAdapter(OrchestratorAdapter):
    """Prefect adapter — stub. Full implementation planned for v0.2."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise NotImplementedError(
            "PrefectAdapter is not yet implemented. Track progress at "
            "https://github.com/dataguard-agent/dataguard-agent/issues"
        )

    @property
    def orchestrator_name(self) -> str:
        return "prefect"

    async def list_pipelines(self, tag: str | None = None, status: RunStatus | None = None) -> list[PipelineSummary]:
        raise NotImplementedError

    async def get_pipeline(self, pipeline_id: str) -> PipelineSummary:
        raise NotImplementedError

    async def get_run(self, pipeline_id: str, run_id: str) -> RunDetails:
        raise NotImplementedError

    async def get_latest_run(self, pipeline_id: str) -> RunDetails:
        raise NotImplementedError

    async def get_run_history(self, pipeline_id: str, limit: int = 10) -> list[RunDetails]:
        raise NotImplementedError

    async def get_run_logs(self, pipeline_id: str, run_id: str, task_id: str | None = None, head_lines: int = 50, tail_lines: int = 100) -> str:
        raise NotImplementedError
