# Orchestrator Adapters

DataGuard Agent communicates with data orchestrators through pluggable adapters inheriting from `OrchestratorAdapter`. Each adapter normalizes pipeline metadata, execution states, and logs into a uniform data model.

---

## Architecture & Base Contract

All adapters inherit from `OrchestratorAdapter` defined in `packages/adapters/src/dataguard_adapters/base.py`:

```python
class OrchestratorAdapter(ABC):
    @abstractmethod
    async def list_pipelines(self) -> list[PipelineSummary]: ...

    @abstractmethod
    async def get_pipeline_status(self, pipeline_id: str) -> PipelineStatus: ...

    @abstractmethod
    async def get_failure_details(self, pipeline_id: str, run_id: str | None = None) -> FailureDetails: ...

    @abstractmethod
    async def get_run_logs(self, pipeline_id: str, run_id: str, max_lines: int = 500) -> str: ...

    @abstractmethod
    async def aclose(self) -> None: ...
```

Adapters support asynchronous context management:

```python
async with AirflowAdapter(base_url="http://airflow:8080", username="admin", password="password") as adapter:
    pipelines = await adapter.list_pipelines()
```

---

## Supported Orchestrators

### Apache Airflow (`AirflowAdapter`)
Interacts with the Apache Airflow 2.x stable REST API.

- **Endpoints Used**:
  - `GET /api/v1/dags` — Dag retrieval with pagination
  - `GET /api/v1/dags/{dag_id}/dagRuns` — Execution history and state
  - `GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances` — Task failures
  - `GET /api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}` — Execution logs
- **Authentication**: HTTP Basic Auth or Bearer token
- **Log Compression**: Automates head/tail truncation for high-volume logs to minimize token consumption while preserving traceback context.

### Argo Workflows (`ArgoAdapter`)
Interacts with the Argo Workflows REST API on Kubernetes.

- **Endpoints Used**:
  - `GET /api/v1/workflows/{namespace}` — Workflow enumeration
  - `GET /api/v1/workflows/{namespace}/{name}` — Node status and phase
  - `GET /api/v1/workflows/{namespace}/{name}/log` — Pod and step container logs
- **Authentication**: ServiceAccount Bearer Token
- **Error Mapping**: Automatically converts HTTP 404 and connection errors into typed domain exceptions (`PipelineNotFoundError`, `RunNotFoundError`).

### Dagster & Prefect (`DagsterAdapter`, `PrefectAdapter`)
- Pluggable stubs available in `dataguard_adapters.dagster` and `dataguard_adapters.prefect`.
- Full GraphQL / Cloud REST integration planned for v0.2.

---

## Multi-Orchestrator Registry (`AdapterRegistry`)

In enterprise environments with heterogeneous orchestrators, `AdapterRegistry` manages multiple adapters concurrently:

```python
from dataguard_adapters.registry import AdapterRegistry
from dataguard_adapters.airflow import AirflowAdapter
from dataguard_adapters.argo import ArgoAdapter

registry = AdapterRegistry()
registry.register("airflow", AirflowAdapter(base_url="http://airflow:8080", username="admin", password="password"))
registry.register("argo", ArgoAdapter(base_url="https://argo:2746", token="k8s-token"))

# Query all registered orchestrators in parallel
all_pipelines = await registry.list_all_pipelines()

# Route to specific orchestrator
adapter = registry.get("airflow")
```

---

## Implementing a Custom Adapter

To add a new orchestrator (e.g., Luigi, Mage, Kubeflow):

1. Subclass `OrchestratorAdapter` in `packages/adapters/src/dataguard_adapters/`.
2. Implement required async methods:
   - `list_pipelines`: Returns `list[PipelineSummary]`
   - `get_pipeline_status`: Returns `PipelineStatus` with run history
   - `get_failure_details`: Returns `FailureDetails` with error messages and stack traces
   - `get_run_logs`: Returns log string trimmed to `max_lines`
   - `aclose`: Closes any underlying `httpx.AsyncClient` or network sessions.
3. Register the new adapter with `AdapterRegistry`.
