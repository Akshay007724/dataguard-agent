from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import httpx

from dataguard_core.logging import get_logger

log = get_logger(__name__)


@dataclass
class LineageNode:
    id: str
    type: str  # dataset | job
    name: str
    namespace: str
    last_run_at: datetime | None = None
    last_run_status: str | None = None


@dataclass
class LineageEdge:
    source_id: str
    target_id: str
    relation: str  # upstream | downstream


@dataclass
class LineageGraph:
    root_id: str
    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    def node_by_id(self, node_id: str) -> LineageNode | None:
        return next((n for n in self.nodes if n.id == node_id), None)


class MarquezClient:
    """Read-only Marquez API client for OpenLineage graph traversal.

    Args:
        base_url: Marquez API base URL, e.g. http://marquez:5000.
        namespace: OpenLineage namespace to query.
    """

    def __init__(self, base_url: str, namespace: str = "default") -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=15,
            headers={"Accept": "application/json"},
        )
        self._namespace = namespace

    async def get_dataset(self, dataset_id: str) -> dict | None:  # type: ignore[type-arg]
        resp = await self._client.get(
            f"/api/v1/namespaces/{self._namespace}/datasets/{dataset_id}"
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def get_dataset_lineage(
        self,
        dataset_id: str,
        depth: int = 3,
    ) -> dict | None:  # type: ignore[type-arg]
        resp = await self._client.get(
            f"/api/v1/lineage",
            params={
                "nodeId": f"dataset:{self._namespace}:{dataset_id}",
                "depth": depth,
            },
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def get_job_lineage(
        self,
        job_name: str,
        depth: int = 3,
    ) -> dict | None:  # type: ignore[type-arg]
        resp = await self._client.get(
            f"/api/v1/lineage",
            params={
                "nodeId": f"job:{self._namespace}:{job_name}",
                "depth": depth,
            },
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def get_job_runs(self, job_name: str, limit: int = 5) -> list[dict]:  # type: ignore[type-arg]
        resp = await self._client.get(
            f"/api/v1/namespaces/{self._namespace}/jobs/{job_name}/runs",
            params={"limit": limit},
        )
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return resp.json().get("runs", [])  # type: ignore[no-any-return]

    async def close(self) -> None:
        await self._client.aclose()
