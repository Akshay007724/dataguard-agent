from __future__ import annotations

from datetime import datetime

from dataguard_core.logging import get_logger

from pipeline_sentinel.lineage.openlineage import LineageEdge, LineageGraph, LineageNode, MarquezClient

log = get_logger(__name__)


class LineageTracer:
    """Traces data lineage graphs via the OpenLineage / Marquez API.

    Args:
        client: Configured MarquezClient instance.
    """

    def __init__(self, client: MarquezClient) -> None:
        self._client = client

    async def trace(
        self,
        dataset_id: str | None = None,
        pipeline_id: str | None = None,
        direction: str = "both",
        depth: int = 3,
    ) -> LineageGraph:
        """Traverse the lineage graph from a dataset or pipeline node.

        Args:
            dataset_id: Start traversal from this dataset.
            pipeline_id: Start traversal from this pipeline/job.
            direction: 'upstream', 'downstream', or 'both'.
            depth: Maximum graph traversal depth.

        Returns:
            LineageGraph with nodes and edges.

        Raises:
            ValueError: If neither dataset_id nor pipeline_id is provided.
        """
        if dataset_id is None and pipeline_id is None:
            raise ValueError("Provide at least one of dataset_id or pipeline_id")

        if dataset_id:
            raw = await self._client.get_dataset_lineage(dataset_id, depth=depth)
            root_id = f"dataset:{dataset_id}"
        else:
            raw = await self._client.get_job_lineage(pipeline_id, depth=depth)  # type: ignore[arg-type]
            root_id = f"job:{pipeline_id}"

        if raw is None:
            log.warning("lineage_not_found", dataset_id=dataset_id, pipeline_id=pipeline_id)
            return LineageGraph(root_id=root_id)

        return self._parse_graph(root_id, raw, direction)

    @staticmethod
    def _parse_graph(root_id: str, raw: dict, direction: str) -> LineageGraph:  # type: ignore[type-arg]
        graph = LineageGraph(root_id=root_id)
        seen_nodes: set[str] = set()

        for node in raw.get("graph", []):
            node_id = node.get("id", "")
            node_type = "dataset" if node_id.startswith("dataset:") else "job"
            name_parts = node_id.split(":", 2)
            name = name_parts[2] if len(name_parts) == 3 else node_id
            namespace = name_parts[1] if len(name_parts) >= 2 else "default"

            if node_id not in seen_nodes:
                graph.nodes.append(
                    LineageNode(
                        id=node_id,
                        type=node_type,
                        name=name,
                        namespace=namespace,
                        last_run_at=_parse_dt(node.get("latestRun", {}).get("nominalEndTime")),
                        last_run_status=node.get("latestRun", {}).get("state"),
                    )
                )
                seen_nodes.add(node_id)

            in_edges = node.get("inEdges", [])
            out_edges = node.get("outEdges", [])

            if direction in ("upstream", "both"):
                for edge in in_edges:
                    origin = edge.get("origin", "")
                    graph.edges.append(LineageEdge(source_id=origin, target_id=node_id, relation="upstream"))

            if direction in ("downstream", "both"):
                for edge in out_edges:
                    destination = edge.get("destination", "")
                    graph.edges.append(LineageEdge(source_id=node_id, target_id=destination, relation="downstream"))

        return graph


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
