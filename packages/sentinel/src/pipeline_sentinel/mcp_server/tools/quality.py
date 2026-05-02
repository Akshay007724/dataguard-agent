from __future__ import annotations

import json
from typing import Any

from dataguard_core.logging import get_logger

from pipeline_sentinel.detectors.base import BaseDetector

log = get_logger(__name__)


async def handle_check_data_quality(
    detectors: list[BaseDetector],
    arguments: dict[str, Any],
) -> str:
    dataset_id: str = arguments["dataset_id"]
    requested_checks: list[str] | None = arguments.get("checks")

    active_detectors = [
        d for d in detectors
        if requested_checks is None or d.name in requested_checks
    ]

    if not active_detectors:
        return json.dumps({
            "dataset_id": dataset_id,
            "error": "No matching detectors found",
            "available_detectors": [d.name for d in detectors],
        })

    results = []
    for detector in active_detectors:
        try:
            result = await detector.run(dataset_id)
            results.append({
                "detector": result.detector,
                "dataset_id": result.dataset_id,
                "passed": result.passed,
                "highest_severity": result.highest_severity,
                "checks": [
                    {
                        "name": c.name,
                        "passed": c.passed,
                        "severity": c.severity,
                        "message": c.message,
                        "actual": c.actual,
                        "expected": c.expected,
                        "sample_rows": c.sample_rows[:5],
                    }
                    for c in result.checks
                ],
            })
        except Exception as exc:
            log.warning("detector_failed", detector=detector.name, dataset_id=dataset_id, error=str(exc))
            results.append({
                "detector": detector.name,
                "dataset_id": dataset_id,
                "error": str(exc),
            })

    overall_passed = all(r.get("passed", False) for r in results if "error" not in r)
    return json.dumps({
        "dataset_id": dataset_id,
        "overall_passed": overall_passed,
        "detectors_run": len(results),
        "results": results,
    })


async def handle_trace_lineage(
    tracer: Any,
    arguments: dict[str, Any],
) -> str:
    from pipeline_sentinel.lineage.tracer import LineageTracer

    tracer_: LineageTracer = tracer
    dataset_id: str | None = arguments.get("dataset_id")
    pipeline_id: str | None = arguments.get("pipeline_id")
    direction: str = arguments.get("direction", "both")
    depth: int = int(arguments.get("depth", 3))

    if not dataset_id and not pipeline_id:
        return json.dumps({"error": "Provide at least one of dataset_id or pipeline_id"})

    graph = await tracer_.trace(
        dataset_id=dataset_id,
        pipeline_id=pipeline_id,
        direction=direction,
        depth=depth,
    )

    return json.dumps({
        "root_id": graph.root_id,
        "node_count": len(graph.nodes),
        "edge_count": len(graph.edges),
        "nodes": [
            {
                "id": n.id,
                "type": n.type,
                "name": n.name,
                "namespace": n.namespace,
                "last_run_at": n.last_run_at.isoformat() if n.last_run_at else None,
                "last_run_status": n.last_run_status,
            }
            for n in graph.nodes
        ],
        "edges": [
            {
                "source_id": e.source_id,
                "target_id": e.target_id,
                "relation": e.relation,
            }
            for e in graph.edges
        ],
    }, default=str)
