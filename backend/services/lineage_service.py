from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.services.common import OUTPUTS_DIR, parse_timestamp, read_jsonl_file, timestamp_to_iso


LINEAGE_PATH = OUTPUTS_DIR / "week4" / "lineage_snapshots.jsonl"


def _latest_snapshot() -> dict[str, Any]:
    records = read_jsonl_file(LINEAGE_PATH)
    if not records:
        return {}
    latest = records[-1]
    if not isinstance(latest, dict):
        return {}
    return latest


def _normalize_nodes_edges(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    nodes: list[dict[str, Any]] = []
    node_ids: set[str] = set()
    for raw in snapshot.get("nodes", []):
        if not isinstance(raw, dict):
            continue
        node_id = str(raw.get("node_id", "")).strip()
        if not node_id:
            continue
        metadata = raw.get("metadata", {}) if isinstance(raw.get("metadata"), dict) else {}
        node = {
            "id": node_id,
            "label": str(raw.get("label") or node_id),
            "type": str(raw.get("type") or "UNKNOWN"),
            "path": str(metadata.get("path") or ""),
            "purpose": str(metadata.get("purpose") or ""),
        }
        nodes.append(node)
        node_ids.add(node_id)

    edges: list[dict[str, Any]] = []
    for raw in snapshot.get("edges", []):
        if not isinstance(raw, dict):
            continue
        source = str(raw.get("source", "")).strip()
        target = str(raw.get("target", "")).strip()
        if not source or not target or source not in node_ids or target not in node_ids:
            continue
        relationship = str(raw.get("relationship") or "DEPENDS_ON")
        try:
            confidence = float(raw.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        edges.append(
            {
                "source": source,
                "target": target,
                "relationship": relationship,
                "confidence": max(0.0, min(1.0, confidence)),
            }
        )
    return nodes, edges


def _is_cross_week_node(node_id: str) -> bool:
    if node_id.startswith("dataset::outputs/"):
        return True
    if node_id.startswith("service::week"):
        return True
    return False


def _cross_week_view(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept_ids = {node["id"] for node in nodes if _is_cross_week_node(node["id"])}
    filtered_edges = [edge for edge in edges if edge["source"] in kept_ids and edge["target"] in kept_ids]

    # Keep only nodes that actually participate in visible edges.
    active_ids: set[str] = set()
    for edge in filtered_edges:
        active_ids.add(edge["source"])
        active_ids.add(edge["target"])
    filtered_nodes = [node for node in nodes if node["id"] in active_ids]
    return filtered_nodes, filtered_edges


def _dedupe_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for edge in edges:
        key = (edge["source"], edge["target"], edge["relationship"])
        existing = merged.get(key)
        if existing is None:
            merged[key] = dict(edge)
            continue
        existing["confidence"] = max(float(existing.get("confidence", 0.0)), float(edge.get("confidence", 0.0)))
    return list(merged.values())


def _inject_week7_outputs(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {str(node["id"]): dict(node) for node in nodes if node.get("id")}

    week7_service_nodes = {
        "service::week7-validation-runner": {
            "id": "service::week7-validation-runner",
            "label": "week7-validation-runner",
            "type": "SERVICE",
            "path": "contracts/runner.py",
            "purpose": "validate contract checks and emit validation reports",
        },
        "service::week7-ai-contract-extension": {
            "id": "service::week7-ai-contract-extension",
            "label": "week7-ai-contract-extension",
            "type": "SERVICE",
            "path": "contracts/ai_extensions.py",
            "purpose": "run AI-specific checks and emit AI extension reports",
        },
        "service::week7-violation-attributor": {
            "id": "service::week7-violation-attributor",
            "label": "week7-violation-attributor",
            "type": "SERVICE",
            "path": "contracts/attributor.py",
            "purpose": "attribute violations and emit blame-chain records",
        },
    }
    for service_id, service_node in week7_service_nodes.items():
        if service_id not in by_id:
            by_id[service_id] = service_node

    week7_outputs = [
        (
            "service::week7-validation-runner",
            {
                "id": "dataset::outputs/week7/validation_reports.json",
                "label": "week7_validation_reports.json",
                "type": "TABLE",
                "path": "validation_reports/",
                "purpose": "aggregated week7 contract validation results",
            },
        ),
        (
            "service::week7-ai-contract-extension",
            {
                "id": "dataset::outputs/week7/ai_extensions.json",
                "label": "week7_ai_extensions.json",
                "type": "TABLE",
                "path": "validation_reports/ai_extensions.json",
                "purpose": "AI contract extension checks and metrics",
            },
        ),
        (
            "service::week7-violation-attributor",
            {
                "id": "dataset::outputs/week7/violation_log.jsonl",
                "label": "week7_violation_log.jsonl",
                "type": "TABLE",
                "path": "violation_log/violations.jsonl",
                "purpose": "violation attribution records with blast radius",
            },
        ),
    ]

    augmented_edges = list(edges)
    for producer, output_node in week7_outputs:
        output_id = str(output_node["id"])
        if output_id not in by_id:
            by_id[output_id] = output_node
        augmented_edges.append(
            {
                "source": producer,
                "target": output_id,
                "relationship": "PRODUCES",
                "confidence": 0.98,
            }
        )

    return list(by_id.values()), _dedupe_edges(augmented_edges)


def get_lineage_map() -> dict[str, Any]:
    snapshot = _latest_snapshot()
    if not snapshot:
        return {
            "status": "missing",
            "captured_at": None,
            "last_updated": None,
            "full": {"nodes": [], "edges": [], "node_count": 0, "edge_count": 0},
            "cross_week": {"nodes": [], "edges": [], "node_count": 0, "edge_count": 0},
        }

    nodes, edges = _normalize_nodes_edges(snapshot)
    edges = _dedupe_edges(edges)
    nodes, edges = _inject_week7_outputs(nodes, edges)
    cross_nodes, cross_edges = _cross_week_view(nodes, edges)

    captured_at_raw = snapshot.get("captured_at")
    captured_at = timestamp_to_iso(parse_timestamp(captured_at_raw))
    if captured_at is None:
        captured_at = captured_at_raw if isinstance(captured_at_raw, str) else None
    last_updated = timestamp_to_iso(datetime.fromtimestamp(LINEAGE_PATH.stat().st_mtime, tz=timezone.utc))

    return {
        "status": "ok",
        "captured_at": captured_at,
        "last_updated": last_updated,
        "full": {
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
        "cross_week": {
            "nodes": cross_nodes,
            "edges": cross_edges,
            "node_count": len(cross_nodes),
            "edge_count": len(cross_edges),
        },
    }
