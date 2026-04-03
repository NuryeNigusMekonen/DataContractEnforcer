from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from simulators.common import (
    JsonDict,
    ROOT,
    ViolationSpec,
    canonical_system_name,
    deep_copy_records,
    deterministic_uuid,
    fake_git_sha,
    isoformat_z,
    selected_indices,
)


def _node(node_id: str, label: str, node_type: str, path: str, captured_at: str, purpose: str) -> JsonDict:
    suffix = path.rsplit(".", 1)[-1] if "." in path else "txt"
    return {
        "node_id": node_id,
        "label": label,
        "type": node_type,
        "metadata": {
            "path": path,
            "language": suffix,
            "purpose": purpose,
            "last_modified": captured_at,
        },
    }


def _base_nodes(captured_at: str, week1_records: list[JsonDict], week3_records: list[JsonDict]) -> list[JsonDict]:
    nodes = [
        _node("service::week1-intent-tracker", "week1-intent-tracker", "SERVICE", "services/week1-intent-tracker/main.py", captured_at, "producer"),
        _node("dataset::outputs/week1/intent_records.jsonl", "intent_records.jsonl", "DATASET", "outputs/week1/intent_records.jsonl", captured_at, "dataset"),
        _node("service::week2-digital-courtroom", "week2-digital-courtroom", "SERVICE", "services/week2-digital-courtroom/main.py", captured_at, "producer"),
        _node("dataset::outputs/week2/verdicts.jsonl", "verdicts.jsonl", "DATASET", "outputs/week2/verdicts.jsonl", captured_at, "dataset"),
        _node("service::week3-document-refinery", "week3-document-refinery", "SERVICE", "services/week3-document-refinery/main.py", captured_at, "producer"),
        _node("dataset::outputs/week3/extractions.jsonl", "extractions.jsonl", "DATASET", "outputs/week3/extractions.jsonl", captured_at, "dataset"),
        _node("service::week4-brownfield-cartographer", "week4-brownfield-cartographer", "SERVICE", "services/week4-brownfield-cartographer/main.py", captured_at, "producer"),
        _node("dataset::outputs/week4/lineage_snapshots.jsonl", "lineage_snapshots.jsonl", "DATASET", "outputs/week4/lineage_snapshots.jsonl", captured_at, "dataset"),
        _node("service::week5-event-ledger", "week5-event-ledger", "SERVICE", "services/week5-event-ledger/main.py", captured_at, "producer"),
        _node("dataset::outputs/week5/events.jsonl", "events.jsonl", "DATASET", "outputs/week5/events.jsonl", captured_at, "dataset"),
        _node("dataset::outputs/traces/runs.jsonl", "runs.jsonl", "DATASET", "outputs/traces/runs.jsonl", captured_at, "dataset"),
        _node("service::week7-validation-runner", "week7-validation-runner", "SERVICE", "contracts/runner.py", captured_at, "consumer"),
        _node("service::week7-ai-contract-extension", "week7-ai-contract-extension", "SERVICE", "contracts/ai_extensions.py", captured_at, "consumer"),
        _node("service::week7-violation-attributor", "week7-violation-attributor", "SERVICE", "contracts/attributor.py", captured_at, "consumer"),
    ]
    unique_paths: list[str] = []
    for record in week1_records[:3]:
        for code_ref in record.get("code_refs", []):
            file_path = code_ref.get("file")
            if isinstance(file_path, str) and file_path not in unique_paths:
                unique_paths.append(file_path)
    for record in week3_records[:2]:
        source_path = record.get("source_path")
        if isinstance(source_path, str) and source_path not in unique_paths:
            unique_paths.append(source_path)
    for path in unique_paths[:5]:
        label = path.rsplit("/", 1)[-1]
        nodes.append(_node(f"file::{path}", label, "FILE", path, captured_at, "source_file"))
    return nodes


def _edges(nodes: list[JsonDict]) -> list[JsonDict]:
    node_ids = {node["node_id"] for node in nodes}
    raw_edges = [
        ("service::week1-intent-tracker", "dataset::outputs/week1/intent_records.jsonl", "PRODUCES", 0.98),
        ("dataset::outputs/week1/intent_records.jsonl", "service::week2-digital-courtroom", "CONSUMES", 0.96),
        ("service::week2-digital-courtroom", "dataset::outputs/week2/verdicts.jsonl", "PRODUCES", 0.97),
        ("service::week3-document-refinery", "dataset::outputs/week3/extractions.jsonl", "PRODUCES", 0.98),
        ("service::week3-document-refinery", "dataset::outputs/traces/runs.jsonl", "WRITES", 0.93),
        ("dataset::outputs/week3/extractions.jsonl", "service::week4-brownfield-cartographer", "CONSUMES", 0.95),
        ("service::week4-brownfield-cartographer", "dataset::outputs/week4/lineage_snapshots.jsonl", "PRODUCES", 0.97),
        ("service::week5-event-ledger", "dataset::outputs/week5/events.jsonl", "PRODUCES", 0.98),
        ("dataset::outputs/week5/events.jsonl", "service::week7-validation-runner", "CONSUMES", 0.94),
        ("dataset::outputs/week4/lineage_snapshots.jsonl", "service::week7-violation-attributor", "CONSUMES", 0.95),
        ("dataset::outputs/traces/runs.jsonl", "service::week7-ai-contract-extension", "CONSUMES", 0.92),
        ("dataset::outputs/week2/verdicts.jsonl", "service::week7-ai-contract-extension", "CONSUMES", 0.93),
    ]
    file_nodes = [node_id for node_id in node_ids if node_id.startswith("file::")]
    if file_nodes:
        raw_edges.append(("file::contracts/runner.py", "service::week7-validation-runner", "CALLS", 0.88))
    edges: list[JsonDict] = []
    for source, target, relationship, confidence in raw_edges:
        if source not in node_ids or target not in node_ids:
            continue
        edges.append(
            {
                "source": source,
                "target": target,
                "relationship": relationship,
                "confidence": confidence,
            }
        )
    return edges


def generate_week4_records(
    count: int,
    seed: int,
    violations: list[ViolationSpec] | None = None,
    week1_records: list[JsonDict] | None = None,
    week3_records: list[JsonDict] | None = None,
) -> list[JsonDict]:
    week1_records = week1_records or []
    week3_records = week3_records or []
    base_time = datetime(2026, 3, 31, 1, 25, tzinfo=UTC)
    records: list[JsonDict] = []
    for index in range(count):
        captured_at = isoformat_z(base_time + timedelta(hours=index * 6))
        nodes = _base_nodes(captured_at, week1_records, week3_records)
        records.append(
            {
                "snapshot_id": deterministic_uuid("week4", index),
                "codebase_root": str(ROOT),
                "git_commit": fake_git_sha("week4", index, captured_at),
                "nodes": nodes,
                "edges": _edges(nodes),
                "captured_at": captured_at,
            }
        )
    return apply_week4_violations(records, violations or [], seed)


def apply_week4_violations(records: list[JsonDict], violations: list[ViolationSpec], seed: int) -> list[JsonDict]:
    if not violations:
        return records
    mutated = deep_copy_records(records)
    for violation in violations:
        if canonical_system_name(violation.system) != "week4":
            continue
        for index in selected_indices(mutated, violation, seed):
            record = mutated[index]
            if violation.type == "missing_node_ref":
                if record.get("edges"):
                    record["edges"][0]["target"] = "service::ghost-node"
            elif violation.type == "invalid_relationship_enum":
                if record.get("edges"):
                    record["edges"][0]["relationship"] = "MUTATES"
            elif violation.type == "malformed_git_commit":
                record["git_commit"] = "not-a-valid-git-sha"
            else:
                raise ValueError(f"unsupported week4 violation: {violation.type}")
    return mutated

