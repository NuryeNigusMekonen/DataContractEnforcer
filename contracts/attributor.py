from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import re
import subprocess
from pathlib import Path
import sys
from typing import Any

import yaml
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import dataset_kind_from, load_jsonl, normalize_contract_filename, parse_timestamp, utc_now
from contracts.lineage import resolve_contract_lineage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Attribute validation failures using lineage and git history.")
    parser.add_argument("--violation", required=True, help="Path to a validation report JSON file.")
    parser.add_argument("--lineage", required=True, help="Path to a lineage snapshots JSONL file.")
    parser.add_argument("--registry", required=False, help="Path to ContractRegistry subscriptions YAML.")
    parser.add_argument("--contract", required=False, help="Path to the generated contract YAML (optional fallback).")
    parser.add_argument("--output", required=False, help="Output JSONL path for attributed violations.")
    parser.add_argument("--since", default="14 days ago", help="Window for git log traversal.")
    parser.add_argument(
        "--live-summary",
        action="store_true",
        help="Print a concise terminal summary with failing check, lineage traversal, top commit, and blast radius.",
    )
    return parser.parse_args()


def repo_is_git_repo() -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def repo_has_commits() -> bool:
    if not repo_is_git_repo():
        return False
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def candidate_files(field_name: str, lineage_snapshot: dict[str, Any]) -> list[str]:
    keywords = [part for part in field_name.replace("_", ".").split(".") if part]
    matches: list[str] = []
    for node in lineage_snapshot.get("nodes", []):
        path = str(node.get("metadata", {}).get("path", ""))
        haystack = f"{node.get('node_id', '')} {path} {node.get('label', '')}".lower()
        if any(keyword.lower() in haystack for keyword in keywords):
            matches.append(path)
    if matches:
        return sorted({match for match in matches if match})
    dataset_hints = []
    lowered = field_name.lower()
    if "extracted_facts" in lowered or "entity" in lowered:
        dataset_hints = ["week3", "extract"]
    elif "event" in lowered or "sequence" in lowered:
        dataset_hints = ["week5", "event"]
    elif "verdict" in lowered or "score" in lowered:
        dataset_hints = ["week2", "verdict"]
    elif "trace" in lowered or "token" in lowered:
        dataset_hints = ["trace", "langsmith"]
    for node in lineage_snapshot.get("nodes", []):
        path = str(node.get("metadata", {}).get("path", ""))
        haystack = f"{node.get('node_id', '')} {path} {node.get('label', '')}".lower()
        if any(hint in haystack for hint in dataset_hints):
            matches.append(path)
    return sorted({match for match in matches if match})


def lineage_indexes(lineage_snapshot: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    node_index: dict[str, dict[str, Any]] = {}
    forward: dict[str, list[dict[str, Any]]] = {}
    reverse: dict[str, list[dict[str, Any]]] = {}
    for node in lineage_snapshot.get("nodes", []):
        node_id = str(node.get("node_id", ""))
        if node_id:
            node_index[node_id] = node
    for edge in lineage_snapshot.get("edges", []):
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if not source or not target:
            continue
        forward.setdefault(source, []).append(edge)
        reverse.setdefault(target, []).append(edge)
    return node_index, forward, reverse


def dataset_seed_nodes(contract: dict[str, Any], report: dict[str, Any], lineage_snapshot: dict[str, Any]) -> list[str]:
    source_path = str(contract.get("source_path") or report.get("data_path") or "")
    contract_id = str(contract.get("contract_id") or contract.get("id") or report.get("contract_id") or "")
    lowered_tokens = {token for token in re.findall(r"[a-z0-9]+", f"{source_path} {contract_id}".lower()) if len(token) >= 3}
    seeds: list[str] = []
    for node in lineage_snapshot.get("nodes", []):
        node_id = str(node.get("node_id", ""))
        node_path = str(node.get("metadata", {}).get("path", ""))
        label = str(node.get("label", ""))
        node_type = str(node.get("type", "")).upper()
        haystack = f"{node_id} {node_path} {label}".lower()
        if source_path and node_path == source_path:
            seeds.append(node_id)
            continue
        if node_type == "DATASET" and lowered_tokens and sum(token in haystack for token in lowered_tokens) >= 2:
            seeds.append(node_id)
    deduped: list[str] = []
    seen: set[str] = set()
    for node_id in seeds:
        if node_id not in seen:
            seen.add(node_id)
            deduped.append(node_id)
    return deduped


def _service_root_from_path(path: str) -> str:
    parts = Path(path).parts
    if "services" not in parts:
        return ""
    index = parts.index("services")
    if index + 1 >= len(parts):
        return ""
    return Path(*parts[: index + 2]).as_posix()


def traverse_upstream_producer_files(
    *,
    contract: dict[str, Any],
    report: dict[str, Any],
    failure: dict[str, Any],
    lineage_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    node_index, _, reverse = lineage_indexes(lineage_snapshot)
    seeds = dataset_seed_nodes(contract, report, lineage_snapshot)
    if not seeds:
        return []

    field_name = normalize_field_path(str(failure.get("column_name", "")))
    field_tokens = {token for token in re.findall(r"[a-z0-9]+", field_name.lower()) if len(token) >= 3}
    queue: deque[tuple[str, int]] = deque((seed, 0) for seed in seeds)
    visited: set[str] = set(seeds)
    service_candidates: list[dict[str, Any]] = []
    while queue:
        node_id, hops = queue.popleft()
        for edge in reverse.get(node_id, []):
            source_id = str(edge.get("source", ""))
            if not source_id or source_id in visited:
                continue
            visited.add(source_id)
            source_node = node_index.get(source_id, {})
            source_type = str(source_node.get("type", "")).upper()
            source_path = str(source_node.get("metadata", {}).get("path", ""))
            label = str(source_node.get("label", ""))
            next_hops = hops + 1
            if source_type == "SERVICE":
                service_candidates.append(
                    {
                        "node_id": source_id,
                        "service_path": source_path,
                        "service_label": label,
                        "hops": next_hops,
                    }
                )
            queue.append((source_id, next_hops))

    candidates: list[dict[str, Any]] = []
    for service in service_candidates:
        service_root = _service_root_from_path(service["service_path"])
        if not service_root:
            continue
        for node in lineage_snapshot.get("nodes", []):
            node_type = str(node.get("type", "")).upper()
            file_path = str(node.get("metadata", {}).get("path", ""))
            if node_type != "FILE" or not file_path.startswith(service_root):
                continue
            label = str(node.get("label", ""))
            haystack = f"{file_path} {label}".lower()
            token_matches = sum(token in haystack for token in field_tokens)
            candidates.append(
                {
                    "file_path": file_path,
                    "lineage_hops": service["hops"] + 1,
                    "producer_service": service["node_id"],
                    "producer_label": service["service_label"],
                    "token_matches": token_matches,
                }
            )

    ranked = sorted(
        candidates,
        key=lambda item: (-int(item["token_matches"]), int(item["lineage_hops"]), str(item["file_path"])),
    )
    deduped: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for candidate in ranked:
        path = str(candidate["file_path"])
        if path in seen_paths:
            continue
        seen_paths.add(path)
        deduped.append(candidate)
    return deduped


def existing_repo_paths(paths: list[str]) -> list[str]:
    existing = []
    for path in paths:
        candidate = Path(path)
        if candidate.exists():
            existing.append(candidate.as_posix())
    return existing


def special_case_candidates(field_name: str, report: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    data_path = str(report.get("data_path", ""))
    if "_violated" in data_path:
        candidates.append("create_violation.py")
        clean_variant = data_path.replace("_violated", "")
        if Path(clean_variant).exists():
            candidates.append(clean_variant)
    if "prompt_input_validation" in field_name:
        candidates.append("artifacts/week3/outputs/extractions.jsonl")
    return existing_repo_paths(candidates)


def commit_records_for(file_path: str, since: str, limit: int = 5) -> list[dict[str, str]]:
    if not repo_has_commits():
        return []
    result = subprocess.run(
        ["git", "log", "--follow", "--since", since, f"-n{limit}", "--format=%H|%an|%ae|%ai|%s", "--", file_path],
        capture_output=True,
        text=True,
        check=False,
    )
    commits: list[dict[str, str]] = []
    for line in result.stdout.splitlines():
        parts = line.split("|", 4)
        if len(parts) == 5:
            commit_hash, author_name, author_email, timestamp, message = parts
            commits.append(
                {
                    "commit_hash": commit_hash,
                    "author": author_email or author_name,
                    "commit_timestamp": timestamp,
                    "commit_message": message,
                }
            )
    return commits


def fallback_commit_record(file_path: str) -> dict[str, str]:
    path = Path(file_path)
    digest = hashlib.sha1()
    digest.update(file_path.encode("utf-8"))
    if path.exists() and path.is_file():
        digest.update(path.read_bytes())
    return {
        "commit_hash": digest.hexdigest(),
        "author": "workspace@local",
        "commit_timestamp": utc_now(),
        "commit_message": "Workspace state fallback before git history was available.",
    }


def days_since_commit(timestamp: str, reference_time: datetime | None = None) -> int:
    parsed = parse_timestamp(timestamp)
    if parsed is None:
        return 0
    current = reference_time or datetime.now(timezone.utc)
    delta = current - parsed.astimezone(timezone.utc)
    return max(0, delta.days)


def infer_contract_path(contract_id: str) -> str | None:
    if not contract_id:
        return None
    candidate = Path("generated_contracts") / f"{normalize_contract_filename(contract_id)}.yaml"
    return str(candidate) if candidate.exists() else None


def load_contract_context(
    *,
    contract_id: str,
    contract_path: str | None,
    report: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    resolved_path = contract_path or infer_contract_path(contract_id)
    if resolved_path and Path(resolved_path).exists():
        with Path(resolved_path).open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle), resolved_path
    data_path = str(report.get("data_path", ""))
    return (
        {
            "contract_id": contract_id,
            "id": contract_id,
            "dataset": dataset_kind_from(data_path, contract_id),
            "source_path": data_path,
            "info": {"title": contract_id},
        },
        resolved_path,
    )


def compute_lineage_blast_radius(
    *,
    contract: dict[str, Any],
    lineage_snapshot: dict[str, Any],
    registry_path: str | None,
    records_failing: int,
    violation_id: str,
    failing_field: str,
) -> dict[str, Any]:
    lineage = resolve_contract_lineage(contract, lineage_snapshot, registry_path)
    downstream = lineage.get("downstream", [])
    affected_subscribers = [entry["id"] for entry in downstream if entry.get("kind") == "SUBSCRIBER"]
    affected_contracts = [entry["id"] for entry in downstream if entry.get("kind") == "CONTRACT"]
    return {
        "violation_id": violation_id,
        "source": "tier1_transitive",
        "contract_id": str(contract.get("contract_id") or contract.get("id") or ""),
        "failing_field": normalize_field_path(failing_field),
        "affected_nodes": [entry.get("id") for entry in downstream],
        "affected_pipelines": [
            entry.get("id")
            for entry in downstream
            if str(entry.get("kind", "")).upper() in {"SERVICE", "SUBSCRIBER"}
        ],
        "affected_subscribers": affected_subscribers,
        "affected_contracts": affected_contracts,
        "estimated_records": records_failing,
        "graph_seeds": lineage.get("graph_seeds", []),
        "lineage": downstream,
    }


def normalize_field_path(field_name: str) -> str:
    normalized = re.sub(r"\[\d+\]", "", field_name)
    normalized = normalized.replace("[*]", "").replace("[]", "")
    normalized = normalized.replace(" ", "")
    normalized = normalized.strip(".")
    normalized = normalized.replace("..", ".")
    return normalized


def registry_blast_radius(contract_id: str, failing_field: str, registry_path: str | None) -> list[dict[str, Any]]:
    if not registry_path:
        return []
    path = Path(registry_path)
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    subscriptions = payload.get("subscriptions", [])
    if not isinstance(subscriptions, list):
        return []
    normalized_failing = normalize_field_path(failing_field)
    affected: list[dict[str, Any]] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        if str(subscription.get("contract_id", "")) != contract_id:
            continue
        for breaking_field in subscription.get("breaking_fields", []):
            if isinstance(breaking_field, dict):
                field = str(breaking_field.get("field", ""))
                reason = str(breaking_field.get("reason", ""))
            else:
                field = str(breaking_field)
                reason = ""
            normalized_breaking = normalize_field_path(field)
            if not normalized_breaking:
                continue
            if normalized_failing == normalized_breaking or normalized_failing.startswith(f"{normalized_breaking}."):
                affected.append(
                    {
                        "subscriber_id": str(subscription.get("subscriber_id", "")),
                        "contact": str(subscription.get("contact", "unknown")),
                        "matched_breaking_field": normalized_breaking,
                        "reason": reason,
                        "fields_consumed": list(subscription.get("fields_consumed", [])),
                        "validation_mode": str(subscription.get("validation_mode", "AUDIT")),
                        "registered_at": str(subscription.get("registered_at", "")),
                    }
                )
                break
    return affected


def annotate_contamination_depth(
    lineage_entries: list[dict[str, Any]],
    registry_matches: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int], int]:
    direct_subscribers = {str(entry.get("subscriber_id", "")) for entry in registry_matches if entry.get("subscriber_id")}
    annotated: list[dict[str, Any]] = []
    contamination_depth: dict[str, int] = {}
    max_depth = 0
    for entry in lineage_entries:
        item = dict(entry)
        entry_id = str(item.get("id", ""))
        via = [str(value) for value in item.get("via", [])]
        if entry_id in direct_subscribers:
            depth = 1
        else:
            depth = max(1, int(item.get("hops", 0)))
            for index, node_id in enumerate(via):
                if node_id in direct_subscribers:
                    depth = 1 + (len(via) - index)
                    break
        item["contamination_depth"] = depth
        annotated.append(item)
        if entry_id:
            contamination_depth[entry_id] = depth
        max_depth = max(max_depth, depth)
    return annotated, contamination_depth, max_depth


def compute_blast_radius(
    *,
    contract_id: str,
    failing_field: str,
    records_failing: int,
    violation_id: str,
    lineage_snapshot: dict[str, Any],
    registry_path: str | None,
    contract: dict[str, Any],
) -> dict[str, Any]:
    affected = registry_blast_radius(contract_id, failing_field, registry_path)
    lineage_blast_radius = compute_lineage_blast_radius(
        contract=contract,
        lineage_snapshot=lineage_snapshot,
        registry_path=registry_path,
        records_failing=records_failing,
        violation_id=violation_id,
        failing_field=failing_field,
    )
    annotated_lineage, contamination_depth, max_depth = annotate_contamination_depth(
        list(lineage_blast_radius.get("lineage", [])),
        affected,
    )
    lineage_blast_radius["lineage"] = annotated_lineage
    lineage_blast_radius["affected_nodes"] = [entry.get("id") for entry in annotated_lineage]
    lineage_blast_radius["affected_pipelines"] = [
        entry.get("id")
        for entry in annotated_lineage
        if str(entry.get("kind", "")).upper() in {"SERVICE", "SUBSCRIBER"}
    ]
    lineage_blast_radius["affected_contacts"] = sorted({entry["contact"] for entry in affected})
    lineage_blast_radius["matches"] = affected
    lineage_blast_radius["contamination_depth"] = contamination_depth
    lineage_blast_radius["max_contamination_depth"] = max_depth
    return lineage_blast_radius


def build_blame_chain(candidates: list[dict[str, Any]], since: str) -> list[dict[str, Any]]:
    reference_time = parse_timestamp(utc_now()) or datetime.now(timezone.utc)
    chain: list[dict[str, Any]] = []
    for candidate in candidates:
        file_path = str(candidate["file_path"])
        commit = commit_records_for(file_path, since, limit=1)
        commit_meta = commit[0] if commit else fallback_commit_record(file_path)
        hops = int(candidate.get("lineage_hops", 0))
        days = days_since_commit(str(commit_meta["commit_timestamp"]), reference_time)
        confidence = max(0.0, round((1.0 - (days * 0.1)) - (hops * 0.2), 2))
        chain.append(
            {
                "file_path": file_path,
                "commit_hash": commit_meta["commit_hash"],
                "author": commit_meta["author"],
                "commit_timestamp": commit_meta["commit_timestamp"],
                "commit_message": commit_meta["commit_message"],
                "confidence_score": confidence,
                "lineage_hops": hops,
                "days_since_commit": days,
                "producer_service": candidate.get("producer_service", ""),
            }
        )
    ranked = sorted(
        chain,
        key=lambda item: (-float(item["confidence_score"]), int(item["lineage_hops"]), str(item["file_path"])),
    )[:5]
    for rank, entry in enumerate(ranked, start=1):
        entry["rank"] = rank
    return ranked


def infer_candidate_files(
    failure: dict[str, Any],
    lineage_snapshot: dict[str, Any],
    report: dict[str, Any],
    contract: dict[str, Any],
) -> list[dict[str, Any]]:
    field_name = str(failure.get("column_name", ""))
    traversal_candidates = traverse_upstream_producer_files(
        contract=contract,
        report=report,
        failure=failure,
        lineage_snapshot=lineage_snapshot,
    )
    files: list[dict[str, Any]] = traversal_candidates[:]
    for file_path in special_case_candidates(field_name, report):
        files.append({"file_path": file_path, "lineage_hops": 0, "producer_service": "", "token_matches": 0})
    for file_path in existing_repo_paths(candidate_files(field_name, lineage_snapshot)):
        files.append({"file_path": file_path, "lineage_hops": 1, "producer_service": "", "token_matches": 0})
    contract_source = str(report.get("data_path", ""))
    if not traversal_candidates and contract_source and Path(contract_source).exists():
        files.append({"file_path": contract_source, "lineage_hops": 0, "producer_service": "", "token_matches": 0})
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in files:
        file_path = str(item["file_path"])
        if file_path not in seen:
            seen.add(file_path)
            deduped.append(item)
    return deduped[:5]


def attribute_failure(
    failure: dict[str, Any],
    lineage_snapshot: dict[str, Any],
    contract_id: str,
    contract_path: str | None,
    registry_path: str | None,
    report: dict[str, Any],
    since: str,
) -> dict[str, Any]:
    field_name = failure.get("column_name", "")
    contract, resolved_contract_path = load_contract_context(
        contract_id=contract_id,
        contract_path=contract_path,
        report=report,
    )
    files = infer_candidate_files(failure, lineage_snapshot, report, contract)
    blame_chain = build_blame_chain(files, since)
    violation_id = f"{failure.get('check_id')}-{utc_now()}"
    schema_evolution = report.get("schema_evolution", {})
    adapter = report.get("adapter", {})
    return {
        "violation_id": violation_id,
        "detected_at": utc_now(),
        "status": failure.get("status"),
        "severity": failure.get("severity"),
        "check_id": failure.get("check_id"),
        "field_name": field_name,
        "message": failure.get("message"),
        "records_failing": failure.get("records_failing"),
        "candidate_files": [item["file_path"] for item in files],
        "blame_chain": blame_chain,
        "blast_radius": compute_blast_radius(
            contract_id=contract_id,
            failing_field=str(field_name),
            records_failing=int(failure.get("records_failing", 0)),
            violation_id=violation_id,
            lineage_snapshot=lineage_snapshot,
            registry_path=registry_path,
            contract=contract,
        ),
        "contract_path": resolved_contract_path or "",
        "compatibility_failure_cause": schema_evolution.get("primary_breaking_change"),
        "compatibility_classification": schema_evolution.get("compatibility_classification", ""),
        "adapter_attempted": bool(adapter.get("attempted", False)),
        "adapter_applied": bool(adapter.get("applied", False)),
        "adapter_succeeded": bool(adapter.get("succeeded", False)),
        "fallback_succeeded": bool(adapter.get("fallback_succeeded", False)),
        "git_context": "git history scanned" if repo_has_commits() else "workspace fallback commit metadata used",
        "samples": failure.get("samples", []),
    }


def contract_id_from_args_or_report(report: dict[str, Any], contract_path: str | None) -> str:
    report_id = str(report.get("contract_id", ""))
    if report_id:
        return report_id
    if contract_path and Path(contract_path).exists():
        with Path(contract_path).open("r", encoding="utf-8") as handle:
            contract = yaml.safe_load(handle)
        return str(contract.get("contract_id") or contract.get("id") or "")
    return ""


def _lineage_path(record: dict[str, Any]) -> list[str]:
    blast_radius = record.get("blast_radius", {})
    lineage = blast_radius.get("lineage", [])
    ordered = sorted(lineage, key=lambda entry: int(entry.get("hops", 0)))
    return [str(entry.get("id", "")) for entry in ordered if entry.get("id")]


def _top_blame(record: dict[str, Any]) -> dict[str, Any]:
    blame_chain = record.get("blame_chain", [])
    return blame_chain[0] if blame_chain else {}


def primary_live_summary_record(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not records:
        return None
    preferred_checks = [
        "extracted_facts.confidence.range",
        "week3.confidence_unit_scale",
        "extracted_facts.confidence.drift",
    ]
    for check_id in preferred_checks:
        for record in records:
            if str(record.get("check_id", "")) == check_id:
                return record
    return records[0]


def render_live_summary(record: dict[str, Any]) -> str:
    top_blame = _top_blame(record)
    lineage_path = _lineage_path(record)
    failing_check = str(record.get("check_id", ""))
    field_name = str(record.get("field_name", ""))
    top_file = str(top_blame.get("file_path", "unknown"))
    top_commit = str(top_blame.get("commit_hash", "unknown"))
    top_author = str(top_blame.get("author", "unknown"))
    affected_nodes = record.get("blast_radius", {}).get("affected_nodes", [])
    first_hop = lineage_path[0] if lineage_path else "none"
    traversal = " -> ".join([failing_check, *lineage_path]) if lineage_path else failing_check
    lines = [
        "ViolationAttributor live summary",
        f"Failing check: {failing_check}",
        f"Field: {field_name}",
        f"Top cause: {top_file}",
        f"Commit: {top_commit}",
        f"Author: {top_author}",
        f"Lineage traversal: {traversal}",
        f"First downstream hop: {first_hop}",
        f"Blast radius: {', '.join(str(node) for node in affected_nodes) if affected_nodes else 'none'}",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    report = json.loads(Path(args.violation).read_text(encoding="utf-8"))
    registry_path = args.registry
    default_registry = "contract_registry/subscriptions.yaml"
    if registry_path is None and Path(default_registry).exists():
        registry_path = default_registry
    contract_id = contract_id_from_args_or_report(report, args.contract)
    lineage_snapshots = load_jsonl(args.lineage)
    lineage_snapshot = lineage_snapshots[-1] if lineage_snapshots else {}
    failures = [result for result in report.get("results", []) if result.get("status") in {"FAIL", "ERROR"}]
    attributed = [
        attribute_failure(
            failure,
            lineage_snapshot,
            contract_id,
            args.contract,
            registry_path,
            report,
            args.since,
        )
        for failure in failures
    ]
    output_path = Path(args.output or "violation_log/violations.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for record in attributed:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "written": len(attributed),
                "output": str(output_path),
                "contract_id": contract_id,
                "registry": registry_path or "",
            },
            indent=2,
        )
    )
    if args.live_summary:
        primary_record = primary_live_summary_record(attributed)
        if primary_record is not None:
            print(render_live_summary(primary_record))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
