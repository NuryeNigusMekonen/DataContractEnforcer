from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
import sys
from typing import Any

import yaml
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import load_jsonl, utc_now


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Attribute validation failures using lineage and git history.")
    parser.add_argument("--violation", required=True, help="Path to a validation report JSON file.")
    parser.add_argument("--lineage", required=True, help="Path to a lineage snapshots JSONL file.")
    parser.add_argument("--contract", required=True, help="Path to the generated contract YAML.")
    parser.add_argument("--output", required=False, help="Output JSONL path for attributed violations.")
    parser.add_argument("--since", default="14 days ago", help="Window for git log traversal.")
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


def lineage_hops(file_path: str, lineage_snapshot: dict[str, Any]) -> int:
    for node in lineage_snapshot.get("nodes", []):
        if str(node.get("metadata", {}).get("path", "")) == file_path:
            return 0
    return 1


def compute_blast_radius(contract_path: str, records_failing: int, violation_id: str) -> dict[str, Any]:
    with Path(contract_path).open("r", encoding="utf-8") as handle:
        contract = yaml.safe_load(handle)
    downstream = contract.get("lineage", {}).get("downstream", [])
    return {
        "violation_id": violation_id,
        "affected_nodes": [entry.get("id") for entry in downstream],
        "affected_pipelines": [entry.get("id") for entry in downstream if "pipeline" in str(entry.get("id"))],
        "estimated_records": records_failing,
    }


def build_blame_chain(files: list[str], lineage_snapshot: dict[str, Any], since: str) -> list[dict[str, Any]]:
    chain: list[dict[str, Any]] = []
    for rank, file_path in enumerate(files[:5], start=1):
        commit = commit_records_for(file_path, since, limit=1)
        commit_meta = commit[0] if commit else fallback_commit_record(file_path)
        hops = lineage_hops(file_path, lineage_snapshot)
        confidence = max(0.1, round(1.0 - (0.2 * hops) - (0.05 * (rank - 1)), 2))
        chain.append(
            {
                "rank": rank,
                "file_path": file_path,
                "commit_hash": commit_meta["commit_hash"],
                "author": commit_meta["author"],
                "commit_timestamp": commit_meta["commit_timestamp"],
                "commit_message": commit_meta["commit_message"],
                "confidence_score": confidence,
            }
        )
    return chain


def infer_candidate_files(failure: dict[str, Any], lineage_snapshot: dict[str, Any], report: dict[str, Any]) -> list[str]:
    field_name = str(failure.get("column_name", ""))
    files = special_case_candidates(field_name, report)
    files.extend(existing_repo_paths(candidate_files(field_name, lineage_snapshot)))
    contract_source = str(report.get("data_path", ""))
    if contract_source and Path(contract_source).exists():
        files.append(contract_source)
    deduped: list[str] = []
    seen: set[str] = set()
    for file_path in files:
        if file_path not in seen:
            seen.add(file_path)
            deduped.append(file_path)
    return deduped[:5]


def attribute_failure(
    failure: dict[str, Any],
    lineage_snapshot: dict[str, Any],
    contract_path: str,
    report: dict[str, Any],
    since: str,
) -> dict[str, Any]:
    field_name = failure.get("column_name", "")
    files = infer_candidate_files(failure, lineage_snapshot, report)
    blame_chain = build_blame_chain(files, lineage_snapshot, since)
    violation_id = f"{failure.get('check_id')}-{utc_now()}"
    return {
        "violation_id": violation_id,
        "detected_at": utc_now(),
        "status": failure.get("status"),
        "severity": failure.get("severity"),
        "check_id": failure.get("check_id"),
        "field_name": field_name,
        "message": failure.get("message"),
        "records_failing": failure.get("records_failing"),
        "candidate_files": files,
        "blame_chain": blame_chain,
        "blast_radius": compute_blast_radius(contract_path, int(failure.get("records_failing", 0)), violation_id),
        "git_context": "git history scanned" if repo_has_commits() else "workspace fallback commit metadata used",
        "samples": failure.get("samples", []),
    }


def main() -> int:
    args = parse_args()
    report = json.loads(Path(args.violation).read_text(encoding="utf-8"))
    lineage_snapshots = load_jsonl(args.lineage)
    lineage_snapshot = lineage_snapshots[-1] if lineage_snapshots else {}
    failures = [result for result in report.get("results", []) if result.get("status") in {"FAIL", "ERROR"}]
    attributed = [attribute_failure(failure, lineage_snapshot, args.contract, report, args.since) for failure in failures]
    output_path = Path(args.output or "violation_log/violations.jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for record in attributed:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    print(json.dumps({"written": len(attributed), "output": str(output_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
