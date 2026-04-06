from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .common import (
    ENFORCER_REPORT_DIR,
    REPO_ROOT,
    SCHEMA_SNAPSHOTS_DIR,
    VALIDATION_REPORTS_DIR,
    VIOLATION_LOG_DIR,
    best_timestamp,
    count_critical_violations,
    compute_health_score,
    load_current_run_mode,
    parse_timestamp,
    read_json_file,
    read_jsonl_file,
    timestamp_to_iso,
)


GENERATED_CONTRACTS_DIR = REPO_ROOT / "generated_contracts"


def _relative_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _updated_at(path: Path) -> str | None:
    return timestamp_to_iso(best_timestamp(None, path))


def _preview_text(path: Path, *, max_lines: int = 16, max_chars: int = 1600) -> str:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return ""
    preview = "\n".join(lines[:max_lines])
    if len(preview) > max_chars:
        return f"{preview[: max_chars - 1].rstrip()}…"
    return preview


def _rule_summary(rule: Any) -> str:
    if not isinstance(rule, dict):
        return "Rule details unavailable."

    rule_type = str(rule.get("type") or "rule")
    field = str(rule.get("field") or "*")
    details: list[str] = []
    for key in ("format", "minimum", "maximum", "enum", "threshold", "operator"):
        value = rule.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            value = ", ".join(str(item) for item in value[:3])
        details.append(f"{key}={value}")
    if not details:
        return f"{rule_type} on {field}"
    return f"{rule_type} on {field} ({', '.join(details)})"


def _contract_summary(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except OSError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    fields = payload.get("fields", {})
    clauses = payload.get("clauses", [])
    downstream_consumers = payload.get("downstream_consumers", [])
    clause_preview = []
    for clause in clauses[:3] if isinstance(clauses, list) else []:
        if not isinstance(clause, dict):
            continue
        clause_preview.append(
            {
                "id": clause.get("id"),
                "category": clause.get("category"),
                "severity": str(clause.get("severity") or "unknown").upper(),
                "description": clause.get("description"),
                "rule_summary": _rule_summary(clause.get("rule")),
            }
        )

    ai_extensions_applied = False
    if isinstance(fields, dict):
        ai_extensions_applied = any(
            isinstance(definition, dict) and "llm_annotation" in definition
            for definition in fields.values()
        )

    return {
        "name": path.name,
        "path": _relative_path(path),
        "updated_at": _updated_at(path),
        "contract_id": payload.get("contract_id"),
        "title": payload.get("info", {}).get("title") if isinstance(payload.get("info"), dict) else payload.get("contract_id"),
        "dataset": payload.get("dataset"),
        "source_path": payload.get("source_path"),
        "field_count": len(fields) if isinstance(fields, dict) else 0,
        "clause_count": len(clauses) if isinstance(clauses, list) else 0,
        "downstream_count": len(downstream_consumers) if isinstance(downstream_consumers, list) else 0,
        "downstream_labels": [
            str(item.get("label") or item.get("id"))
            for item in downstream_consumers[:4]
            if isinstance(item, dict)
        ]
        if isinstance(downstream_consumers, list)
        else [],
        "ai_extensions_applied": ai_extensions_applied,
        "preview": _preview_text(path),
        "clause_preview": clause_preview,
    }


def _validation_report_summary(path: Path) -> dict[str, Any] | None:
    payload = read_json_file(path)
    if not isinstance(payload, dict):
        return None
    results = payload.get("results", [])
    schema_evolution = payload.get("schema_evolution", {})
    change_count = 0
    if isinstance(schema_evolution, dict):
        raw_changes = schema_evolution.get("changes", [])
        if isinstance(raw_changes, list):
            change_count = sum(1 for item in raw_changes if isinstance(item, dict))
    total_checks = int(payload.get("total_checks", 0) or 0)
    passed = int(payload.get("passed", 0) or 0)
    warned = int(payload.get("warned", 0) or 0)
    failed = int(payload.get("failed", 0) or 0)
    critical_violations = count_critical_violations(results if isinstance(results, list) else [])
    drift_score = min(100, (change_count * 15) + (failed * 4) + (warned * 2))
    return {
        "name": path.name,
        "path": _relative_path(path),
        "updated_at": _updated_at(path),
        "contract_id": payload.get("contract_id"),
        "dataset": payload.get("dataset"),
        "status": payload.get("overall_status", "UNKNOWN"),
        "total_checks": total_checks,
        "passed": passed,
        "warned": warned,
        "failed": failed,
        "health_score": compute_health_score(total_checks, passed, critical_violations),
        "drift_score": drift_score,
        "change_count": change_count,
        "result_count": len(results) if isinstance(results, list) else 0,
        "preview": _preview_text(path),
    }


def _schema_snapshot_summary(path: Path) -> dict[str, Any]:
    if path.is_file():
        return {
            "name": path.name,
            "path": _relative_path(path),
            "updated_at": _updated_at(path),
            "kind": "baseline",
            "preview": _preview_text(path),
            "snapshot_count": 1,
        }

    snapshots = sorted(path.glob("*.yaml"))
    latest = snapshots[-1] if snapshots else None
    return {
        "name": path.name,
        "path": _relative_path(path),
        "updated_at": _updated_at(latest or path),
        "kind": "history",
        "snapshot_count": len(snapshots),
        "latest_snapshot": _relative_path(latest) if latest else None,
        "preview": _preview_text(latest) if latest else "",
    }


def _violation_log_summary(path: Path) -> dict[str, Any]:
    records = read_jsonl_file(path)
    latest = records[0] if records else {}
    return {
        "name": path.name,
        "path": _relative_path(path),
        "updated_at": _updated_at(path),
        "record_count": len(records),
        "latest_detected_at": latest.get("detected_at") if isinstance(latest, dict) else None,
        "preview": _preview_text(path, max_lines=8),
    }


def _report_file_summary(path: Path) -> dict[str, Any]:
    payload = read_json_file(path) if path.suffix == ".json" else None
    return {
        "name": path.name,
        "path": _relative_path(path),
        "updated_at": _updated_at(path),
        "kind": path.suffix.lstrip(".") or "file",
        "generated_at": payload.get("generated_at") if isinstance(payload, dict) else None,
        "preview": _preview_text(path, max_lines=12),
    }


def get_artifact_catalog() -> dict[str, Any]:
    mode = load_current_run_mode()

    contract_files = sorted(GENERATED_CONTRACTS_DIR.glob("*.yaml"))
    contracts = [_contract_summary(path) for path in contract_files]

    validation_reports = []
    for path in sorted(VALIDATION_REPORTS_DIR.glob("*.json")):
        item = _validation_report_summary(path)
        if item is not None:
            validation_reports.append(item)
    validation_reports.sort(
        key=lambda item: parse_timestamp(item.get("updated_at")) or parse_timestamp("1970-01-01T00:00:00Z"),
        reverse=True,
    )

    scope_dir = SCHEMA_SNAPSHOTS_DIR / mode
    schema_snapshots = [_schema_snapshot_summary(path) for path in sorted(scope_dir.iterdir())] if scope_dir.exists() else []
    schema_snapshots.sort(
        key=lambda item: parse_timestamp(item.get("updated_at")) or parse_timestamp("1970-01-01T00:00:00Z"),
        reverse=True,
    )

    violation_logs = [_violation_log_summary(path) for path in sorted(VIOLATION_LOG_DIR.glob("*.jsonl"))]
    report_files = [_report_file_summary(path) for path in sorted(ENFORCER_REPORT_DIR.iterdir()) if path.is_file()]

    clause_previews = []
    for contract in contracts:
        for clause in contract.get("clause_preview", []):
            clause_previews.append(
                {
                    "contract_id": contract.get("contract_id"),
                    "contract_name": contract.get("title"),
                    **clause,
                }
            )

    return {
        "mode": mode,
        "generated_at": timestamp_to_iso(best_timestamp(None, VALIDATION_REPORTS_DIR / "run_summary.json")),
        "contracts": contracts,
        "validation_reports": validation_reports,
        "schema_snapshots": schema_snapshots[:12],
        "violation_logs": violation_logs,
        "report_files": report_files,
        "sample_contract_clauses": clause_previews[:10],
    }
