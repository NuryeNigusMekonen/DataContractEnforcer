from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .common import (
    derive_week_label,
    latest_report_by_contract,
    load_violations,
    parse_timestamp,
    timestamp_to_iso,
)


SEVERITY_PRIORITY = {
    "CRITICAL": 4,
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
    "UNKNOWN": 0,
}


def _severity_score(value: str | None) -> int:
    return SEVERITY_PRIORITY.get(str(value or "UNKNOWN").upper(), 0)


def _recommended_action(severity: str | None, affected_systems: int, status: str | None) -> str:
    normalized_severity = str(severity or "UNKNOWN").upper()
    normalized_status = str(status or "UNKNOWN").upper()
    if normalized_severity in {"CRITICAL", "HIGH"} or normalized_status == "ERROR":
        if affected_systems > 1:
            return "Contain the producer change, alert downstream owners, and validate a rollback or adapter path."
        return "Pause the producer rollout and inspect the failing contract check before the next validation cycle."
    if normalized_severity == "MEDIUM":
        return "Review the producer output and refresh baselines only if this change is intentional."
    return "Track the issue and verify it during the next scheduled validation run."


def _short_message(message: Any, limit: int = 110) -> str:
    text = str(message or "No message available.").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}…"


def _find_matching_result(violation: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    reports = latest_report_by_contract()
    search_space = list(reports.values())

    contract_path = str(violation.get("contract_path", ""))
    for report in search_space:
        if contract_path and str(report.get("contract_id", "")) in contract_path:
            for result in report.get("results", []):
                if result.get("check_id") == violation.get("check_id"):
                    return result, report

    field_name = violation.get("field_name")
    check_id = violation.get("check_id")
    for report in search_space:
        for result in report.get("results", []):
            if result.get("check_id") == check_id:
                return result, report
            if field_name and result.get("column_name") == field_name and result.get("status") in {"FAIL", "WARN"}:
                return result, report
    return None, None


def _shape_violation(raw_violation: dict[str, Any]) -> dict[str, Any]:
    result, report = _find_matching_result(raw_violation)
    result = result or {}
    blast_radius = raw_violation.get("blast_radius", {}) if isinstance(raw_violation.get("blast_radius"), dict) else {}
    source_hint = (
        report.get("contract_id")
        if isinstance(report, dict)
        else raw_violation.get("contract_path") or raw_violation.get("check_id") or raw_violation.get("field_name")
    )
    sample_records = raw_violation.get("samples", [])
    expected = result.get("expected") or raw_violation.get("message") or "See contract definition."
    actual = result.get("actual_value")
    if actual is None:
        actual = sample_records[:3] if sample_records else f"{raw_violation.get('records_failing', 0)} failing records"

    affected_systems = [str(item) for item in blast_radius.get("affected_subscribers", []) if item]
    severity = raw_violation.get("severity", "UNKNOWN")
    status = raw_violation.get("status", "WARN")
    return {
        "violation_id": raw_violation.get("violation_id"),
        "check_id": raw_violation.get("check_id"),
        "field": raw_violation.get("field_name"),
        "severity": severity,
        "status": status,
        "message": raw_violation.get("message", "No message available."),
        "short_message": _short_message(raw_violation.get("message")),
        "sample_records": sample_records[:5],
        "records_failing": raw_violation.get("records_failing", 0),
        "expected": expected,
        "actual": actual,
        "source_week": derive_week_label(str(source_hint)),
        "week": derive_week_label(str(source_hint)),
        "detected_at": raw_violation.get("detected_at"),
        "affected_subscribers": affected_systems,
        "affected_systems": affected_systems,
        "affected_subscriber_count": len(affected_systems),
        "affected_systems_count": len(affected_systems),
        "estimated_records": int(blast_radius.get("estimated_records", 0) or 0),
        "recommended_action": _recommended_action(severity, len(affected_systems), status),
    }


def get_violations(limit: int | None = None, severity: str | None = None, search: str | None = None) -> list[dict[str, Any]]:
    violations = []
    for raw_violation in load_violations():
        violations.append(_shape_violation(raw_violation))

    normalized_severity = str(severity or "").upper().strip()
    if normalized_severity and normalized_severity != "ALL":
        violations = [item for item in violations if str(item.get("severity", "")).upper() == normalized_severity]

    normalized_search = str(search or "").strip().lower()
    if normalized_search:
        violations = [
            item
            for item in violations
            if normalized_search in str(item.get("field", "")).lower()
            or normalized_search in str(item.get("message", "")).lower()
            or normalized_search in str(item.get("week", "")).lower()
        ]

    violations.sort(
        key=lambda item: (
            _severity_score(item.get("severity")),
            int(item.get("affected_systems_count", 0) or 0),
            int(item.get("records_failing", 0) or 0),
            parse_timestamp(item.get("detected_at")) or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )

    if limit is not None:
        return violations[:limit]

    return violations


def get_top_incident() -> dict[str, Any]:
    violations = get_violations()
    incident = violations[0] if violations else None
    if not incident:
        return {
            "violation_id": None,
            "field": "No active incident",
            "severity": "PASS",
            "short_message": "All monitored contracts are currently stable.",
            "message": "No active violations require immediate action.",
            "affected_systems": [],
            "affected_systems_count": 0,
            "recommended_action": "Continue monitoring live validations.",
            "week": None,
            "detected_at": None,
        }
    return incident


def get_blame_chains(limit: int | None = None) -> list[dict[str, Any]]:
    deduped: dict[tuple[str | None, str | None], dict[str, Any]] = {}
    for violation in load_violations():
        for entry in violation.get("blame_chain", []):
            if not isinstance(entry, dict):
                continue
            key = (entry.get("file_path"), entry.get("commit_hash"))
            current = deduped.get(key)
            candidate = {
                "violation_id": violation.get("violation_id"),
                "field": violation.get("field_name"),
                "severity": violation.get("severity"),
                "file_path": entry.get("file_path"),
                "commit_hash": entry.get("commit_hash"),
                "author": entry.get("author"),
                "message": entry.get("commit_message"),
                "confidence_score": entry.get("confidence_score"),
                "rank": entry.get("rank"),
                "commit_timestamp": entry.get("commit_timestamp"),
                "detected_at": violation.get("detected_at"),
                "impact_count": 1,
                "affected_fields": [violation.get("field_name")],
            }
            if current is None:
                deduped[key] = candidate
                continue

            current["impact_count"] = int(current.get("impact_count", 1)) + 1
            fields = {field for field in current.get("affected_fields", []) if field}
            if violation.get("field_name"):
                fields.add(violation.get("field_name"))
            current["affected_fields"] = sorted(fields)

            current_confidence = float(current.get("confidence_score") or 0)
            candidate_confidence = float(candidate.get("confidence_score") or 0)
            if candidate_confidence > current_confidence:
                current.update(
                    {
                        "violation_id": candidate["violation_id"],
                        "field": candidate["field"],
                        "severity": candidate["severity"],
                        "author": candidate["author"],
                        "message": candidate["message"],
                        "confidence_score": candidate["confidence_score"],
                        "rank": candidate["rank"],
                        "commit_timestamp": candidate["commit_timestamp"],
                        "detected_at": candidate["detected_at"],
                    }
                )

    rows = list(deduped.values())
    rows.sort(
        key=lambda row: (
            float(row.get("confidence_score") or 0),
            parse_timestamp(row.get("detected_at")) or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    if limit is not None:
        return rows[:limit]
    return rows


def get_blame_top(limit: int = 3) -> dict[str, Any]:
    rows = get_blame_chains()
    items = []
    for entry in rows[:limit]:
        items.append(
            {
                **entry,
                "file": entry.get("file_path"),
                "commit": str(entry.get("commit_hash") or "")[:10],
                "confidence": round(float(entry.get("confidence_score") or 0) * 100),
            }
        )
    return {
        "items": items,
        "total_count": len(rows),
        "has_more": len(rows) > limit,
    }


def get_blast_radius() -> dict[str, Any]:
    violations = load_violations()
    by_field: dict[str, dict[str, Any]] = {}
    affected_subscribers: set[str] = set()
    all_nodes: set[str] = set()
    max_depth = 0
    estimated_records = 0
    last_updated = None

    for violation in violations:
        payload = violation.get("blast_radius", {})
        if not isinstance(payload, dict):
            continue

        lineage = payload.get("lineage", [])
        depth = max((int(node.get("hops", 0) or 0) for node in lineage if isinstance(node, dict)), default=0)
        subscribers = payload.get("affected_subscribers", [])
        affected_subscribers.update(str(item) for item in subscribers)
        all_nodes.update(str(item) for item in payload.get("affected_nodes", []))
        estimated_records += int(payload.get("estimated_records", 0) or 0)
        last_updated = max(
            [stamp for stamp in (last_updated, parse_timestamp(violation.get("detected_at"))) if stamp is not None],
            default=last_updated,
        )
        max_depth = max(max_depth, depth)

        field = str(violation.get("field_name") or "unknown_field")
        current = by_field.get(field)
        if current is None:
            by_field[field] = {
                "field": violation.get("field_name"),
                "affected_subscribers": list(subscribers),
                "affected_nodes": list(payload.get("affected_nodes", [])),
                "affected_contracts": list(payload.get("affected_contracts", [])),
                "contamination_depth": depth,
                "estimated_records": int(payload.get("estimated_records", 0) or 0),
                "violation_count": 1,
            }
            continue

        current["contamination_depth"] = max(int(current.get("contamination_depth", 0) or 0), depth)
        current["estimated_records"] = int(current.get("estimated_records", 0) or 0) + int(payload.get("estimated_records", 0) or 0)
        current["violation_count"] = int(current.get("violation_count", 1)) + 1
        current["affected_subscribers"] = sorted({*current.get("affected_subscribers", []), *subscribers})
        current["affected_nodes"] = sorted({*current.get("affected_nodes", []), *payload.get("affected_nodes", [])})
        current["affected_contracts"] = sorted({*current.get("affected_contracts", []), *payload.get("affected_contracts", [])})

    return {
        "affected_subscribers": sorted(affected_subscribers),
        "contamination_depth": max_depth,
        "affected_nodes": sorted(all_nodes),
        "estimated_records": estimated_records,
        "last_updated": timestamp_to_iso(last_updated),
        "violations": sorted(
            by_field.values(),
            key=lambda item: (
                int(item.get("contamination_depth", 0) or 0),
                int(item.get("estimated_records", 0) or 0),
            ),
            reverse=True,
        ),
    }


def get_blast_radius_summary(limit: int = 5) -> dict[str, Any]:
    blast_radius = get_blast_radius()
    all_fields = [
        {
            **entry,
            "affected_systems_count": len(entry.get("affected_subscribers", [])),
        }
        for entry in blast_radius.get("violations", [])
    ]
    return {
        "affected_systems_count": len(blast_radius.get("affected_subscribers", [])),
        "max_depth": blast_radius.get("contamination_depth", 0),
        "estimated_impacted_records": blast_radius.get("estimated_records", 0),
        "last_updated": blast_radius.get("last_updated"),
        "top_fields": all_fields[:limit],
        "all_fields": all_fields,
        "has_more": len(all_fields) > limit,
    }
