from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .common import (
    derive_week_label,
    load_enforcer_report,
    load_latest_json,
    load_validation_reports,
    parse_timestamp,
    timestamp_to_iso,
    VALIDATION_REPORTS_DIR,
)
from .violation_service import get_violations


def _timeline_row(
    *,
    item_id: str,
    timestamp: str | None,
    category: str,
    title: str,
    status: str,
    details: str,
    source: str | None = None,
) -> dict[str, Any]:
    return {
        "id": item_id,
        "timestamp": timestamp,
        "category": category,
        "title": title,
        "status": status,
        "details": details,
        "source": source,
    }


def get_timeline(limit: int = 15) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen_validation_ids: set[str] = set()

    for report in load_validation_reports():
        report_id = str(report.get("report_id") or report.get("_source_path") or "validation")
        if report_id in seen_validation_ids:
            continue
        seen_validation_ids.add(report_id)
        week_label = derive_week_label(report.get("contract_id"))
        items.append(
            _timeline_row(
                item_id=report_id,
                timestamp=report.get("_timestamp"),
                category="validation",
                title=f"{week_label} validation run",
                status=report.get("overall_status", "UNKNOWN"),
                details=f"{report.get('passed', 0)}/{report.get('total_checks', 0)} checks passed",
                source=report.get("_source_path"),
            )
        )

    for violation in get_violations():
        items.append(
            _timeline_row(
                item_id=violation.get("violation_id", "violation"),
                timestamp=violation.get("detected_at"),
                category="violation",
                title=f"{violation.get('source_week', 'Unknown')} violation: {violation.get('field', 'unknown field')}",
                status=violation.get("severity", "UNKNOWN"),
                details=violation.get("message", "Violation detected."),
                source=violation.get("field"),
            )
        )

    schema_payload, _ = load_latest_json(VALIDATION_REPORTS_DIR, "schema_evolution*.json")
    if isinstance(schema_payload, dict):
        items.append(
            _timeline_row(
                item_id=f"schema-{schema_payload.get('contract_id', 'unknown')}",
                timestamp=timestamp_to_iso(parse_timestamp(schema_payload.get("generated_at"))),
                category="schema",
                title="Schema evolution detected",
                status=schema_payload.get("compatibility_verdict", "UNKNOWN"),
                details=f"{schema_payload.get('contract_id')}: {schema_payload.get('compatibility_verdict', 'UNKNOWN')}",
                source=schema_payload.get("new_snapshot"),
            )
        )

    what_if_payload, _ = load_latest_json(VALIDATION_REPORTS_DIR, "what_if*.json")
    if isinstance(what_if_payload, dict):
        final_status = what_if_payload.get("adapter_status") if what_if_payload.get("adapter_attempted") else what_if_payload.get("raw_changed_status")
        items.append(
            _timeline_row(
                item_id=what_if_payload.get("simulation_id", "what-if"),
                timestamp=timestamp_to_iso(parse_timestamp(what_if_payload.get("run_timestamp"))),
                category="what-if",
                title="What-if simulation evaluated",
                status=final_status or "UNKNOWN",
                details=(
                    f"{what_if_payload.get('proposed_change', {}).get('field', 'field')}: "
                    f"{what_if_payload.get('proposed_change', {}).get('from', 'unknown')} -> "
                    f"{what_if_payload.get('proposed_change', {}).get('to', 'unknown')}"
                ),
                source=what_if_payload.get("contract_id"),
            )
        )

    enforcer_report = load_enforcer_report()
    if isinstance(enforcer_report, dict):
        items.append(
            _timeline_row(
                item_id="enforcer-report",
                timestamp=timestamp_to_iso(parse_timestamp(enforcer_report.get("generated_at"))),
                category="summary",
                title="Weekly enforcer summary refreshed",
                status=str(enforcer_report.get("data_health_score", "UNKNOWN")),
                details=enforcer_report.get("health_narrative", "Dashboard summary refreshed."),
                source="enforcer_report/report_data.json",
            )
        )

    items.sort(
        key=lambda item: parse_timestamp(item.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return items[:limit]


def get_timeline_panel(limit: int = 8) -> dict[str, Any]:
    full_timeline = get_timeline(limit=50)
    items = []
    for entry in full_timeline[:limit]:
        items.append(
            {
                **entry,
                "time": entry.get("timestamp"),
                "severity": entry.get("status"),
                "short_message": entry.get("title"),
            }
        )
    return {
        "items": items,
        "total_count": len(full_timeline),
        "has_more": len(full_timeline) > limit,
    }
