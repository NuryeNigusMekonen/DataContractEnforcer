from __future__ import annotations

from typing import Any

from .common import VALIDATION_REPORTS_DIR, best_timestamp, latest_report_by_contract, load_latest_json, timestamp_to_iso


def _action_required(default_recommendation: Any, verdict: str | None) -> str:
    if isinstance(default_recommendation, dict):
        recommendation = default_recommendation.get("recommended_action")
        if recommendation:
            return str(recommendation)
    if isinstance(default_recommendation, list):
        first_item = next((item for item in default_recommendation if item), None)
        if first_item:
            return str(first_item)
    if default_recommendation:
        return str(default_recommendation)
    if str(verdict or "").upper() == "BREAKING":
        return "Coordinate a migration or compatibility bridge before release."
    return "No immediate action required."


def _normalize_change(
    *,
    contract_id: str | None,
    field_name: str | None,
    change_type: str | None,
    compatibility_verdict: str | None,
    migration_recommendation: str | None,
    rationale: str | None,
) -> dict[str, Any]:
    compatibility = str(compatibility_verdict or "UNKNOWN").upper()
    return {
        "contract_name": contract_id,
        "contract_id": contract_id,
        "field_name": field_name,
        "change": change_type or rationale or "Change detected",
        "compatibility": compatibility,
        "compatibility_verdict": compatibility,
        "action_required": _action_required(migration_recommendation, compatibility),
        "rationale": rationale,
    }


def get_schema_evolution() -> dict[str, Any]:
    payload, path = load_latest_json(VALIDATION_REPORTS_DIR, "schema_evolution*.json")
    if not isinstance(payload, dict):
        live_report = next(
            (
                report for report in latest_report_by_contract().values()
                if isinstance(report.get("schema_evolution"), dict) and report.get("schema_evolution")
            ),
            None,
        )
        if not live_report:
            return {
                "contract_id": None,
                "change_type": None,
                "compatibility_verdict": "UNKNOWN",
                "migration_recommendation": [],
                "changes": [],
                "items": [],
                "last_updated": None,
            }
        schema_evolution = live_report.get("schema_evolution", {})
        recommendation = schema_evolution.get("notification")
        changes = [
            _normalize_change(
                contract_id=live_report.get("contract_id"),
                field_name=change.get("field_name"),
                change_type=change.get("change_type"),
                compatibility_verdict="BREAKING" if change.get("change_type") != "NO_CHANGE" else "COMPATIBLE",
                migration_recommendation=recommendation,
                rationale=change.get("rationale"),
            )
            for change in schema_evolution.get("changes", [])
            if isinstance(change, dict)
        ]
        return {
            "contract_id": live_report.get("contract_id"),
            "change_type": schema_evolution.get("primary_breaking_change", {}).get("change_type"),
            "compatibility_verdict": schema_evolution.get("compatibility_classification", "UNKNOWN"),
            "migration_recommendation": [recommendation] if recommendation else [],
            "changes": changes,
            "items": changes,
            "last_updated": live_report.get("_timestamp"),
        }

    changes = []
    migration_checklist = payload.get("migration_checklist", [])
    default_recommendation = migration_checklist[0] if migration_checklist else "Review downstream consumers before releasing the schema."

    for change in payload.get("changes", []):
        if not isinstance(change, dict):
            continue
        classification = change.get("classification", "UNKNOWN")
        changes.append(
            _normalize_change(
                contract_id=payload.get("contract_id"),
                field_name=change.get("field_name"),
                change_type=classification,
                compatibility_verdict=classification,
                migration_recommendation=default_recommendation,
                rationale=change.get("rationale"),
            )
        )

    changes.sort(key=lambda item: 1 if item.get("compatibility") == "BREAKING" else 0, reverse=True)
    primary_change_type = next(
        (change["change"] for change in changes if change["compatibility"] == "BREAKING"),
        changes[0]["change"] if changes else None,
    )
    return {
        "contract_id": payload.get("contract_id"),
        "change_type": primary_change_type,
        "compatibility_verdict": payload.get("compatibility_verdict", "UNKNOWN"),
        "migration_recommendation": migration_checklist,
        "old_snapshot": payload.get("old_snapshot"),
        "new_snapshot": payload.get("new_snapshot"),
        "changes": changes,
        "items": changes,
        "last_updated": timestamp_to_iso(best_timestamp(payload, path)),
    }
