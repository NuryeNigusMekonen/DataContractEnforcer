from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.what_if import load_change_spec, simulate_what_if

from .common import CHANGE_SPECS_DIR, REPO_ROOT, VALIDATION_REPORTS_DIR, best_timestamp, latest_report_by_contract, load_latest_json, read_json_file, timestamp_to_iso
from .validation_service import VALIDATION_TARGETS


WHAT_IF_LATEST_PATH = VALIDATION_REPORTS_DIR / "what_if_latest.json"
DEFAULT_ADAPTER_CONFIG = REPO_ROOT / "contract_registry" / "adapters.yaml"
DEFAULT_LINEAGE_PATH = REPO_ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl"
DEFAULT_REGISTRY_PATH = REPO_ROOT / "contract_registry" / "subscriptions.yaml"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _target_for_contract(contract_id: str) -> dict[str, Any]:
    for target in VALIDATION_TARGETS.values():
        if target["contract_id"] == contract_id:
            return target
    raise KeyError(f"No validation target configured for contract {contract_id}")


def available_change_specs() -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for path in sorted(CHANGE_SPECS_DIR.glob("*.json")):
        payload = read_json_file(path)
        if not isinstance(payload, dict):
            continue
        options.append(
            {
                "id": path.name,
                "label": path.stem.replace("_", " ").replace("-", " ").title(),
                "path": str(path.relative_to(REPO_ROOT)),
                "contract_id": payload.get("contract_id"),
                "change_type": payload.get("change_type"),
                "field": payload.get("field"),
            }
        )
    return options


def run_what_if(change_spec_reference: str) -> dict[str, Any]:
    path = Path(change_spec_reference)
    spec_path = path if path.is_absolute() else REPO_ROOT / change_spec_reference
    if not spec_path.exists():
        spec_path = CHANGE_SPECS_DIR / change_spec_reference
    change_spec = load_change_spec(spec_path)
    contract_id = str(change_spec.get("contract_id", ""))
    target = _target_for_contract(contract_id)
    contract_path = next(candidate for candidate in target["contract_candidates"] if Path(candidate).exists())
    baseline_report = latest_report_by_contract().get(contract_id)
    report = simulate_what_if(
        contract_path=contract_path,
        data_path=target["data_path"],
        change_spec_path=spec_path,
        adapter_config=str(DEFAULT_ADAPTER_CONFIG) if DEFAULT_ADAPTER_CONFIG.exists() else None,
        lineage_path=str(DEFAULT_LINEAGE_PATH) if DEFAULT_LINEAGE_PATH.exists() else None,
        registry_path=str(DEFAULT_REGISTRY_PATH) if DEFAULT_REGISTRY_PATH.exists() else None,
        baseline_evaluation=baseline_report,
    )
    WHAT_IF_LATEST_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return _shape_what_if(report, WHAT_IF_LATEST_PATH)


def _shape_what_if(payload: dict[str, Any], path: Path | None) -> dict[str, Any]:
    raw_status = payload.get("raw_changed_status") or payload.get("status") or "UNKNOWN"
    adapter_attempted = bool(payload.get("adapter_attempted"))
    adapter_status = payload.get("adapter_status")
    if not adapter_status:
        adapter_status = raw_status if adapter_attempted else "NOT_ATTEMPTED"
    final_verdict = payload.get("compatibility_verdict") or (adapter_status if adapter_attempted else raw_status)
    adapter_details = _as_dict(payload.get("adapter_details"))
    baseline_summary = _as_dict(payload.get("baseline_summary"))
    raw_summary = _as_dict(payload.get("raw_changed_summary"))
    affected_subscribers = _as_list(payload.get("affected_subscribers"))
    transitive_impacts = _as_list(payload.get("transitive_impacts"))
    affected_systems = _dedupe_strings([
        *[
            subscriber.get("subscriber_id") or subscriber.get("id")
            for subscriber in affected_subscribers
            if isinstance(subscriber, dict)
        ],
        *[
            impact.get("id")
            for impact in transitive_impacts
            if isinstance(impact, dict) and str(impact.get("kind", "")).upper() in {"SERVICE", "SUBSCRIBER", "CONTRACT"}
        ],
    ])
    return {
        "simulation_id": payload.get("simulation_id"),
        "contract_id": payload.get("contract_id"),
        "proposed_change": payload.get("proposed_change"),
        "baseline_status": payload.get("baseline_status"),
        "baseline_summary": baseline_summary,
        "raw_status": raw_status,
        "raw_summary": raw_summary,
        "adapter_status": adapter_status,
        "adapter_attempted": adapter_attempted,
        "adapter_details": adapter_details,
        "adapter_summary": {
            "status": adapter_status,
            "rules_applied": len(adapter_details.get("rules_applied", [])),
            "failure_reason": adapter_details.get("failure_reason"),
            "recovered": bool(adapter_details.get("succeeded")),
        },
        "compatibility_verdict": payload.get("compatibility_verdict", final_verdict),
        "final_verdict": final_verdict,
        "affected_systems": affected_systems,
        "affected_systems_count": len(affected_systems),
        "recommendation": payload.get("recommended_action"),
        "available_specs": available_change_specs(),
        "last_updated": timestamp_to_iso(best_timestamp(payload, path)),
    }


def get_what_if() -> dict[str, Any]:
    payload, path = load_latest_json(VALIDATION_REPORTS_DIR, "what_if*.json")
    if not isinstance(payload, dict):
        return {
            "proposed_change": None,
            "raw_status": "UNKNOWN",
            "adapter_status": "UNKNOWN",
            "compatibility_verdict": "UNKNOWN",
            "final_verdict": "UNKNOWN",
            "affected_systems": [],
            "recommendation": None,
            "available_specs": available_change_specs(),
            "last_updated": None,
        }
    return _shape_what_if(payload, path)
