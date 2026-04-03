from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.attributor import attribute_failure, contract_id_from_args_or_report
from contracts.common import load_jsonl, sha256_file, utc_now
from contracts.runner import evaluate_contract_records
from scripts.generate_all_outputs import generate_all_outputs_from_scenario_path

from .common import (
    OUTPUTS_DIR,
    REPO_ROOT,
    SCENARIOS_DIR,
    TRACE_CONTRACT,
    VALIDATION_REPORTS_DIR,
    VIOLATION_LOG_DIR,
    derive_week_label,
    latest_report_by_contract,
    parse_timestamp,
    read_json_file,
)


VALIDATION_TARGETS: dict[str, dict[str, Any]] = {
    "week1": {
        "key": "week1",
        "label": "Week 1",
        "contract_id": "week1-intent-records",
        "contract_candidates": [
            REPO_ROOT / "generated_contracts" / "week1-intent-records.yaml",
            REPO_ROOT / "generated_contracts" / "week1_intent_records.yaml",
        ],
        "data_path": OUTPUTS_DIR / "week1" / "intent_records.jsonl",
        "report_path": VALIDATION_REPORTS_DIR / "live_week1.json",
    },
    "week2": {
        "key": "week2",
        "label": "Week 2",
        "contract_id": "week2-verdict-records",
        "contract_candidates": [
            REPO_ROOT / "generated_contracts" / "week2-verdict-records.yaml",
            REPO_ROOT / "generated_contracts" / "week2_verdicts.yaml",
        ],
        "data_path": OUTPUTS_DIR / "week2" / "verdicts.jsonl",
        "report_path": VALIDATION_REPORTS_DIR / "live_week2.json",
    },
    "week3": {
        "key": "week3",
        "label": "Week 3",
        "contract_id": "week3-document-refinery-extractions",
        "contract_candidates": [
            REPO_ROOT / "generated_contracts" / "week3-document-refinery-extractions.yaml",
            REPO_ROOT / "generated_contracts" / "week3_extractions.yaml",
        ],
        "data_path": OUTPUTS_DIR / "week3" / "extractions.jsonl",
        "report_path": VALIDATION_REPORTS_DIR / "live_week3.json",
    },
    "week4": {
        "key": "week4",
        "label": "Week 4",
        "contract_id": "week4-lineage-snapshots",
        "contract_candidates": [
            REPO_ROOT / "generated_contracts" / "week4-lineage-snapshots.yaml",
            REPO_ROOT / "generated_contracts" / "week4_lineage.yaml",
        ],
        "data_path": OUTPUTS_DIR / "week4" / "lineage_snapshots.jsonl",
        "report_path": VALIDATION_REPORTS_DIR / "live_week4.json",
    },
    "week5": {
        "key": "week5",
        "label": "Week 5",
        "contract_id": "week5-event-records",
        "contract_candidates": [
            REPO_ROOT / "generated_contracts" / "week5-event-records.yaml",
            REPO_ROOT / "generated_contracts" / "week5_events.yaml",
        ],
        "data_path": OUTPUTS_DIR / "week5" / "events.jsonl",
        "report_path": VALIDATION_REPORTS_DIR / "live_week5.json",
    },
    "traces": {
        "key": "traces",
        "label": "Traces",
        "contract_id": TRACE_CONTRACT["contract_id"],
        "contract_candidates": [
            REPO_ROOT / "generated_contracts" / "langsmith-trace-records.yaml",
            REPO_ROOT / "generated_contracts" / "langsmith_traces.yaml",
        ],
        "data_path": OUTPUTS_DIR / "traces" / "runs.jsonl",
        "report_path": VALIDATION_REPORTS_DIR / "live_traces.json",
    },
}

DEFAULT_LINEAGE_PATH = OUTPUTS_DIR / "week4" / "lineage_snapshots.jsonl"
DEFAULT_REGISTRY_PATH = REPO_ROOT / "contract_registry" / "subscriptions.yaml"
LIVE_VIOLATIONS_PATH = VIOLATION_LOG_DIR / "live_violations.jsonl"


def _resolve_existing_path(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"None of the candidate paths exist: {', '.join(str(path) for path in candidates)}")


def get_validation_target(week_key: str) -> dict[str, Any]:
    if week_key not in VALIDATION_TARGETS:
        raise KeyError(f"Unknown validation target: {week_key}")
    return VALIDATION_TARGETS[week_key]


def get_all_validation_targets() -> list[dict[str, Any]]:
    return [VALIDATION_TARGETS[key] for key in ("week1", "week2", "week3", "week4", "week5", "traces")]


def _load_contract(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Contract at {path} is not a mapping.")
    return payload


def _build_report(*, contract: dict[str, Any], data_path: Path, report_path: Path) -> dict[str, Any]:
    records = load_jsonl(data_path)
    evaluation = evaluate_contract_records(
        contract,
        records,
        mode="ENFORCE",
        data_path=str(data_path.relative_to(REPO_ROOT)),
        attempt_adapter=True,
        persist_baselines=True,
    )
    report = {
        "report_id": str(report_path.stem),
        "snapshot_id": sha256_file(data_path),
        "run_timestamp": utc_now(),
        "generated_at": utc_now(),
        "mode": evaluation["mode"],
        "blocking": evaluation["blocking"],
        "overall_status": evaluation["overall_status"],
        "contract_id": contract.get("contract_id"),
        "dataset": contract.get("dataset"),
        "expected_contract_version": evaluation["expected_contract_version"],
        "data_path": str(data_path.relative_to(REPO_ROOT)),
        "record_count": evaluation["record_count"],
        "raw_record_count": evaluation["raw_record_count"],
        "total_checks": evaluation["total_checks"],
        "passed": evaluation["passed"],
        "failed": evaluation["failed"],
        "warned": evaluation["warned"],
        "errored": evaluation["errored"],
        "summary": evaluation["summary"],
        "schema_evolution": evaluation["schema_evolution"],
        "adapter": evaluation["adapter"],
        "results": evaluation["results"],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def validate_week(week_key: str) -> dict[str, Any]:
    target = get_validation_target(week_key)
    contract_path = _resolve_existing_path(target["contract_candidates"])
    data_path = Path(target["data_path"])
    report_path = Path(target["report_path"])
    contract = _load_contract(contract_path)
    return _build_report(contract=contract, data_path=data_path, report_path=report_path)


def refresh_live_violations(reports: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if reports is None:
        reports = []
        for target in get_all_validation_targets():
            payload = read_json_file(Path(target["report_path"]))
            if isinstance(payload, dict):
                reports.append(payload)

    lineage_records = load_jsonl(DEFAULT_LINEAGE_PATH) if DEFAULT_LINEAGE_PATH.exists() else []
    lineage_snapshot = lineage_records[-1] if lineage_records else {}
    registry_path = str(DEFAULT_REGISTRY_PATH) if DEFAULT_REGISTRY_PATH.exists() else None

    attributed: list[dict[str, Any]] = []
    for target in get_all_validation_targets():
        report_path = Path(target["report_path"])
        report = next(
            (
                candidate
                for candidate in reports
                if isinstance(candidate, dict) and candidate.get("report_id") == report_path.stem
            ),
            None,
        )
        if not isinstance(report, dict):
            continue

        failures = [result for result in report.get("results", []) if result.get("status") in {"FAIL", "ERROR"}]
        if not failures:
            continue

        contract_path = _resolve_existing_path(target["contract_candidates"])
        contract_id = contract_id_from_args_or_report(report, str(contract_path))
        for failure in failures:
            attributed.append(
                attribute_failure(
                    failure,
                    lineage_snapshot,
                    contract_id,
                    str(contract_path),
                    registry_path,
                    report,
                    "14 days ago",
                )
            )

    LIVE_VIOLATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LIVE_VIOLATIONS_PATH.open("w", encoding="utf-8") as handle:
        for record in attributed:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    return attributed


def run_validation_batch(week_keys: list[str]) -> dict[str, Any]:
    reports: list[dict[str, Any]] = []
    for week_key in week_keys:
        reports.append(validate_week(week_key))
    violations = refresh_live_violations()
    return {
        "validated_weeks": week_keys,
        "reports": reports,
        "violation_count": len(violations),
        "completed_at": utc_now(),
    }


def validate_all_weeks() -> dict[str, Any]:
    return run_validation_batch([target["key"] for target in get_all_validation_targets()])


def regenerate_outputs(scenario_path: str, *, clear_existing: bool = False) -> dict[str, Any]:
    return generate_all_outputs_from_scenario_path(scenario_path, clear_existing=clear_existing)


def available_scenarios() -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    for path in sorted(SCENARIOS_DIR.glob("*.yaml")):
        options.append(
            {
                "id": path.name,
                "label": path.stem.replace("_", " ").replace("-", " ").title(),
                "path": str(path.relative_to(REPO_ROOT)),
            }
        )
    return options


def get_latest_validations() -> list[dict[str, Any]]:
    reports = latest_report_by_contract().values()
    rows: list[dict[str, Any]] = []
    for report in reports:
        rows.append(
            {
                "week_name": derive_week_label(report.get("contract_id")),
                "contract_id": report.get("contract_id"),
                "status": report.get("overall_status"),
                "total_checks": report.get("total_checks"),
                "passed": report.get("passed"),
                "failed": report.get("failed"),
                "warned": report.get("warned"),
                "last_updated": report.get("_timestamp"),
                "source": report.get("_source_path"),
            }
        )
    rows.sort(
        key=lambda row: parse_timestamp(row.get("last_updated")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return rows
