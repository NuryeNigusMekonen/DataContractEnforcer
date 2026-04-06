from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.validation_service import refresh_live_violations, validate_all_weeks
from contracts.ai_extensions import build_ai_extension_report
from contracts.common import load_jsonl, schema_snapshot_scope, utc_now
from contracts.generator import build_contract, write_contract_files
from contracts.report_generator import build_pdf_bytes, generate_report, wrap_lines
from contracts.what_if import simulate_what_if
from create_violation import inject_violations_from_outputs
from scripts.sync_real_week_artifacts import main as sync_real_outputs_main


VALIDATION_REPORTS_DIR = ROOT / "validation_reports"
VIOLATION_LOG_DIR = ROOT / "violation_log"
ENFORCER_REPORT_DIR = ROOT / "enforcer_report"
OUTPUTS_DIR = ROOT / "outputs"
REGISTRY_PATH = ROOT / "contract_registry" / "subscriptions.yaml"
LINEAGE_PATH = OUTPUTS_DIR / "week4" / "lineage_snapshots.jsonl"
ADAPTER_PATH = ROOT / "contract_registry" / "adapters.yaml"
CHANGE_SPECS_DIR = ROOT / "test_data" / "changes"

CONTRACT_TARGETS = [
    {
        "contract_id": "week1-intent-records",
        "source": OUTPUTS_DIR / "week1" / "intent_records.jsonl",
    },
    {
        "contract_id": "week2-verdict-records",
        "source": OUTPUTS_DIR / "week2" / "verdicts.jsonl",
    },
    {
        "contract_id": "week3-document-refinery-extractions",
        "source": OUTPUTS_DIR / "week3" / "extractions.jsonl",
    },
    {
        "contract_id": "week4-lineage-snapshots",
        "source": OUTPUTS_DIR / "week4" / "lineage_snapshots.jsonl",
    },
    {
        "contract_id": "week5-event-records",
        "source": OUTPUTS_DIR / "week5" / "events.jsonl",
    },
    {
        "contract_id": "langsmith-trace-records",
        "source": OUTPUTS_DIR / "traces" / "runs.jsonl",
    },
]

DATASETS_FOR_AI = {
    "real": {
        "extractions": OUTPUTS_DIR / "week3" / "extractions.jsonl",
        "verdicts": OUTPUTS_DIR / "week2" / "verdicts.jsonl",
        "traces": OUTPUTS_DIR / "traces" / "runs.jsonl",
    },
    "violated": {
        "extractions": OUTPUTS_DIR / "week3" / "extractions_violated.jsonl",
        "verdicts": OUTPUTS_DIR / "week2" / "verdicts_violated.jsonl",
        "traces": OUTPUTS_DIR / "traces" / "runs_violated.jsonl",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Week 7 end-to-end for either real outputs or violated outputs."
    )
    parser.add_argument(
        "--mode",
        choices=["real", "violated"],
        required=True,
        help="Which end-to-end path to run.",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip syncing real week artifacts into outputs/ before running.",
    )
    parser.add_argument(
        "--skip-contracts",
        action="store_true",
        help="Skip contract generation step.",
    )
    parser.add_argument(
        "--skip-what-if",
        action="store_true",
        help="Skip what-if simulations during the e2e run.",
    )
    return parser.parse_args()


def ensure_runtime_dirs() -> None:
    VALIDATION_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    VIOLATION_LOG_DIR.mkdir(parents=True, exist_ok=True)
    ENFORCER_REPORT_DIR.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_report_pdf(report: dict[str, Any], output_path: Path) -> None:
    top_violations = report.get("top_violations", [])
    schema_changes = report.get("schema_changes_detected", [])
    what_if_changes = report.get("what_if_simulations", [])
    recommendations = report.get("recommendations", [])
    ai_payload = report.get("ai_system_risk_assessment", {})

    violation_lines = [f"- {item}" for item in top_violations] if top_violations else ["- None"]
    schema_lines = [f"- {item}" for item in schema_changes[:5]] if schema_changes else ["- None"]
    what_if_lines = [f"- {item}" for item in what_if_changes[:5]] if what_if_changes else ["- None"]
    recommendation_lines = [f"- {item}" for item in recommendations] if recommendations else ["- None"]
    ai_lines = [json.dumps(ai_payload, sort_keys=True)] if ai_payload else ["None"]

    pdf_lines = [
        "Data Contract Enforcer Report",
        f"Generated at: {report.get('generated_at', utc_now())}",
        (
            f"Producer Contract Health Score: {report.get('producer_contract_health_score', 'n/a')} - "
            f"{report.get('producer_contract_health_narrative', '')}"
        ),
        "",
        "Violations This Week:",
        *violation_lines,
        "",
        "Schema Changes Detected:",
        *schema_lines,
        "",
        "What-If Simulations:",
        *what_if_lines,
        "",
        "AI System Risk Assessment:",
        *ai_lines,
        "",
        "Recommended Actions:",
        *recommendation_lines,
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(build_pdf_bytes(wrap_lines(pdf_lines)))


def _violated_variant(path: Path) -> Path:
    return path.with_name(f"{path.stem}_violated{path.suffix}")


def _contract_path(contract_id: str) -> Path:
    candidate = ROOT / "generated_contracts" / f"{contract_id}.yaml"
    if candidate.exists():
        return candidate
    fallback = ROOT / "generated_contracts" / f"{contract_id}.yml"
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"Missing generated contract for what-if: {contract_id}")


def run_what_if_checks(
    label: str,
    *,
    prefer_violated: bool,
    validation_reports: list[dict[str, Any]],
) -> list[str]:
    spec_paths = sorted(CHANGE_SPECS_DIR.glob("*.json"))
    if not spec_paths:
        return []
    targets_by_contract = {item["contract_id"]: item for item in CONTRACT_TARGETS}
    baseline_by_contract = {
        str(report.get("contract_id")): report
        for report in validation_reports
        if isinstance(report, dict) and report.get("contract_id")
    }
    snapshots: list[str] = []
    latest_report: dict[str, Any] | None = None
    for spec_path in spec_paths:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        contract_id = str(spec.get("contract_id", "")).strip()
        target = targets_by_contract.get(contract_id)
        if not target:
            continue
        contract_path = _contract_path(contract_id)
        data_path = Path(target["source"])
        if prefer_violated:
            candidate = _violated_variant(data_path)
            if candidate.exists():
                data_path = candidate
        report = simulate_what_if(
            contract_path=contract_path,
            data_path=data_path,
            change_spec_path=spec_path,
            adapter_config=str(ADAPTER_PATH) if ADAPTER_PATH.exists() else None,
            lineage_path=str(LINEAGE_PATH) if LINEAGE_PATH.exists() else None,
            registry_path=str(REGISTRY_PATH) if REGISTRY_PATH.exists() else None,
            baseline_evaluation=baseline_by_contract.get(contract_id),
        )
        report["source_label"] = label
        report["source_data_path"] = str(data_path.relative_to(ROOT))
        snapshot_path = VALIDATION_REPORTS_DIR / f"what_if_{spec_path.stem}.json"
        write_json(snapshot_path, report)
        snapshots.append(str(snapshot_path.relative_to(ROOT)))
        latest_report = report
    if latest_report is not None:
        write_json(VALIDATION_REPORTS_DIR / "what_if_latest.json", latest_report)
    return snapshots


def sync_real_outputs() -> None:
    code = sync_real_outputs_main()
    if code != 0:
        raise RuntimeError(f"sync_real_week_artifacts failed with exit code {code}")


def _source_for_mode(source: Path, *, prefer_violated: bool) -> Path:
    if prefer_violated:
        violated_path = _violated_variant(source)
        if violated_path.exists():
            return violated_path
    return source


def generate_contracts() -> list[dict[str, str]]:
    generated: list[dict[str, str]] = []
    with schema_snapshot_scope("real"):
        for target in CONTRACT_TARGETS:
            source = target["source"]
            if not source.exists():
                raise FileNotFoundError(f"Missing source dataset for contract generation: {source}")
            contract = build_contract(
                str(source),
                target["contract_id"],
                str(LINEAGE_PATH) if LINEAGE_PATH.exists() else None,
                str(REGISTRY_PATH) if REGISTRY_PATH.exists() else None,
            )
            contract_path, dbt_path = write_contract_files(contract, str(ROOT / "generated_contracts"))
            generated.append(
                {
                    "contract_id": target["contract_id"],
                    "source": str(source.relative_to(ROOT)),
                    "contract": str(contract_path.relative_to(ROOT)),
                    "dbt": str(dbt_path.relative_to(ROOT)),
                }
            )
    return generated


def generate_snapshot_history(label: str, *, prefer_violated: bool) -> list[dict[str, str]]:
    snapshot_records: list[dict[str, str]] = []
    with schema_snapshot_scope(label):
        for target in CONTRACT_TARGETS:
            source = _source_for_mode(Path(target["source"]), prefer_violated=prefer_violated)
            if not source.exists():
                continue
            contract = build_contract(
                str(source),
                target["contract_id"],
                str(LINEAGE_PATH) if LINEAGE_PATH.exists() else None,
                str(REGISTRY_PATH) if REGISTRY_PATH.exists() else None,
            )
            with tempfile.TemporaryDirectory() as temp_dir:
                write_contract_files(contract, temp_dir)
            snapshot_records.append(
                {
                    "contract_id": target["contract_id"],
                    "source": str(source.relative_to(ROOT)),
                    "snapshot_scope": label,
                }
            )
    return snapshot_records


def run_ai_checks(label: str) -> dict[str, Any]:
    data_paths = DATASETS_FOR_AI[label]
    extraction_records = load_jsonl(data_paths["extractions"])
    verdict_records = load_jsonl(data_paths["verdicts"])
    trace_records = load_jsonl(data_paths["traces"])
    report = build_ai_extension_report(
        extraction_records,
        verdict_records,
        trace_records,
        source_label=label,
    )
    write_json(VALIDATION_REPORTS_DIR / "ai_extensions.json", report)
    write_json(VALIDATION_REPORTS_DIR / "ai_metrics.json", report)
    return report


def write_schema_evolution_summary(reports: list[dict[str, Any]]) -> str | None:
    for report in reports:
        schema_payload = report.get("schema_evolution")
        if not isinstance(schema_payload, dict):
            continue
        changes = schema_payload.get("changes", [])
        if not isinstance(changes, list) or not changes:
            continue
        notification = schema_payload.get("notification")
        payload = {
            "generated_at": utc_now(),
            "contract_id": report.get("contract_id"),
            "compatibility_verdict": schema_payload.get("compatibility_classification", "UNKNOWN"),
            "migration_checklist": [notification] if notification else [],
            "changes": changes,
        }
        target = VALIDATION_REPORTS_DIR / "schema_evolution.json"
        write_json(target, payload)
        return str(target.relative_to(ROOT))
    return None


def write_enforcer_report() -> dict[str, str]:
    report_payload = generate_report(
        reports_dir=str(VALIDATION_REPORTS_DIR),
        violations_path=str(VIOLATION_LOG_DIR / "violations.jsonl"),
        mode="weekly",
    )
    report_path = ENFORCER_REPORT_DIR / "report_data.json"
    report_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    pdf_path = ENFORCER_REPORT_DIR / f"report_{report_date}.pdf"
    write_json(report_path, report_payload)
    write_report_pdf(report_payload, pdf_path)
    return {
        "report_path": str(report_path.relative_to(ROOT)),
        "pdf_path": str(pdf_path.relative_to(ROOT)),
    }


def run_validation_pass(label: str, *, prefer_violated: bool, skip_what_if: bool) -> dict[str, Any]:
    with schema_snapshot_scope(label):
        validation = validate_all_weeks(prefer_violated=prefer_violated)
        status_by_report: dict[str, str] = {}
        data_path_by_report: dict[str, str] = {}
        for report in validation.get("reports", []):
            report_id = str(report.get("report_id", "report"))
            status_by_report[report_id] = str(report.get("overall_status", "UNKNOWN"))
            data_path_by_report[report_id] = str(report.get("data_path", ""))
        ai_report = run_ai_checks(label)
        violations = refresh_live_violations(validation.get("reports", []))
        what_if_reports = [] if skip_what_if else run_what_if_checks(
            label,
            prefer_violated=prefer_violated,
            validation_reports=validation.get("reports", []),
        )
        schema_report = write_schema_evolution_summary(validation.get("reports", []))
        enforcer_report = write_enforcer_report()
    summary = {
        "label": label,
        "prefer_violated": prefer_violated,
        "validated_weeks": validation.get("validated_weeks", []),
        "violation_count": len(violations),
        "status_by_report": status_by_report,
        "data_path_by_report": data_path_by_report,
        "snapshots": [f"validation_reports/{report.get('report_id', 'report')}.json" for report in validation.get("reports", [])],
        "ai_report": "validation_reports/ai_extensions.json",
        "what_if_reports": what_if_reports,
        "violations_snapshot": "violation_log/violations.jsonl",
        "completed_at": validation.get("completed_at"),
        "schema_report": schema_report,
        "ai_summary": {
            "embedding_drift": ai_report.get("embedding_drift", {}).get("status"),
            "prompt_input_validation": ai_report.get("prompt_input_validation", {}).get("status"),
            "structured_llm_output_enforcement": ai_report.get("structured_llm_output_enforcement", {}).get("status"),
            "llm_output_schema_rate": ai_report.get("llm_output_schema_rate", {}).get("status"),
            "langsmith_trace_schema_contracts": ai_report.get("langsmith_trace_schema_contracts", {}).get("status"),
        },
    }
    summary["enforcer_report"] = enforcer_report["report_path"]
    summary["enforcer_report_pdf"] = enforcer_report["pdf_path"]
    return summary


def run_mode(
    mode: str,
    *,
    skip_sync: bool = False,
    skip_contracts: bool = False,
    skip_what_if: bool = False,
) -> dict[str, Any]:
    if mode not in {"real", "violated"}:
        raise ValueError(f"Unsupported Week 7 mode: {mode}")

    ensure_runtime_dirs()
    summary: dict[str, Any] = {
        "run_at": utc_now(),
        "mode": mode,
        "steps": [],
    }

    if not skip_sync:
        sync_real_outputs()
        summary["steps"].append("synced_real_outputs")

    if not skip_contracts:
        generated = generate_contracts()
        summary["steps"].append("generated_contracts")
        summary["generated_contracts"] = generated

    if mode == "real":
        summary["schema_snapshots"] = generate_snapshot_history("real", prefer_violated=False)
        summary["steps"].append("snapshotted_real")
        summary["real"] = run_validation_pass("real", prefer_violated=False, skip_what_if=skip_what_if)
        summary["steps"].append("validated_real")
    else:
        injected = inject_violations_from_outputs()
        summary["injected"] = injected
        summary["steps"].append("injected_violations")
        summary["schema_snapshots"] = generate_snapshot_history("violated", prefer_violated=True)
        summary["steps"].append("snapshotted_violated")
        summary["violated"] = run_validation_pass("violated", prefer_violated=True, skip_what_if=skip_what_if)
        summary["steps"].append("validated_violated")

    summary_path = VALIDATION_REPORTS_DIR / "run_summary.json"
    write_json(summary_path, summary)
    return summary


def run() -> dict[str, Any]:
    args = parse_args()
    summary = run_mode(
        args.mode,
        skip_sync=args.skip_sync,
        skip_contracts=args.skip_contracts,
        skip_what_if=args.skip_what_if,
    )
    summary_path = VALIDATION_REPORTS_DIR / "run_summary.json"
    print(json.dumps(summary, indent=2))
    print(f"\nSaved summary: {summary_path.relative_to(ROOT)}")
    return summary


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
