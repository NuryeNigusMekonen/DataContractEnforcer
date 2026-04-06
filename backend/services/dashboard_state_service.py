from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from contracts.ai_extensions import build_ai_extension_report
from contracts.common import load_jsonl, utc_now
from contracts.report_generator import build_pdf_bytes, generate_report, wrap_lines

from .common import (
    ENFORCER_REPORT_DIR,
    OUTPUTS_DIR,
    REPO_ROOT,
    VALIDATION_REPORTS_DIR,
    read_json_file,
)


def _violated_variant(path: Path) -> Path:
    return path.with_name(f"{path.stem}_violated{path.suffix}")


def _source_for_mode(path: Path, *, prefer_violated: bool) -> Path:
    if prefer_violated:
        candidate = _violated_variant(path)
        if candidate.exists():
            return candidate
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_report_pdf(report: dict[str, Any], output_path: Path) -> None:
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


def refresh_ai_reports(*, prefer_violated: bool) -> str:
    extractions_path = _source_for_mode(OUTPUTS_DIR / "week3" / "extractions.jsonl", prefer_violated=prefer_violated)
    verdicts_path = _source_for_mode(OUTPUTS_DIR / "week2" / "verdicts.jsonl", prefer_violated=prefer_violated)
    traces_path = _source_for_mode(OUTPUTS_DIR / "traces" / "runs.jsonl", prefer_violated=prefer_violated)

    extraction_records = load_jsonl(extractions_path)
    verdict_records = load_jsonl(verdicts_path)
    trace_records = load_jsonl(traces_path)

    payload = build_ai_extension_report(
        extraction_records,
        verdict_records,
        trace_records,
        source_label="violated" if prefer_violated else "real",
    )
    target = VALIDATION_REPORTS_DIR / "ai_extensions.json"
    _write_json(target, payload)
    _write_json(VALIDATION_REPORTS_DIR / "ai_metrics.json", payload)
    return str(target.relative_to(REPO_ROOT))


def refresh_schema_summary(reports: list[dict[str, Any]]) -> str:
    selected_report: dict[str, Any] | None = None
    selected_changes: list[dict[str, Any]] = []

    for report in reports:
        schema_payload = report.get("schema_evolution")
        if not isinstance(schema_payload, dict):
            continue
        changes = schema_payload.get("changes", [])
        if not isinstance(changes, list):
            continue
        if changes and not selected_changes:
            selected_report = report
            selected_changes = [change for change in changes if isinstance(change, dict)]
            break
        if selected_report is None:
            selected_report = report

    payload = {
        "generated_at": utc_now(),
        "contract_id": selected_report.get("contract_id") if selected_report else None,
        "compatibility_verdict": "UNKNOWN",
        "migration_checklist": [],
        "changes": selected_changes,
    }

    if selected_report:
        schema_payload = selected_report.get("schema_evolution")
        if isinstance(schema_payload, dict):
            notification = schema_payload.get("notification")
            payload["compatibility_verdict"] = schema_payload.get("compatibility_classification", "UNKNOWN")
            payload["migration_checklist"] = [notification] if notification else []

    target = VALIDATION_REPORTS_DIR / "schema_evolution.json"
    _write_json(target, payload)
    return str(target.relative_to(REPO_ROOT))


def refresh_enforcer_report() -> dict[str, str]:
    report_payload = generate_report(
        reports_dir=str(VALIDATION_REPORTS_DIR),
        violations_path=str(REPO_ROOT / "violation_log" / "violations.jsonl"),
        mode="weekly",
    )
    report_path = ENFORCER_REPORT_DIR / "report_data.json"
    report_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    pdf_path = ENFORCER_REPORT_DIR / f"report_{report_date}.pdf"
    _write_json(report_path, report_payload)
    _write_report_pdf(report_payload, pdf_path)
    return {
        "report_path": str(report_path.relative_to(REPO_ROOT)),
        "pdf_path": str(pdf_path.relative_to(REPO_ROOT)),
    }


def refresh_run_summary(
    *,
    reports: list[dict[str, Any]],
    violations: list[dict[str, Any]],
    prefer_violated: bool,
    reason: str,
) -> str:
    summary_path = VALIDATION_REPORTS_DIR / "run_summary.json"
    payload = read_json_file(summary_path)
    if not isinstance(payload, dict):
        payload = {
            "run_at": utc_now(),
            "mode": "violated" if prefer_violated else "real",
            "steps": [],
        }

    status_by_report: dict[str, str] = {}
    data_path_by_report: dict[str, str] = {}
    for report in reports:
        report_id = str(report.get("report_id", "report"))
        status_by_report[report_id] = str(report.get("overall_status", "UNKNOWN"))
        data_path_by_report[report_id] = str(report.get("data_path", ""))

    payload["final_live"] = {
        "updated_at": utc_now(),
        "reason": reason,
        "effective_mode": "violated" if prefer_violated else "real",
        "violation_count": len(violations),
        "status_by_report": status_by_report,
        "data_path_by_report": data_path_by_report,
        "snapshots": [f"validation_reports/{report.get('report_id', 'report')}.json" for report in reports],
    }
    _write_json(summary_path, payload)
    return str(summary_path.relative_to(REPO_ROOT))


def refresh_dashboard_state(
    *,
    reports: list[dict[str, Any]],
    violations: list[dict[str, Any]],
    prefer_violated: bool,
    reason: str,
) -> dict[str, Any]:
    ai_report = refresh_ai_reports(prefer_violated=prefer_violated)
    schema_report = refresh_schema_summary(reports)
    enforcer_report = refresh_enforcer_report()
    summary_path = refresh_run_summary(
        reports=reports,
        violations=violations,
        prefer_violated=prefer_violated,
        reason=reason,
    )
    return {
        "ai_report": ai_report,
        "schema_report": schema_report,
        "enforcer_report": enforcer_report["report_path"],
        "enforcer_report_pdf": enforcer_report["pdf_path"],
        "run_summary": summary_path,
    }
