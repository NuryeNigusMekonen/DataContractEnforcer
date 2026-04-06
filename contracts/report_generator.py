from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any
import textwrap

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.ai_extensions import ai_violation_records


SEVERITY_RANK = {"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}
AI_STATUS_KEYS = (
    "embedding_drift",
    "prompt_input_validation",
    "structured_llm_output_enforcement",
    "langsmith_trace_schema_contracts",
)


def public_ai_report(ai_report: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(ai_report, dict):
        return {}
    return {
        key: value
        for key, value in ai_report.items()
        if key != "llm_output_schema_rate"
    }


def _critical_violation_count(results: list[dict[str, Any]]) -> int:
    return sum(
        1
        for result in results
        if str(result.get("status", "")).upper() in {"FAIL", "ERROR"}
        and str(result.get("severity", "")).upper() == "CRITICAL"
    )

def _result_status_counts(results: list[dict[str, Any]]) -> tuple[int, int, int]:
    passed = 0
    failed = 0
    warned = 0
    for result in results:
        status = str(result.get("status", "")).upper()
        if status == "PASS":
            passed += 1
        elif status in {"FAIL", "ERROR"}:
            failed += 1
        elif status == "WARN":
            warned += 1
    return passed, failed, warned


def summarize_validation_reports(validation_reports: list[dict[str, Any]]) -> tuple[int, int, int, int, int]:
    total_checks = 0
    passed = 0
    failed = 0
    warned = 0
    critical_violations = 0

    for report in validation_reports:
        results = report.get("results", []) if isinstance(report.get("results"), list) else []
        summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}

        report_total = int(report.get("total_checks", 0) or len(results))
        report_passed = report.get("passed", summary.get("PASS"))
        report_failed = report.get("failed", summary.get("FAIL"))
        report_warned = report.get("warned", summary.get("WARN"))

        if report_passed is None or report_failed is None or report_warned is None:
            inferred_passed, inferred_failed, inferred_warned = _result_status_counts(results)
            if report_passed is None:
                report_passed = inferred_passed
            if report_failed is None:
                report_failed = inferred_failed
            if report_warned is None:
                report_warned = inferred_warned

        total_checks += report_total
        passed += int(report_passed or 0)
        failed += int(report_failed or 0)
        warned += int(report_warned or 0)
        critical_violations += _critical_violation_count(results)

    return total_checks, passed, failed, warned, critical_violations


def compute_health_score(validation_reports: list[dict[str, Any]]) -> int:
    total_checks, passed, _failed, _warned, critical_violations = summarize_validation_reports(validation_reports)
    if total_checks == 0:
        return 100
    score = ((passed / total_checks) * 100) - (critical_violations * 20)
    return max(0, min(100, int(score)))


def plain_language_violation(result: dict[str, Any]) -> str:
    field_name = result.get("field_name") or result.get("column_name") or "unknown field"
    failing_system = result.get("contract_id") or result.get("system") or result.get("check_id", "unknown system")
    impact_nodes = result.get("blast_radius", {}).get("affected_nodes", [])
    impact = "No downstream nodes were identified."
    if impact_nodes:
        impact = f"Downstream impact reaches {', '.join(str(node) for node in impact_nodes[:3])}."
    return (
        f"System issue in {failing_system} via {result.get('check_id', 'unknown check')}: the {field_name} field "
        f"failed validation with status {result.get('status')}. {impact} "
        f"Affected records: {result.get('records_failing', 'unknown')}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Data Contract Enforcer report artifacts.")
    parser.add_argument(
        "--mode",
        default="weekly",
        choices=["weekly", "baseline"],
        help="weekly uses all reports + violation log; baseline uses clean validation reports only.",
    )
    parser.add_argument("--reports-dir", default="validation_reports")
    parser.add_argument("--violations", default="violation_log/violations.jsonl")
    parser.add_argument("--output", default="", help="Optional explicit report_data.json output path.")
    return parser.parse_args()


def dedupe_violations(violations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for violation in violations:
        check_id = str(violation.get("check_id", ""))
        field_name = str(violation.get("field_name", violation.get("column_name", "")))
        status = str(violation.get("status", ""))
        key = (check_id, field_name, status)
        current = deduped.get(key)
        if current is None:
            deduped[key] = violation
            continue
        current_rank = SEVERITY_RANK.get(str(current.get("severity", "LOW")), 0)
        candidate_rank = SEVERITY_RANK.get(str(violation.get("severity", "LOW")), 0)
        if candidate_rank > current_rank:
            deduped[key] = violation
            continue
        if candidate_rank == current_rank and int(violation.get("records_failing", 0) or 0) > int(current.get("records_failing", 0) or 0):
            deduped[key] = violation
    return list(deduped.values())


def load_reports(reports_dir: str = "validation_reports", mode: str = "weekly") -> list[dict[str, Any]]:
    if mode == "baseline":
        preferred = [
            Path(reports_dir) / "clean.json",
            Path(reports_dir) / "wednesday_baseline.json",
            Path(reports_dir) / "clean_run.json",
        ]
        chosen: list[Path] = [path for path in preferred if path.exists()]
        if not chosen:
            chosen = sorted(Path(reports_dir).glob("clean*.json"))
        reports = [json.loads(path.read_text(encoding="utf-8")) for path in chosen]
        return [report for report in reports if "results" in report]
    reports = [json.loads(Path(path).read_text(encoding="utf-8")) for path in sorted(glob.glob(f"{reports_dir}/*.json"))]
    return [report for report in reports if "results" in report]


def load_violations(violations_path: str = "violation_log/violations.jsonl") -> list[dict[str, Any]]:
    path = Path(violations_path)
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                records.append(json.loads(line))
    return records


def load_ai_report(reports_dir: str = "validation_reports") -> dict[str, Any]:
    path = Path(reports_dir) / "ai_extensions.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_ai_report(ai_report: dict[str, Any]) -> tuple[int, int, int, int]:
    total_checks = 0
    passed = 0
    failed = 0
    warned = 0
    if not isinstance(ai_report, dict):
        return total_checks, passed, failed, warned

    for key in AI_STATUS_KEYS:
        payload = ai_report.get(key)
        if not isinstance(payload, dict):
            continue
        total_checks += 1
        status = str(payload.get("status", "UNKNOWN")).upper()
        if status in {"PASS", "BASELINE_SET"}:
            passed += 1
        elif status in {"FAIL", "ERROR"}:
            failed += 1
        elif status == "WARN":
            warned += 1
    return total_checks, passed, failed, warned


def load_schema_reports(reports_dir: str = "validation_reports") -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for path in sorted(glob.glob(f"{reports_dir}/schema_evolution*.json")):
        payloads.append(json.loads(Path(path).read_text(encoding="utf-8")))
    if payloads:
        return payloads

    # Fallback for e2e bundles: consume per-report embedded schema_evolution blocks.
    for path in sorted(glob.glob(f"{reports_dir}/*.json")):
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        if "results" not in payload:
            continue
        schema_payload = payload.get("schema_evolution")
        if isinstance(schema_payload, dict):
            payloads.append(schema_payload)
    return payloads


def load_what_if_reports(reports_dir: str = "validation_reports") -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for path in sorted(glob.glob(f"{reports_dir}/what_if_*.json")):
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if "compatibility_verdict" in payload:
            payloads.append(payload)
    return payloads


def schema_change_summary(schema_reports: list[dict[str, Any]]) -> list[str]:
    summaries: list[str] = []
    for report in schema_reports:
        for change in report.get("changes", []):
            classification = change.get("compatibility_class", change.get("classification", "unknown"))
            summaries.append(
                f"{change.get('field_name')}: {classification} - {change.get('rationale')}"
            )
    return summaries[:10]


def what_if_summary(what_if_reports: list[dict[str, Any]]) -> list[str]:
    summaries: list[str] = []
    for report in what_if_reports:
        proposed = report.get("proposed_change", {})
        summaries.append(
            f"{report.get('contract_id')}: {proposed.get('change_type')} on {proposed.get('field')} -> "
            f"{report.get('compatibility_verdict')} "
            f"(raw {report.get('raw_changed_status')}, adapter {report.get('adapter_status') or 'N/A'})"
        )
    return summaries[:10]


def recommended_actions(violations: list[dict[str, Any]]) -> list[str]:
    if not violations:
        return [
            "Keep running the full contract pipeline before sharing fresh downstream datasets.",
            "Regenerate schema snapshots after every intentional schema change so drift baselines stay trustworthy.",
            "Review quarantined AI inputs weekly and fix upstream text extraction gaps before they accumulate.",
        ]
    actions: list[str] = []
    seen_actions: set[str] = set()
    for violation in violations:
        field_name = violation.get("field_name", "unknown field")
        clause = violation.get("check_id", "unknown clause")
        candidates = violation.get("blame_chain", [])
        if candidates:
            top = candidates[0]
            action = (
                f"Update {top.get('file_path')} to satisfy contract clause {clause} for field {field_name}, then rerun validation."
            )
        else:
            contract_path = violation.get("contract_path") or "generated contract"
            action = (
                f"Inspect {contract_path} and restore contract clause {clause} for field {field_name} before downstream use."
            )
        if action not in seen_actions:
            seen_actions.add(action)
            actions.append(action)
        if len(actions) >= 3:
            break
    while len(actions) < 3:
        actions.append("Re-run contract generation and validation after each upstream schema or prompt pipeline change.")
    return actions[:3]


def wrap_lines(lines: list[str], width: int = 100) -> list[str]:
    wrapped: list[str] = []
    for line in lines:
        if not line:
            wrapped.append("")
            continue
        chunks = textwrap.wrap(line, width=width, break_long_words=False, break_on_hyphens=False)
        wrapped.extend(chunks or [""])
    return wrapped


def escape_pdf_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def build_pdf_bytes(lines: list[str]) -> bytes:
    pages: list[list[str]] = []
    page_size = 42
    for index in range(0, len(lines), page_size):
        pages.append(lines[index:index + page_size])
    if not pages:
        pages = [["No report content generated."]]

    objects: list[bytes] = []

    def add_object(payload: bytes) -> int:
        objects.append(payload)
        return len(objects)

    font_id = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    page_ids: list[int] = []
    content_ids: list[int] = []
    placeholder_page_payloads: list[bytes] = []
    for page_lines in pages:
        content_lines = ["BT", "/F1 11 Tf", "50 780 Td", "14 TL"]
        for line in page_lines:
            content_lines.append(f"({escape_pdf_text(line[:110])}) Tj")
            content_lines.append("T*")
        content_lines.append("ET")
        content_stream = "\n".join(content_lines).encode("latin-1", errors="replace")
        content_id = add_object(f"<< /Length {len(content_stream)} >>\nstream\n".encode("latin-1") + content_stream + b"\nendstream")
        content_ids.append(content_id)
        placeholder_page_payloads.append(b"")
        page_ids.append(add_object(b""))
    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    pages_id = add_object(f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("latin-1"))
    for idx, page_id in enumerate(page_ids):
        payload = (
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_ids[idx]} 0 R >>"
        ).encode("latin-1")
        objects[page_id - 1] = payload
    catalog_id = add_object(f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("latin-1"))

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("latin-1"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("latin-1"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
    pdf.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("latin-1")
    )
    return bytes(pdf)


def generate_report(
    reports_dir: str = "validation_reports",
    violations_path: str = "violation_log/violations.jsonl",
    mode: str = "weekly",
) -> dict[str, Any]:
    reports = load_reports(reports_dir, mode=mode)
    ai_report = load_ai_report(reports_dir)
    report_ai_payload = public_ai_report(ai_report)
    file_violations = [] if mode == "baseline" else load_violations(violations_path)
    ai_violations = [] if mode == "baseline" else ai_violation_records(ai_report)
    violations = dedupe_violations(file_violations + ai_violations)
    schema_reports = [] if mode == "baseline" else load_schema_reports(reports_dir)
    what_if_reports = [] if mode == "baseline" else load_what_if_reports(reports_dir)
    all_failures = [result for result in violations if result.get("status") in {"FAIL", "ERROR", "WARN"}]
    unique_failures = dedupe_violations(all_failures)
    top_failures = sorted(
        unique_failures,
        key=lambda item: (SEVERITY_RANK.get(str(item.get("severity")), 0), item.get("records_failing", 0)),
        reverse=True,
    )[:3]
    now = datetime.now(timezone.utc)
    validation_total, validation_passed, validation_failed, validation_warned, critical_violations = summarize_validation_reports(reports)
    ai_total, ai_passed, ai_failed, ai_warned = summarize_ai_report(report_ai_payload)
    total_checks = validation_total + ai_total
    passed = validation_passed + ai_passed
    failed = validation_failed + ai_failed
    warned = validation_warned + ai_warned
    if total_checks == 0:
        health_score = 100
    else:
        health_score = max(0, min(100, int(((passed / total_checks) * 100) - (critical_violations * 20))))
    if failed:
        producer_health_narrative = f"Health score is {health_score}/100 with live contract failures requiring attention."
    elif warned:
        producer_health_narrative = f"Health score is {health_score}/100 with warnings that need review."
    else:
        producer_health_narrative = f"Health score is {health_score}/100 and all monitored contracts are healthy."
    return {
        "mode": mode,
        "generated_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "period": f"{(now - timedelta(days=7)).date()} to {now.date()}",
        "producer_contract_health_score": health_score,
        "producer_contract_health_narrative": producer_health_narrative,
        # Legacy keys retained for compatibility with older consumers.
        "data_health_score": health_score,
        "health_narrative": producer_health_narrative,
        "top_violations": [plain_language_violation(result) for result in top_failures],
        "total_violations_by_severity": {
            severity: len([failure for failure in violations if failure.get("severity") == severity])
            for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        },
        "violation_count": len(violations),
        "total_checks": total_checks,
        "passed_checks": passed,
        "failed_checks": failed,
        "warned_checks": warned,
        "critical_violations": critical_violations,
        "schema_changes_detected": schema_change_summary(schema_reports),
        "what_if_simulations": what_if_summary(what_if_reports),
        "ai_system_risk_assessment": report_ai_payload,
        "recommendations": recommended_actions(top_failures),
    }


def main() -> int:
    args = parse_args()
    report = generate_report(reports_dir=args.reports_dir, violations_path=args.violations, mode=args.mode)
    default_name = "report_data_baseline.json" if args.mode == "baseline" else "report_data.json"
    output_path = Path(args.output) if args.output else Path("enforcer_report") / default_name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    suffix = "_baseline" if args.mode == "baseline" else ""
    pdf_path = output_path.parent / f"report_{report_date}{suffix}.pdf"
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
        f"Generated at: {report['generated_at']}",
        (
            f"Producer Contract Health Score: {report['producer_contract_health_score']} - "
            f"{report['producer_contract_health_narrative']}"
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
    pdf_path.write_bytes(build_pdf_bytes(wrap_lines(pdf_lines)))
    print(json.dumps({"report": str(output_path), "pdf": str(pdf_path), "health_score": report["data_health_score"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
