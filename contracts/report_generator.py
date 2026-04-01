from __future__ import annotations

import glob
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

def compute_health_score(validation_reports: list[dict[str, Any]]) -> int:
    total_checks = sum(int(report.get("total_checks", 0) or len(report.get("results", []))) for report in validation_reports)
    passed = sum(int(report.get("passed", report.get("summary", {}).get("PASS", 0))) for report in validation_reports)
    critical_fails = sum(
        1
        for report in validation_reports
        for result in report.get("results", [])
        if result.get("status") in {"FAIL", "ERROR"} and result.get("severity") == "CRITICAL"
    )
    if total_checks == 0:
        return 100
    score = round((passed / total_checks) * 100) - (critical_fails * 20)
    return max(0, min(100, score))


def plain_language_violation(result: dict[str, Any]) -> str:
    field_name = result.get("field_name") or result.get("column_name") or "unknown field"
    impact_nodes = result.get("blast_radius", {}).get("affected_nodes", [])
    impact = "No downstream nodes were identified."
    if impact_nodes:
        impact = f"Downstream impact reaches {', '.join(str(node) for node in impact_nodes[:3])}."
    return (
        f"System issue in {result.get('check_id', 'unknown check')}: the {field_name} field "
        f"failed validation with status {result.get('status')}. {impact} "
        f"Affected records: {result.get('records_failing', 'unknown')}."
    )


def load_reports(reports_dir: str = "validation_reports") -> list[dict[str, Any]]:
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


def load_ai_report(path: str = "validation_reports/ai_extensions.json") -> dict[str, Any]:
    report_path = Path(path)
    if not report_path.exists():
        return {}
    return json.loads(report_path.read_text(encoding="utf-8"))


def load_schema_reports(reports_dir: str = "validation_reports") -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for path in sorted(glob.glob(f"{reports_dir}/schema_evolution*.json")):
        payloads.append(json.loads(Path(path).read_text(encoding="utf-8")))
    return payloads


def schema_change_summary(schema_reports: list[dict[str, Any]]) -> list[str]:
    summaries: list[str] = []
    for report in schema_reports:
        for change in report.get("changes", []):
            summaries.append(
                f"{change.get('field_name')}: {change.get('classification')} - {change.get('rationale')}"
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
    for violation in violations[:3]:
        field_name = violation.get("field_name", "unknown field")
        candidates = violation.get("blame_chain", [])
        if candidates:
            top = candidates[0]
            actions.append(
                f"Update {top.get('file_path')} to restore contract compliance for {field_name} and rerun validation."
            )
        else:
            actions.append(
                f"Investigate the upstream producer for {field_name} and restore the expected contract shape before downstream use."
            )
    while len(actions) < 3:
        actions.append("Re-run contract generation and validation after each upstream schema or prompt pipeline change.")
    return actions[:3]


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


def generate_report(reports_dir: str = "validation_reports", violations_path: str = "violation_log/violations.jsonl") -> dict[str, Any]:
    reports = load_reports(reports_dir)
    violations = load_violations(violations_path)
    ai_report = load_ai_report()
    schema_reports = load_schema_reports(reports_dir)
    all_failures = [result for result in violations if result.get("status") in {"FAIL", "ERROR", "WARN"}]
    top_failures = sorted(
        all_failures,
        key=lambda item: ({"CRITICAL": 3, "HIGH": 2, "MEDIUM": 1, "LOW": 0}.get(str(item.get("severity")), 0), item.get("records_failing", 0)),
        reverse=True,
    )[:3]
    now = datetime.now(timezone.utc)
    health_score = compute_health_score(reports)
    return {
        "generated_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "period": f"{(now - timedelta(days=7)).date()} to {now.date()}",
        "data_health_score": health_score,
        "health_narrative": (
            "No critical violations detected."
            if health_score >= 90
            else f"Health score dropped to {health_score}/100 due to actionable contract failures."
        ),
        "top_violations": [plain_language_violation(result) for result in top_failures],
        "total_violations_by_severity": {
            severity: len([failure for failure in violations if failure.get("severity") == severity])
            for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        },
        "violation_count": len(violations),
        "schema_changes_detected": schema_change_summary(schema_reports),
        "ai_system_risk_assessment": ai_report,
        "recommendations": recommended_actions(top_failures),
    }


def main() -> int:
    report = generate_report()
    output_path = Path("enforcer_report/report_data.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    pdf_path = output_path.parent / f"report_{report_date}.pdf"
    pdf_lines = [
        "Data Contract Enforcer Report",
        f"Generated at: {report['generated_at']}",
        f"Data Health Score: {report['data_health_score']} - {report['health_narrative']}",
        "",
        "Violations This Week:",
        *[f"- {item}" for item in report.get("top_violations", [])],
        "",
        "Schema Changes Detected:",
        *[f"- {item}" for item in report.get("schema_changes_detected", [])[:5]],
        "",
        "AI System Risk Assessment:",
        json.dumps(report.get("ai_system_risk_assessment", {}), sort_keys=True),
        "",
        "Recommended Actions:",
        *[f"- {item}" for item in report.get("recommendations", [])],
    ]
    pdf_path.write_bytes(build_pdf_bytes(pdf_lines))
    print(json.dumps({"report": str(output_path), "pdf": str(pdf_path), "health_score": report["data_health_score"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
