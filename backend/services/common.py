from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATION_REPORTS_DIR = REPO_ROOT / "validation_reports"
VIOLATION_LOG_DIR = REPO_ROOT / "violation_log"
SCHEMA_SNAPSHOTS_DIR = REPO_ROOT / "schema_snapshots"
ENFORCER_REPORT_DIR = REPO_ROOT / "enforcer_report"
OUTPUTS_DIR = REPO_ROOT / "outputs"
RUNS_FILE = REPO_ROOT / "runs.jsonl"
TEST_DATA_DIR = REPO_ROOT / "test_data"
CHANGE_SPECS_DIR = TEST_DATA_DIR / "changes"
SCENARIOS_DIR = TEST_DATA_DIR / "scenarios"

TRACE_CONTRACT = {"key": "traces", "label": "Traces", "contract_id": "langsmith-trace-records"}
CURRENT_VALIDATION_REPORTS = (
    "week1.json",
    "week2.json",
    "week3.json",
    "week4.json",
    "week5.json",
    "traces.json",
)

WEEK_CONTRACTS = [
    {"key": "week1", "label": "Week 1", "contract_id": "week1-intent-records"},
    {"key": "week2", "label": "Week 2", "contract_id": "week2-verdict-records"},
    {"key": "week3", "label": "Week 3", "contract_id": "week3-document-refinery-extractions"},
    {"key": "week4", "label": "Week 4", "contract_id": "week4-lineage-snapshots"},
    {"key": "week5", "label": "Week 5", "contract_id": "week5-event-records"},
]


def read_json_file(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def read_jsonl_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    records: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                records.append(payload)
    except OSError:
        return []
    return records


def parse_timestamp(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None

    candidate = value.strip()
    patterns = (
        ("%Y-%m-%dT%H:%M:%SZ", None),
        ("%Y-%m-%dT%H:%M:%S.%fZ", None),
        ("%Y-%m-%d %H:%M:%S %z", None),
    )

    for pattern, _ in patterns:
        try:
            parsed = datetime.strptime(candidate, pattern)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue

    try:
        normalized = candidate.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def timestamp_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def best_timestamp(payload: dict[str, Any] | None, path: Path | None = None) -> datetime | None:
    if isinstance(payload, dict):
        for key in ("generated_at", "run_timestamp", "detected_at", "end_time", "start_time"):
            parsed = parse_timestamp(payload.get(key))
            if parsed is not None:
                return parsed

    if path is not None and path.exists():
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return None


def load_latest_json(folder: Path, pattern: str) -> tuple[dict[str, Any] | list[Any] | None, Path | None]:
    candidates = sorted(folder.glob(pattern))
    best_payload: dict[str, Any] | list[Any] | None = None
    best_path: Path | None = None
    best_time: datetime | None = None

    for candidate in candidates:
        payload = read_json_file(candidate)
        if payload is None:
            continue
        observed = best_timestamp(payload if isinstance(payload, dict) else None, candidate)
        if best_time is None or (observed is not None and observed > best_time):
            best_payload = payload
            best_path = candidate
            best_time = observed

    return best_payload, best_path


def load_validation_reports() -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    canonical_paths = [VALIDATION_REPORTS_DIR / name for name in CURRENT_VALIDATION_REPORTS]
    available_paths = [path for path in canonical_paths if path.exists()]
    candidate_paths = available_paths if available_paths else sorted(VALIDATION_REPORTS_DIR.glob("*.json"))

    for path in candidate_paths:
        payload = read_json_file(path)
        if not isinstance(payload, dict):
            continue
        if "contract_id" not in payload or "overall_status" not in payload:
            continue

        enriched = dict(payload)
        enriched["_source_path"] = str(path.relative_to(REPO_ROOT))
        enriched["_timestamp"] = timestamp_to_iso(best_timestamp(payload, path))
        reports.append(enriched)

    reports.sort(key=lambda report: parse_timestamp(report.get("_timestamp")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return reports


def latest_report_by_contract() -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for report in load_validation_reports():
        contract_id = report.get("contract_id")
        if not isinstance(contract_id, str) or contract_id in latest:
            continue
        latest[contract_id] = report
    return latest


def load_violations() -> list[dict[str, Any]]:
    path = VIOLATION_LOG_DIR / "violations.jsonl"
    violations = read_jsonl_file(path)
    ai_report = load_ai_report()
    if isinstance(ai_report, dict):
        from contracts.ai_extensions import ai_violation_records

        violations.extend(ai_violation_records(ai_report))

    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for violation in violations:
        key = (
            str(violation.get("check_id", "")),
            str(violation.get("field_name", violation.get("column_name", ""))),
            str(violation.get("status", "")),
        )
        current = deduped.get(key)
        if current is None or int(violation.get("records_failing", 0) or 0) >= int(current.get("records_failing", 0) or 0):
            deduped[key] = violation
    violations = list(deduped.values())
    violations.sort(
        key=lambda violation: parse_timestamp(violation.get("detected_at")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return violations


def load_ai_report() -> dict[str, Any] | None:
    for candidate in ("ai_metrics.json", "ai_extensions.json"):
        payload = read_json_file(VALIDATION_REPORTS_DIR / candidate)
        if isinstance(payload, dict):
            return payload
    return None


def load_enforcer_report() -> dict[str, Any] | None:
    canonical_path = ENFORCER_REPORT_DIR / "report_data.json"
    payload = read_json_file(canonical_path)
    if isinstance(payload, dict):
        enriched = dict(payload)
        enriched["_source_path"] = str(canonical_path.relative_to(REPO_ROOT))
        return enriched
    return None


def load_current_run_mode() -> str:
    summary_path = VALIDATION_REPORTS_DIR / "run_summary.json"
    payload = read_json_file(summary_path)
    if not isinstance(payload, dict):
        return "real"
    final_live = payload.get("final_live")
    if isinstance(final_live, dict):
        effective_mode = final_live.get("effective_mode")
        if effective_mode in {"real", "violated"}:
            return str(effective_mode)
    mode = payload.get("mode")
    if mode in {"real", "violated"}:
        return str(mode)
    return "real"


def derive_week_label(value: str | None) -> str:
    if not value:
        return "Unknown"

    lowered = value.lower()
    if "week1" in lowered:
        return "Week 1"
    if "week2" in lowered:
        return "Week 2"
    if "week3" in lowered:
        return "Week 3"
    if "week4" in lowered:
        return "Week 4"
    if "week5" in lowered:
        return "Week 5"
    if "trace" in lowered or "langsmith" in lowered or "ai." in lowered:
        return "Traces"
    return "Unknown"


def combine_status(statuses: list[str]) -> str:
    normalized = [status.upper() for status in statuses if isinstance(status, str)]
    if any(status in {"FAIL", "ERROR"} for status in normalized):
        return "FAIL"
    if any(status in {"WARN"} for status in normalized):
        return "WARN"
    return "PASS"


def count_critical_violations(results: list[dict[str, Any]] | None) -> int:
    if not results:
        return 0
    return sum(
        1
        for result in results
        if str(result.get("status", "")).upper() in {"FAIL", "ERROR"}
        and str(result.get("severity", "")).upper() == "CRITICAL"
    )


def compute_health_score(total_checks: int, passed: int, critical_violations: int = 0) -> int:
    if total_checks <= 0:
        return 100
    score = ((passed / total_checks) * 100) - (max(0, critical_violations) * 20)
    return max(0, min(100, int(score)))
