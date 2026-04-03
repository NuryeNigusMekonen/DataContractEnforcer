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
    for path in sorted(VALIDATION_REPORTS_DIR.glob("*.json")):
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
    live_path = VIOLATION_LOG_DIR / "live_violations.jsonl"
    path = live_path if live_path.exists() else VIOLATION_LOG_DIR / "violations.jsonl"
    violations = read_jsonl_file(path)
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
    payload = read_json_file(ENFORCER_REPORT_DIR / "report_data.json")
    return payload if isinstance(payload, dict) else None


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


def compute_health_score(total_checks: int, passed: int, warned: int) -> int:
    if total_checks <= 0:
        return 100
    score = ((passed + (warned * 0.5)) / total_checks) * 100
    return max(0, min(100, int(score)))
