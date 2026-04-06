from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .common import (
    RUNS_FILE,
    TRACE_CONTRACT,
    WEEK_CONTRACTS,
    best_timestamp,
    combine_status,
    count_critical_violations,
    compute_health_score,
    latest_report_by_contract,
    load_ai_report,
    load_violations,
    parse_timestamp,
    read_jsonl_file,
    timestamp_to_iso,
)

try:
    from backend.watcher import get_watcher
except ModuleNotFoundError:
    from watcher import get_watcher

from .validation_service import available_scenarios


def _collect_affected_systems(violations: list[dict[str, Any]]) -> list[str]:
    systems: set[str] = set()
    for violation in violations:
        payload = violation.get("blast_radius", {})
        if not isinstance(payload, dict):
            continue
        for subscriber in payload.get("affected_subscribers", []):
            if subscriber:
                systems.add(str(subscriber))
    return sorted(systems)


def _latest_validation_time(weeks: list[dict[str, Any]]) -> str | None:
    timestamps = [parse_timestamp(week.get("last_updated")) for week in weeks]
    return timestamp_to_iso(max((stamp for stamp in timestamps if stamp is not None), default=None))


def _build_traces_status() -> dict[str, Any]:
    latest_reports = latest_report_by_contract()
    traces_report = latest_reports.get(TRACE_CONTRACT["contract_id"])
    ai_report = load_ai_report()
    ai_checks: list[dict[str, Any]] = []
    if ai_report:
        for key in (
            "embedding_drift",
            "prompt_input_validation",
            "structured_llm_output_enforcement",
            "langsmith_trace_schema_contracts",
        ):
            payload = ai_report.get(key)
            if isinstance(payload, dict):
                ai_checks.append({"name": key, "status": payload.get("status", "PASS")})

    ai_statuses = [check["status"] for check in ai_checks]
    ai_passed = sum(1 for status in ai_statuses if str(status).upper() in {"PASS", "BASELINE_SET"})
    ai_failed = sum(1 for status in ai_statuses if str(status).upper() in {"FAIL", "ERROR"})
    ai_warned = sum(1 for status in ai_statuses if str(status).upper() == "WARN")

    if traces_report:
        base_status = traces_report.get("overall_status", "PASS")
        statuses = [base_status, *ai_statuses] if ai_checks else [base_status]
        return {
            "key": "traces",
            "week_name": "Traces",
            "status": combine_status(statuses),
            "checks_passed": int(traces_report.get("passed", 0) or 0) + ai_passed,
            "checks_failed": int(traces_report.get("failed", 0) or 0) + ai_failed,
            "checks_warned": int(traces_report.get("warned", 0) or 0) + ai_warned,
            "total_checks": int(traces_report.get("total_checks", 0) or 0) + len(ai_checks),
            "last_updated": traces_report.get("_timestamp"),
            "details": ai_checks,
        }

    if ai_report:
        last_updated = timestamp_to_iso(best_timestamp(ai_report))
        return {
            "key": "traces",
            "week_name": "Traces",
            "status": combine_status(ai_statuses),
            "checks_passed": ai_passed,
            "checks_failed": ai_failed,
            "checks_warned": ai_warned,
            "total_checks": len(ai_checks),
            "last_updated": last_updated,
            "details": ai_checks,
        }

    runs = read_jsonl_file(RUNS_FILE)
    errored = sum(1 for run in runs if run.get("error"))
    statuses = ["FAIL" if errored else "PASS"]
    timestamps = [parse_timestamp(run.get("end_time")) or parse_timestamp(run.get("start_time")) for run in runs]
    observed = max((stamp for stamp in timestamps if stamp is not None), default=datetime.now(timezone.utc))
    return {
        "key": "traces",
        "week_name": "Traces",
        "status": combine_status(statuses),
        "checks_passed": 0 if errored else len(runs),
        "checks_failed": errored,
        "checks_warned": 0,
        "total_checks": len(runs),
        "last_updated": timestamp_to_iso(observed),
        "details": [],
    }


def get_weeks_status() -> list[dict[str, Any]]:
    latest_reports = latest_report_by_contract()
    weeks: list[dict[str, Any]] = []

    for config in WEEK_CONTRACTS:
        report = latest_reports.get(config["contract_id"])
        if report:
            weeks.append(
                {
                    "key": config["key"],
                    "week_name": config["label"],
                    "contract_id": config["contract_id"],
                    "status": report.get("overall_status", "PASS"),
                    "checks_passed": report.get("passed", 0),
                    "checks_failed": report.get("failed", 0),
                    "checks_warned": report.get("warned", 0),
                    "total_checks": report.get("total_checks", 0),
                    "last_updated": report.get("_timestamp"),
                    "record_count": report.get("record_count", 0),
                    "source": report.get("_source_path"),
                }
            )
        else:
            weeks.append(
                {
                    "key": config["key"],
                    "week_name": config["label"],
                    "contract_id": config["contract_id"],
                    "status": "WARN",
                    "checks_passed": 0,
                    "checks_failed": 0,
                    "checks_warned": 0,
                    "total_checks": 0,
                    "last_updated": None,
                    "record_count": 0,
                    "source": None,
                }
            )

    weeks.append(_build_traces_status())
    return weeks


def get_summary() -> dict[str, Any]:
    weeks = get_weeks_status()
    latest_reports = latest_report_by_contract()
    violations = load_violations()
    watcher_state = get_watcher().snapshot_state()

    total_checks = sum(int(week.get("total_checks", 0) or 0) for week in weeks)
    passed = sum(int(week.get("checks_passed", 0) or 0) for week in weeks)
    failed = sum(int(week.get("checks_failed", 0) or 0) for week in weeks)
    warned = sum(int(week.get("checks_warned", 0) or 0) for week in weeks)
    critical_violations = sum(
        count_critical_violations(report.get("results"))
        for report in latest_reports.values()
        if isinstance(report, dict)
    )

    last_updated = _latest_validation_time(weeks)
    health_score = compute_health_score(total_checks, passed, critical_violations)
    if failed:
        narrative = f"Health score is {health_score}/100 with live contract failures requiring attention."
    elif warned:
        narrative = f"Health score is {health_score}/100 with warnings that need review."
    else:
        narrative = f"Health score is {health_score}/100 and all monitored contracts are healthy."

    affected_systems = _collect_affected_systems(violations)
    active_critical_incidents = sum(
        1
        for violation in violations
        if str(violation.get("severity", "")).upper() in {"CRITICAL", "HIGH"}
        or str(violation.get("status", "")).upper() == "ERROR"
    )

    return {
        "data_health_score": health_score,
        "total_checks": total_checks,
        "pass": passed,
        "fail": failed,
        "warn": warned,
        "critical_violations": critical_violations,
        "last_updated": last_updated,
        "last_validation_time": last_updated,
        "health_narrative": narrative,
        "active_critical_incidents": active_critical_incidents,
        "affected_systems_count": len(affected_systems),
        "affected_systems": affected_systems,
        "top_violations": [violation.get("message", "") for violation in violations[:3]],
        "watcher": watcher_state,
        "available_scenarios": available_scenarios(),
    }


def get_kpi_summary() -> dict[str, Any]:
    summary = get_summary()
    last_validation_time = summary.get("last_validation_time")
    affected_systems_count = int(summary.get("affected_systems_count", 0) or 0)
    incident_count = int(summary.get("active_critical_incidents", 0) or 0)

    if incident_count:
        incident_context = "Critical contract failures need immediate triage."
    else:
        incident_context = "No active critical incidents across monitored contracts."

    if affected_systems_count:
        affected_context = "Downstream consumers are exposed to current failures."
    else:
        affected_context = "No downstream systems are currently exposed."

    return {
        "health_score": summary.get("data_health_score", 100),
        "health_narrative": summary.get("health_narrative"),
        "incident_count": incident_count,
        "incident_context": incident_context,
        "affected_systems_count": affected_systems_count,
        "affected_systems_context": affected_context,
        "last_validation_time": last_validation_time,
        "last_validation_context": "Most recent contract validation refresh.",
        "watcher": summary.get("watcher", {}),
        "available_scenarios": summary.get("available_scenarios", []),
    }
