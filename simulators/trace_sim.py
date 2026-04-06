from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from simulators.common import (
    ApplicationContext,
    JsonDict,
    ViolationSpec,
    canonical_system_name,
    deep_copy_records,
    deterministic_uuid,
    fake_model_name,
    isoformat_z,
    seeded_random,
    selected_indices,
)


def _cost(prompt_tokens: int, completion_tokens: int, *, run_type: str) -> float:
    if run_type == "embedding":
        return round(prompt_tokens * 0.00000013, 6)
    return round(prompt_tokens * 0.000003 + completion_tokens * 0.000012, 6)


def generate_trace_records(
    count: int,
    seed: int,
    violations: list[ViolationSpec] | None = None,
    applications: list[ApplicationContext] | None = None,
    week2_records: list[JsonDict] | None = None,
    week3_records: list[JsonDict] | None = None,
    week5_records: list[JsonDict] | None = None,
) -> list[JsonDict]:
    if not applications:
        raise ValueError("trace generation requires application contexts")
    rng = seeded_random(seed, "traces")
    week2_records = week2_records or []
    week3_records = week3_records or []
    week5_records = week5_records or []
    records: list[JsonDict] = []
    sessions = max(1, (count + 5) // 6)
    base_time = datetime(2026, 3, 12, 10, 0, tzinfo=UTC)
    for session_index in range(sessions):
        app = applications[session_index % len(applications)]
        extraction = week3_records[session_index % len(week3_records)] if week3_records else {}
        verdict = week2_records[session_index % len(week2_records)] if week2_records else {}
        event = week5_records[session_index % len(week5_records)] if week5_records else {}
        session_id = deterministic_uuid("trace-session", app.application_id, session_index)
        root_id = deterministic_uuid("trace-root", session_id)
        start = base_time + timedelta(minutes=session_index * 21)
        run_blueprint: list[dict[str, Any]] = [
            {
                "name": "loan_review_session",
                "run_type": "chain",
                "parent_run_id": "",
                "inputs": {
                    "application_id": app.application_id,
                    "doc_id": extraction.get("doc_id", ""),
                    "verdict_target": verdict.get("target_ref", ""),
                },
                "outputs": {"status": "decision_generated", "event_id": event.get("event_id", "")},
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "tags": ["week3", "week2", "week5", "root"],
                "duration_seconds": 7,
            },
            {
                "name": "load_document_context",
                "run_type": "retriever",
                "parent_run_id": root_id,
                "inputs": {
                    "source_path": extraction.get("source_path", ""),
                    "application_id": app.application_id,
                    "doc_id": extraction.get("doc_id", ""),
                },
                "outputs": {"facts_loaded": len(extraction.get("extracted_facts", []))},
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "tags": ["week3", "retrieval"],
                "duration_seconds": 2,
            },
            {
                "name": "extract_financial_summary",
                "run_type": "llm",
                "parent_run_id": root_id,
                "inputs": {"model": fake_model_name(rng, "trace"), "doc_id": extraction.get("doc_id", "")},
                "outputs": {"summary_status": "ok", "confidence": 0.9},
                "prompt_tokens": 920 + session_index * 17,
                "completion_tokens": 180 + session_index * 11,
                "tags": ["week3", "llm", "extract"],
                "duration_seconds": 5,
            },
            {
                "name": "score_submission",
                "run_type": "llm",
                "parent_run_id": root_id,
                "inputs": {
                    "target_ref": verdict.get("target_ref", ""),
                    "application_id": app.application_id,
                    "arg": verdict.get("target_ref", ""),
                },
                "outputs": {
                    "overall_verdict": verdict.get("overall_verdict", "PASS"),
                    "result": verdict.get("overall_verdict", "PASS"),
                },
                "prompt_tokens": 810 + session_index * 13,
                "completion_tokens": 145 + session_index * 9,
                "tags": ["week4", "llm", "score"],
                "duration_seconds": 4,
            },
            {
                "name": "write_output",
                "run_type": "tool",
                "parent_run_id": root_id,
                "inputs": {
                    "event_type": event.get("event_type", ""),
                    "aggregate_id": event.get("aggregate_id", ""),
                    "command": "append_event",
                },
                "outputs": {
                    "event_id": event.get("event_id", ""),
                    "write_status": "committed",
                    "append_result": {
                        "event_id": event.get("event_id", ""),
                        "status": "ok",
                    },
                },
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "tags": ["week5", "tool", "event-store"],
                "duration_seconds": 1,
            },
            {
                "name": "index_session_summary",
                "run_type": "embedding",
                "parent_run_id": root_id,
                "inputs": {"session_id": session_id, "application_id": app.application_id},
                "outputs": {"vector_count": 1, "index_name": "simulated-review-summaries", "result": "indexed"},
                "prompt_tokens": 430 + session_index * 7,
                "completion_tokens": 0,
                "tags": ["week4", "embedding", "langsmith"],
                "duration_seconds": 3,
            },
        ]
        for run_index, blueprint in enumerate(run_blueprint):
            started_at = start + timedelta(seconds=run_index * 8)
            ended_at = started_at + timedelta(seconds=blueprint["duration_seconds"])
            total_tokens = blueprint["prompt_tokens"] + blueprint["completion_tokens"]
            records.append(
                {
                    "id": root_id if run_index == 0 else deterministic_uuid("trace-run", session_id, run_index, blueprint["name"]),
                    "name": blueprint["name"],
                    "run_type": blueprint["run_type"],
                    "inputs": blueprint["inputs"],
                    "outputs": blueprint["outputs"],
                    "error": None,
                    "start_time": isoformat_z(started_at),
                    "end_time": isoformat_z(ended_at),
                    "total_tokens": total_tokens,
                    "prompt_tokens": blueprint["prompt_tokens"],
                    "completion_tokens": blueprint["completion_tokens"],
                    "total_cost": _cost(blueprint["prompt_tokens"], blueprint["completion_tokens"], run_type=blueprint["run_type"]),
                    "tags": blueprint["tags"],
                    "parent_run_id": blueprint["parent_run_id"],
                    "session_id": session_id,
                }
            )
            if len(records) >= count:
                break
        if len(records) >= count:
            break
    trimmed = records[:count]
    return apply_trace_violations(trimmed, violations or [], seed)


def apply_trace_violations(records: list[JsonDict], violations: list[ViolationSpec], seed: int) -> list[JsonDict]:
    if not violations:
        return records
    mutated = deep_copy_records(records)
    for violation in violations:
        if canonical_system_name(violation.system) != "traces":
            continue
        for index in selected_indices(mutated, violation, seed):
            record = mutated[index]
            if violation.type == "total_tokens_mismatch":
                record["total_tokens"] = int(record["total_tokens"]) + 17
            elif violation.type == "invalid_run_type":
                record["run_type"] = "planner"
            elif violation.type == "end_time_before_start":
                record["end_time"] = isoformat_z(
                    datetime.fromisoformat(record["start_time"].replace("Z", "+00:00")) - timedelta(seconds=2)
                )
            elif violation.type == "negative_total_cost":
                record["total_cost"] = -0.01
            else:
                raise ValueError(f"unsupported trace violation: {violation.type}")
    return mutated
