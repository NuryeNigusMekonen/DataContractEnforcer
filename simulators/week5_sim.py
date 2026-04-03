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


def _event_metadata(
    app: ApplicationContext,
    source_service: str,
    user_id: str,
    causation_id: str | None,
) -> JsonDict:
    return {
        "correlation_id": app.correlation_id,
        "source_service": source_service,
        "user_id": user_id,
        "causation_id": causation_id,
    }


def _extraction_payload(app: ApplicationContext, completed_at: str) -> JsonDict:
    return {
        "application_id": app.application_id,
        "completed_at": completed_at,
        "document_path": app.document_paths["income_statement"],
        "extraction_context": {
            "origin": "simulated_pdf",
            "pages": 6,
            "strategy_used": "layout_aware_refinery",
        },
        "extraction_notes": [
            "OCR confidence remained above the acceptance threshold.",
            "Table values were cross-checked against the appendix page.",
        ],
        "fact_provenance": {
            "ebitda": {"page": 3, "source": "income statement"},
            "net_income": {"page": 3, "source": "income statement"},
            "total_assets": {"page": 4, "source": "balance sheet"},
            "total_liabilities": {"page": 4, "source": "balance sheet"},
            "total_revenue": {"page": 2, "source": "income statement"},
        },
        "facts": {
            "ebitda": app.financials["ebitda"],
            "net_income": app.financials["net_income"],
            "total_assets": app.financials["total_assets"],
            "total_liabilities": app.financials["total_liabilities"],
            "total_revenue": app.financials["total_revenue"],
        },
        "field_confidence": {
            "ebitda": 0.88,
            "net_income": 0.9,
            "total_assets": 0.92,
            "total_liabilities": 0.89,
            "total_revenue": 0.93,
        },
    }


def _application_events(app: ApplicationContext, event_base: datetime) -> list[JsonDict]:
    occurred = [isoformat_z(event_base + timedelta(minutes=offset)) for offset in [0, 2, 4, 5, 7, 9, 11, 13, 15, 18]]
    decision_amount = round(app.requested_amount_usd * 0.82, 2)
    decision_confidence = 0.87 if app.index % 4 else 0.74
    compliance_verdict = "PASS" if decision_confidence >= 0.8 else "WARN"
    model_rng = seeded_random(app.application_id, "decision")
    payloads: list[tuple[str, str, str, JsonDict]] = [
        (
            "ApplicationSubmitted",
            "1.0",
            "loan-application-service",
            {
                "applicant_id": app.applicant_id,
                "application_id": app.application_id,
                "loan_purpose": "working_capital",
                "requested_amount_usd": app.requested_amount_usd,
                "submission_channel": "portfolio-portal",
                "submitted_at": occurred[0].replace("Z", "+00:00"),
            },
        ),
        (
            "DocumentUploadRequested",
            "1.0",
            "loan-application-service",
            {
                "application_id": app.application_id,
                "document_path": app.document_paths["income_statement"],
                "requested_at": occurred[1].replace("Z", "+00:00"),
                "requested_by": app.applicant_id.lower(),
            },
        ),
        (
            "DocumentUploaded",
            "1.0",
            "document-intake-service",
            {
                "application_id": app.application_id,
                "document_path": app.document_paths["income_statement"],
                "uploaded_at": occurred[2].replace("Z", "+00:00"),
                "uploaded_by": app.applicant_id.lower(),
            },
        ),
        (
            "DocumentAdded",
            "1.0",
            "document-package-service",
            {
                "application_id": app.application_id,
                "document_path": app.document_paths["income_statement"],
                "document_type": "pdf",
                "added_at": occurred[3].replace("Z", "+00:00"),
            },
        ),
        (
            "ExtractionStarted",
            "1.0",
            "week3-document-refinery",
            {
                "application_id": app.application_id,
                "document_path": app.document_paths["income_statement"],
                "pipeline": "week3-document-refinery",
                "started_at": occurred[4].replace("Z", "+00:00"),
            },
        ),
        (
            "ExtractionCompleted",
            "1.0",
            "week3-document-refinery",
            _extraction_payload(app, occurred[5].replace("Z", "+00:00")),
        ),
        (
            "QualityAssessmentCompleted",
            "1.0",
            "week3-document-refinery",
            {
                "application_id": app.application_id,
                "anomalies": [],
                "auditor_notes": "Seeded quality assessment output.",
                "critical_missing_fields": [],
                "is_coherent": True,
                "overall_confidence": 0.91,
                "reextraction_recommended": False,
            },
        ),
        (
            "ComplianceCheckRequested",
            "1.0",
            "compliance-orchestrator",
            {
                "application_id": app.application_id,
                "checks_required": ["KYC-001", "AML-004", "REG-003"],
                "regulation_set_version": "2026.1",
            },
        ),
        (
            "ComplianceCheckCompleted",
            "1.0",
            "week2-digital-courtroom",
            {
                "application_id": app.application_id,
                "completed_at": occurred[8].replace("Z", "+00:00"),
                "completed_checks": 3,
                "failed_rule_ids": [],
                "overall_verdict": compliance_verdict,
                "total_checks": 3,
            },
        ),
        (
            "DecisionGenerated",
            "2.0",
            "week2-digital-courtroom",
            {
                "application_id": app.application_id,
                "assessed_max_limit_usd": decision_amount,
                "compliance_status": "CLEARED",
                "confidence_score": decision_confidence,
                "contributing_agent_sessions": [app.agent_session_id],
                "decision_basis_summary": "seed simulation orchestration output",
                "model_versions": {"orchestrator-seed-01": fake_model_name(model_rng, "decision")},
                "orchestrator_agent_id": "orchestrator-seed-01",
                "recommendation": "approve_with_standard_monitoring",
            },
        ),
    ]
    events: list[JsonDict] = []
    causation_id: str | None = None
    for sequence_number, (event_type, schema_version, source_service, payload) in enumerate(payloads, start=1):
        occurred_at = occurred[sequence_number - 1]
        event_id = deterministic_uuid("week5", app.application_id, sequence_number, event_type)
        events.append(
            {
                "event_id": event_id,
                "event_type": event_type,
                "aggregate_id": app.aggregate_id,
                "aggregate_type": "LoanApplication",
                "sequence_number": sequence_number,
                "payload": payload,
                "metadata": _event_metadata(app, source_service, app.applicant_id.lower(), causation_id),
                "schema_version": schema_version,
                "occurred_at": occurred_at,
                "recorded_at": isoformat_z(datetime.fromisoformat(occurred_at.replace("Z", "+00:00")) + timedelta(seconds=2)),
            }
        )
        causation_id = event_id
    return events


def generate_week5_records(
    count: int,
    seed: int,
    violations: list[ViolationSpec] | None = None,
    applications: list[ApplicationContext] | None = None,
) -> list[JsonDict]:
    if not applications:
        raise ValueError("week5 generation requires application contexts")
    base_time = datetime(2026, 1, 10, 10, 0, tzinfo=UTC)
    records: list[JsonDict] = []
    for index, app in enumerate(applications):
        records.extend(_application_events(app, base_time + timedelta(hours=index * 3)))
        if len(records) >= count:
            break
    trimmed = records[:count]
    return apply_week5_violations(trimmed, violations or [], seed)


def apply_week5_violations(records: list[JsonDict], violations: list[ViolationSpec], seed: int) -> list[JsonDict]:
    if not violations:
        return records
    mutated = deep_copy_records(records)
    for violation in violations:
        if canonical_system_name(violation.system) != "week5":
            continue
        if violation.type == "sequence_break" and mutated:
            aggregate_id = mutated[0]["aggregate_id"]
            aggregate_records = [record for record in mutated if record.get("aggregate_id") == aggregate_id]
            if len(aggregate_records) >= 3:
                aggregate_records[1]["sequence_number"] = aggregate_records[2]["sequence_number"]
            continue
        target_indices = selected_indices(mutated, violation, seed)
        for index in target_indices:
            record = mutated[index]
            if violation.type == "timestamp_break":
                record["recorded_at"] = isoformat_z(
                    datetime.fromisoformat(record["occurred_at"].replace("Z", "+00:00")) - timedelta(seconds=5)
                )
            elif violation.type == "unregistered_event_type":
                record["event_type"] = "UnmappedEvent"
            elif violation.type == "payload_schema_mismatch":
                if record["event_type"] == "ApplicationSubmitted":
                    record["payload"].pop("requested_amount_usd", None)
                else:
                    record["payload"] = {}
            elif violation.type != "sequence_break":
                raise ValueError(f"unsupported week5 violation: {violation.type}")
    return mutated
