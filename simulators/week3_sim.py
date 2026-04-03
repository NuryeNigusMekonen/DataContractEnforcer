from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from simulators.common import (
    ApplicationContext,
    DOCUMENT_TYPES,
    JsonDict,
    ViolationSpec,
    canonical_system_name,
    clamp,
    deep_copy_records,
    deterministic_uuid,
    fake_model_name,
    fake_sha256,
    isoformat_z,
    seeded_random,
    selected_indices,
)


ENTITY_TYPES = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]


def _financial_entities(app: ApplicationContext, document_type: str) -> list[JsonDict]:
    as_of_date = f"2025-12-{(app.index % 18) + 10:02d}"
    company_entity = deterministic_uuid("entity", app.application_id, "company")
    amount_entity = deterministic_uuid("entity", app.application_id, "amount", document_type)
    date_entity = deterministic_uuid("entity", app.application_id, "date", document_type)
    location_entity = deterministic_uuid("entity", app.application_id, "country")
    return [
        {
            "entity_id": company_entity,
            "name": app.company_name,
            "type": "ORG",
            "role": "applicant",
            "confidence": 0.98,
        },
        {
            "entity_id": amount_entity,
            "name": f"${app.requested_amount_usd:,.2f}",
            "type": "AMOUNT",
            "role": "requested_amount",
            "confidence": 0.94,
        },
        {
            "entity_id": date_entity,
            "name": as_of_date,
            "type": "DATE",
            "role": "statement_as_of",
            "confidence": 0.95,
        },
        {
            "entity_id": location_entity,
            "name": app.country,
            "type": "LOCATION",
            "role": "jurisdiction",
            "confidence": 0.92,
        },
    ]


def _extracted_facts(app: ApplicationContext, document_type: str, entities: list[JsonDict]) -> list[JsonDict]:
    entity_lookup = {entity["role"]: entity["entity_id"] for entity in entities if "role" in entity}
    facts_catalog = [
        (
            "total_revenue",
            app.financials["total_revenue"],
            2,
            f"{app.company_name} reported total revenue of ${app.financials['total_revenue']:,.2f}.",
        ),
        (
            "net_income",
            app.financials["net_income"],
            3,
            f"Net income for {app.company_name} closed at ${app.financials['net_income']:,.2f}.",
        ),
        (
            "total_assets",
            app.financials["total_assets"],
            4,
            f"Total assets reached ${app.financials['total_assets']:,.2f} at the close of the reporting period.",
        ),
        (
            "total_liabilities",
            app.financials["total_liabilities"],
            4,
            f"Total liabilities were recorded at ${app.financials['total_liabilities']:,.2f}.",
        ),
    ]
    if document_type == "compliance_memo":
        facts_catalog.append(
            (
                "jurisdiction",
                0.0,
                1,
                f"The filing package is subject to {app.country} commercial lending and AML controls.",
            )
        )
    facts: list[JsonDict] = []
    for fact_index, (label, _numeric_value, page_ref, text) in enumerate(facts_catalog):
        refs = [entity_lookup["applicant"], entity_lookup["statement_as_of"]]
        if label != "jurisdiction":
            refs.append(entity_lookup["requested_amount"])
        else:
            refs.append(entity_lookup["jurisdiction"])
        facts.append(
            {
                "fact_id": deterministic_uuid("fact", app.application_id, document_type, label),
                "text": text,
                "source_excerpt": text,
                "page_ref": page_ref + fact_index % 2,
                "entity_refs": refs,
                "confidence": round(clamp(0.79 + fact_index * 0.04 + app.index * 0.005, 0.78, 0.98), 2),
            }
        )
    return facts


def generate_week3_records(
    count: int,
    seed: int,
    violations: list[ViolationSpec] | None = None,
    applications: list[ApplicationContext] | None = None,
) -> list[JsonDict]:
    if not applications:
        raise ValueError("week3 generation requires application contexts")
    rng = seeded_random(seed, "week3")
    base_time = datetime(2026, 3, 7, 9, 0, tzinfo=UTC)
    records: list[JsonDict] = []
    for index in range(count):
        app = applications[index % len(applications)]
        document_type = DOCUMENT_TYPES[index % len(DOCUMENT_TYPES)]
        entities = _financial_entities(app, document_type)
        extracted_at = base_time + timedelta(minutes=index * 17)
        input_tokens = 650 + (index % 7) * 80 + app.index * 3
        output_tokens = 130 + (index % 5) * 22
        processing_time_ms = 180 + index * 11 + (index % 4) * 23
        source_path = app.refinery_paths[document_type]
        records.append(
            {
                "doc_id": deterministic_uuid("doc", app.application_id, document_type),
                "source_path": source_path,
                "source_hash": fake_sha256(source_path, app.source_hash),
                "extracted_facts": _extracted_facts(app, document_type, entities),
                "entities": entities,
                "extraction_model": fake_model_name(rng, "extraction"),
                "processing_time_ms": processing_time_ms,
                "token_count": {"input": input_tokens, "output": output_tokens},
                "extracted_at": isoformat_z(extracted_at),
            }
        )
    return apply_week3_violations(records, violations or [], seed)


def apply_week3_violations(records: list[JsonDict], violations: list[ViolationSpec], seed: int) -> list[JsonDict]:
    if not violations:
        return records
    mutated = deep_copy_records(records)
    for violation in violations:
        if canonical_system_name(violation.system) != "week3":
            continue
        for index in selected_indices(mutated, violation, seed):
            record = mutated[index]
            if violation.type == "confidence_scale_break":
                for fact in record.get("extracted_facts", []):
                    fact["confidence"] = round(float(fact["confidence"]) * 100, 1)
            elif violation.type == "invalid_entity_refs":
                if record.get("extracted_facts"):
                    record["extracted_facts"][0]["entity_refs"] = [deterministic_uuid("missing-entity", index)]
            elif violation.type == "invalid_entity_enum":
                if record.get("entities"):
                    record["entities"][0]["type"] = "COMPANY"
            elif violation.type == "negative_processing_time":
                record["processing_time_ms"] = -25
            else:
                raise ValueError(f"unsupported week3 violation: {violation.type}")
    return mutated

