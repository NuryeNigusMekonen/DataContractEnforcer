from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from simulators.common import (
    JsonDict,
    ViolationSpec,
    canonical_system_name,
    clamp,
    deep_copy_records,
    deterministic_uuid,
    isoformat_z,
    seeded_random,
    selected_indices,
)


INTENT_TEMPLATES: list[dict[str, Any]] = [
    {
        "title": "Stabilize extraction confidence normalization",
        "tags": ["week3", "quality", "confidence"],
        "code_refs": [
            ("services/week3-document-refinery/extractor.py", "extract_financial_facts"),
            ("services/week3-document-refinery/normalization.py", "normalize_confidence"),
        ],
    },
    {
        "title": "Enforce verdict enum safety in scoring flow",
        "tags": ["week2", "governance", "enum"],
        "code_refs": [
            ("services/week2-digital-courtroom/scoring.py", "score_submission"),
            ("services/week2-digital-courtroom/rubric_loader.py", "load_rubric"),
        ],
    },
    {
        "title": "Guarantee lineage edge resolution before publication",
        "tags": ["week4", "lineage", "reliability"],
        "code_refs": [
            ("services/week4-brownfield-cartographer/graph_builder.py", "build_edges"),
            ("services/week4-brownfield-cartographer/node_catalog.py", "materialize_nodes"),
        ],
    },
    {
        "title": "Protect event ordering at append time",
        "tags": ["week5", "ordering", "ledger"],
        "code_refs": [
            ("services/week5-event-ledger/append_store.py", "append_event"),
            ("services/week5-event-ledger/sequence_guard.py", "validate_sequence"),
        ],
    },
    {
        "title": "Surface adapter recovery telemetry in validation runner",
        "tags": ["week7", "adapter", "telemetry"],
        "code_refs": [
            ("contracts/runner.py", "evaluate_contract_records"),
            ("contracts/adapter.py", "SchemaAdapter"),
        ],
    },
    {
        "title": "Backfill token-cost accounting in LangSmith exports",
        "tags": ["traces", "cost", "observability"],
        "code_refs": [
            ("services/trace-exporter/costing.py", "estimate_total_cost"),
            ("services/trace-exporter/export_runs.py", "export_runs"),
        ],
    },
    {
        "title": "Preserve doc lineage through compliance and decision stages",
        "tags": ["week3", "week5", "traceability"],
        "code_refs": [
            ("services/week3-document-refinery/provenance.py", "attach_provenance"),
            ("services/week2-digital-courtroom/decision_flow.py", "render_decision"),
        ],
    },
]

STATUS_VARIANTS = ["planned rollout", "active implementation", "verification checkpoint", "release candidate"]


def generate_week1_records(count: int, seed: int, violations: list[ViolationSpec] | None = None) -> list[JsonDict]:
    rng = seeded_random(seed, "week1")
    base_time = datetime(2026, 2, 20, 8, 30, tzinfo=UTC)
    records: list[JsonDict] = []
    for index in range(count):
        template = INTENT_TEMPLATES[index % len(INTENT_TEMPLATES)]
        status = STATUS_VARIANTS[index % len(STATUS_VARIANTS)]
        created_at = base_time + timedelta(hours=index * 5)
        code_refs = []
        for ref_index, (file_path, symbol) in enumerate(template["code_refs"]):
            line_start = 10 + (index * 7 + ref_index * 13) % 180
            line_end = line_start + 4 + (ref_index % 3)
            code_refs.append(
                {
                    "file": file_path,
                    "line_start": line_start,
                    "line_end": line_end,
                    "symbol": symbol,
                    "confidence": round(clamp(rng.uniform(0.82, 0.98) - ref_index * 0.02, 0.75, 0.99), 2),
                }
            )
        records.append(
            {
                "intent_id": deterministic_uuid("week1", index, template["title"]),
                "description": f"{template['title']}: {status} for Week 7 producer-contract simulation.",
                "code_refs": code_refs,
                "governance_tags": [*template["tags"], "simulation"],
                "created_at": isoformat_z(created_at),
            }
        )
    return apply_week1_violations(records, violations or [], seed)


def apply_week1_violations(records: list[JsonDict], violations: list[ViolationSpec], seed: int) -> list[JsonDict]:
    if not violations:
        return records
    mutated = deep_copy_records(records)
    for violation in violations:
        if canonical_system_name(violation.system) != "week1":
            continue
        for index in selected_indices(mutated, violation, seed):
            record = mutated[index]
            if violation.type == "confidence_out_of_range":
                if record.get("code_refs"):
                    record["code_refs"][0]["confidence"] = 1.27
            elif violation.type == "missing_file_path":
                for code_ref in record.get("code_refs", []):
                    code_ref.pop("file", None)
            elif violation.type == "empty_code_refs":
                record["code_refs"] = []
            else:
                raise ValueError(f"unsupported week1 violation: {violation.type}")
    return mutated

