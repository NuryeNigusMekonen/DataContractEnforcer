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
            ("contracts/runner.py", "validate_week3"),
            ("contracts/adapter.py", "SchemaAdapter"),
        ],
    },
    {
        "title": "Enforce verdict enum safety in scoring flow",
        "tags": ["week2", "governance", "enum"],
        "code_refs": [
            ("contracts/runner.py", "validate_week2"),
            ("contracts/common.py", "dataset_semantic_clauses"),
        ],
    },
    {
        "title": "Guarantee lineage edge resolution before publication",
        "tags": ["week4", "lineage", "reliability"],
        "code_refs": [
            ("contracts/lineage.py", "resolve_contract_lineage"),
            ("backend/services/lineage_service.py", "get_lineage_map"),
        ],
    },
    {
        "title": "Protect event ordering at append time",
        "tags": ["week5", "ordering", "ledger"],
        "code_refs": [
            ("contracts/runner.py", "validate_week5"),
            ("scripts/sync_real_week_artifacts.py", "sync_week5"),
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
            ("contracts/runner.py", "validate_traces"),
            ("scripts/sync_real_week_artifacts.py", "sync_traces"),
        ],
    },
    {
        "title": "Preserve doc lineage through compliance and decision stages",
        "tags": ["week3", "week5", "traceability"],
        "code_refs": [
            ("contracts/attributor.py", "attribute_failure"),
            ("contracts/what_if.py", "simulate_what_if"),
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
