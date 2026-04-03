from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from simulators.common import (
    JsonDict,
    ROOT,
    ViolationSpec,
    canonical_system_name,
    deep_copy_records,
    derive_overall_verdict,
    deterministic_uuid,
    isoformat_z,
    mean_score,
    seeded_random,
    selected_indices,
)


CRITERIA: list[tuple[str, str]] = [
    ("contract_traceability", "Source identifiers stay stable across producer handoffs."),
    ("schema_rigor", "Outputs respect field shape, scale, and enum expectations."),
    ("lineage_coverage", "Lineage captures enough graph context for blast-radius analysis."),
    ("replay_readiness", "Records can be replayed safely without hidden mutable dependencies."),
    ("operator_clarity", "Operators can diagnose failures from the emitted payload alone."),
    ("adapter_safety", "Potential schema shifts can be mitigated with safe adapter rules."),
]


def rubric_hash() -> str:
    rubric_path = ROOT / "rubric" / "week2_rubric.json"
    if rubric_path.exists():
        return hashlib.sha256(rubric_path.read_bytes()).hexdigest()
    return hashlib.sha256(b"week2_rubric_missing").hexdigest()


def generate_week2_records(
    count: int,
    seed: int,
    violations: list[ViolationSpec] | None = None,
    target_refs: list[str] | None = None,
) -> list[JsonDict]:
    rng = seeded_random(seed, "week2")
    base_time = datetime(2026, 2, 27, 9, 0, tzinfo=UTC)
    targets = target_refs or [
        "services/week3-document-refinery/extractor.py",
        "services/week2-digital-courtroom/scoring.py",
        "services/week5-event-ledger/append_store.py",
    ]
    records: list[JsonDict] = []
    rubric_id = rubric_hash()
    for index in range(count):
        target = targets[index % len(targets)]
        scores: dict[str, dict[str, Any]] = {}
        baseline = 2.6 + (index % 5) * 0.4
        for criterion_index, (name, description) in enumerate(CRITERIA):
            score = int(round(max(1, min(5, baseline + rng.uniform(-1.0, 1.0) + criterion_index * 0.05))))
            scores[name] = {
                "score": score,
                "evidence": [target, f"generated_contracts/{Path(target).stem}.yaml"],
                "notes": description,
            }
        overall_score = mean_score(scores)
        confidence = round(min(0.98, 0.72 + overall_score / 10 + rng.uniform(-0.03, 0.03)), 2)
        records.append(
            {
                "verdict_id": deterministic_uuid("week2", target, index),
                "target_ref": target,
                "rubric_id": rubric_id,
                "rubric_version": "3.0.0",
                "scores": scores,
                "overall_verdict": derive_overall_verdict(overall_score),
                "overall_score": overall_score,
                "confidence": confidence,
                "evaluated_at": isoformat_z(base_time + timedelta(minutes=index * 9)),
            }
        )
    return apply_week2_violations(records, violations or [], seed)


def apply_week2_violations(records: list[JsonDict], violations: list[ViolationSpec], seed: int) -> list[JsonDict]:
    if not violations:
        return records
    mutated = deep_copy_records(records)
    for violation in violations:
        if canonical_system_name(violation.system) != "week2":
            continue
        for index in selected_indices(mutated, violation, seed):
            record = mutated[index]
            first_key = next(iter(record["scores"]))
            if violation.type == "invalid_overall_verdict":
                record["overall_verdict"] = "REVIEW"
            elif violation.type == "score_out_of_range":
                record["scores"][first_key]["score"] = 7
            elif violation.type == "overall_score_mismatch":
                record["overall_score"] = round(float(record["overall_score"]) + 0.75, 3)
            else:
                raise ValueError(f"unsupported week2 violation: {violation.type}")
    return mutated
