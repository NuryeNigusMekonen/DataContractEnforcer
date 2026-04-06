from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import load_jsonl, write_jsonl
from simulators.common import ViolationSpec
from simulators.trace_sim import apply_trace_violations
from simulators.week1_sim import apply_week1_violations
from simulators.week2_sim import apply_week2_violations
from simulators.week3_sim import apply_week3_violations
from simulators.week4_sim import apply_week4_violations
from simulators.week5_sim import apply_week5_violations


SEED = 42
SYSTEM_SPECS = [
    {
        "system": "week1",
        "source": "outputs/week1/intent_records.jsonl",
        "target": "outputs/week1/intent_records_violated.jsonl",
        "applier": apply_week1_violations,
        "violations": [ViolationSpec(system="week1", type="confidence_out_of_range", mode="all_records")],
    },
    {
        "system": "week2",
        "source": "outputs/week2/verdicts.jsonl",
        "target": "outputs/week2/verdicts_violated.jsonl",
        "applier": apply_week2_violations,
        "violations": [ViolationSpec(system="week2", type="invalid_overall_verdict", mode="all_records")],
    },
    {
        "system": "week3",
        "source": "outputs/week3/extractions.jsonl",
        "target": "outputs/week3/extractions_violated.jsonl",
        "applier": apply_week3_violations,
        "violations": [ViolationSpec(system="week3", type="confidence_scale_break", mode="all_records")],
    },
    {
        "system": "week4",
        "source": "outputs/week4/lineage_snapshots.jsonl",
        "target": "outputs/week4/lineage_snapshots_violated.jsonl",
        "applier": apply_week4_violations,
        "violations": [ViolationSpec(system="week4", type="missing_node_ref", mode="all_records")],
    },
    {
        "system": "week5",
        "source": "outputs/week5/events.jsonl",
        "target": "outputs/week5/events_violated.jsonl",
        "applier": apply_week5_violations,
        "violations": [ViolationSpec(system="week5", type="timestamp_break", mode="all_records")],
    },
    {
        "system": "traces",
        "source": "outputs/traces/runs.jsonl",
        "target": "outputs/traces/runs_violated.jsonl",
        "applier": apply_trace_violations,
        "violations": [ViolationSpec(system="traces", type="total_tokens_mismatch", mode="all_records")],
    },
]


def inject_violations_from_outputs(*, seed: int = SEED) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    for spec in SYSTEM_SPECS:
        records = load_jsonl(spec["source"])
        mutated = spec["applier"](records, spec["violations"], seed=seed)
        write_jsonl(spec["target"], mutated)
        summary.append(
            {
                "system": spec["system"],
                "source": spec["source"],
                "target": spec["target"],
                "records": len(records),
                "violations": [violation.to_summary() for violation in spec["violations"]],
            }
        )
    return summary


def main() -> int:
    summary = inject_violations_from_outputs(seed=SEED)

    print("INJECTION MODE: shared simulator violation engine (all week outputs + traces)")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
