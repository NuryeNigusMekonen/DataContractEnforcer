from __future__ import annotations

from typing import Any

from simulators.common import (
    JsonDict,
    ScenarioConfig,
    ViolationSpec,
    build_application_catalog,
    group_violations_by_system,
    load_jsonl,
    output_path_for_system,
    outputs_summary,
    required_application_count,
    write_jsonl,
)
from simulators.trace_sim import apply_trace_violations, generate_trace_records
from simulators.week1_sim import apply_week1_violations, generate_week1_records
from simulators.week2_sim import apply_week2_violations, generate_week2_records
from simulators.week3_sim import apply_week3_violations, generate_week3_records
from simulators.week4_sim import apply_week4_violations, generate_week4_records
from simulators.week5_sim import apply_week5_violations, generate_week5_records


RecordList = list[JsonDict]


APPLIERS = {
    "week1": apply_week1_violations,
    "week2": apply_week2_violations,
    "week3": apply_week3_violations,
    "week4": apply_week4_violations,
    "week5": apply_week5_violations,
    "traces": apply_trace_violations,
}


def collect_week2_targets(week1_records: RecordList, week4_records: RecordList) -> list[str]:
    targets: list[str] = []
    for record in week1_records:
        for code_ref in record.get("code_refs", []):
            file_path = code_ref.get("file")
            if isinstance(file_path, str) and file_path not in targets:
                targets.append(file_path)
    for snapshot in week4_records[:1]:
        for node in snapshot.get("nodes", []):
            path = node.get("metadata", {}).get("path")
            if isinstance(path, str) and path not in targets and path.startswith(("services/", "contracts/")):
                targets.append(path)
    return targets


def generate_scenario_outputs(scenario: ScenarioConfig) -> dict[str, RecordList]:
    counts = scenario.counts
    violations = group_violations_by_system(scenario.violations)
    applications = build_application_catalog(required_application_count(counts), scenario.seed)
    outputs: dict[str, RecordList] = {}

    if "week1" in scenario.enabled_simulators:
        outputs["week1"] = generate_week1_records(counts["week1"], scenario.seed, violations.get("week1"))
    else:
        outputs["week1"] = []

    if "week3" in scenario.enabled_simulators:
        outputs["week3"] = generate_week3_records(counts["week3"], scenario.seed, violations.get("week3"), applications)
    else:
        outputs["week3"] = []

    if "week4" in scenario.enabled_simulators:
        outputs["week4"] = generate_week4_records(
            counts["week4"],
            scenario.seed,
            violations.get("week4"),
            outputs["week1"],
            outputs["week3"],
        )
    else:
        outputs["week4"] = []

    if "week2" in scenario.enabled_simulators:
        outputs["week2"] = generate_week2_records(
            counts["week2"],
            scenario.seed,
            violations.get("week2"),
            collect_week2_targets(outputs["week1"], outputs["week4"]),
        )
    else:
        outputs["week2"] = []

    if "week5" in scenario.enabled_simulators:
        outputs["week5"] = generate_week5_records(counts["week5"], scenario.seed, violations.get("week5"), applications)
    else:
        outputs["week5"] = []

    if "traces" in scenario.enabled_simulators:
        outputs["traces"] = generate_trace_records(
            counts["traces"],
            scenario.seed,
            violations.get("traces"),
            applications,
            outputs["week2"],
            outputs["week3"],
            outputs["week5"],
        )
    else:
        outputs["traces"] = []

    return outputs


def write_generated_outputs(records_by_system: dict[str, RecordList]) -> dict[str, str]:
    written: dict[str, str] = {}
    for system, records in records_by_system.items():
        path = output_path_for_system(system)
        write_jsonl(path, records)
        written[system] = str(path)
    return written


def load_output_records(system: str) -> RecordList:
    return load_jsonl(output_path_for_system(system))


def apply_violations_to_current_outputs(violations_by_system: dict[str, list[ViolationSpec]], seed: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    for system, violations in violations_by_system.items():
        records = load_output_records(system)
        if not records:
            raise FileNotFoundError(f"no current output records found for {system}: {output_path_for_system(system)}")
        mutated = APPLIERS[system](records, violations, seed)
        write_jsonl(output_path_for_system(system), mutated)
        counts[system] = len(mutated)
    return counts


def scenario_summary(scenario: ScenarioConfig, records_by_system: dict[str, RecordList]) -> dict[str, Any]:
    return {
        "scenario": scenario.name,
        "seed": scenario.seed,
        "healthy": scenario.healthy,
        "systems_generated": list(records_by_system),
        "record_counts": outputs_summary(records_by_system),
        "violations": [violation.to_summary() for violation in scenario.violations],
    }

