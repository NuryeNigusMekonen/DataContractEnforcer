from __future__ import annotations

import os
import tempfile
from pathlib import Path
import subprocess
import unittest

from contracts.generator import build_contract
from contracts.runner import evaluate_contract_records
from scripts.generate_all_outputs import generate_all_outputs_from_scenario_path
from simulators import generate_scenario_outputs
from simulators.common import (
    JsonDict,
    ScenarioConfig,
    ViolationSpec,
    clear_output_files,
    deterministic_uuid,
    load_jsonl,
    load_scenario,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
LINEAGE_PATH = REPO_ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl"
REGISTRY_PATH = REPO_ROOT / "contract_registry" / "subscriptions.yaml"


def contract_id_for(system: str) -> str:
    return {
        "week1": "week1-intent-records",
        "week2": "week2-verdict-records",
        "week3": "week3-document-refinery-extractions",
        "week4": "week4-lineage-snapshots",
        "week5": "week5-event-records",
        "traces": "langsmith-trace-records",
    }[system]


def file_name_for(system: str) -> str:
    return {
        "week1": "intent_records.jsonl",
        "week2": "verdicts.jsonl",
        "week3": "extractions.jsonl",
        "week4": "lineage_snapshots.jsonl",
        "week5": "events.jsonl",
        "traces": "runs.jsonl",
    }[system]


def validate_dataset(system: str, healthy_records: list[JsonDict], mutated_records: list[JsonDict]) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        tmp_dir = Path(tmp_dir_name)
        data_dir = tmp_dir / "outputs" / system
        data_path = data_dir / file_name_for(system)
        write_jsonl(data_path, healthy_records)
        schema_source = REPO_ROOT / "schemas"
        if not schema_source.exists():
            schema_source = REPO_ROOT / "outputs" / "week5" / "schemas"
        (tmp_dir / "schemas").symlink_to(schema_source, target_is_directory=True)
        (tmp_dir / "rubric").symlink_to(REPO_ROOT / "rubric", target_is_directory=True)
        if system == "week5":
            week5_schema_root = tmp_dir / "outputs" / "week5"
            week5_schema_root.mkdir(parents=True, exist_ok=True)
            week5_schema_source = REPO_ROOT / "outputs" / "week5" / "schemas"
            if not week5_schema_source.exists():
                week5_schema_source = schema_source
            (week5_schema_root / "schemas").symlink_to(week5_schema_source, target_is_directory=True)
        previous_cwd = Path.cwd()
        try:
            os.chdir(tmp_dir)
            contract = build_contract(str(data_path), contract_id_for(system), str(LINEAGE_PATH), str(REGISTRY_PATH))
            return evaluate_contract_records(
                contract,
                mutated_records,
                mode="ENFORCE",
                data_path=str(data_path),
                attempt_adapter=False,
                persist_baselines=False,
            )
        finally:
            os.chdir(previous_cwd)


class SimulationLayerTest(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        clear_output_files()
        generate_all_outputs_from_scenario_path("test_data/scenarios/healthy.yaml", clear_existing=False)

    def test_healthy_scenario_generation(self) -> None:
        scenario = load_scenario("test_data/scenarios/healthy.yaml")
        outputs = generate_scenario_outputs(scenario)
        expected_counts = {"week1": 24, "week2": 24, "week3": 24, "week4": 5, "week5": 60, "traces": 36}
        self.assertEqual({key: len(value) for key, value in outputs.items()}, expected_counts)

        for system, records in outputs.items():
            evaluation = validate_dataset(system, records, records)
            self.assertEqual(evaluation["overall_status"], "PASS", msg=f"{system} failed: {evaluation['results']}")

    def test_deterministic_seed_repeatability(self) -> None:
        scenario = load_scenario("test_data/scenarios/healthy.yaml")
        left = generate_scenario_outputs(scenario)
        right = generate_scenario_outputs(scenario)
        self.assertEqual(left, right)

    def test_jsonl_files_created_in_expected_locations(self) -> None:
        summary = generate_all_outputs_from_scenario_path("test_data/scenarios/healthy.yaml", clear_existing=True)
        self.assertIn("written_files", summary)
        for relative_path in [
            REPO_ROOT / "outputs" / "week1" / "intent_records.jsonl",
            REPO_ROOT / "outputs" / "week2" / "verdicts.jsonl",
            REPO_ROOT / "outputs" / "week3" / "extractions.jsonl",
            REPO_ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl",
            REPO_ROOT / "outputs" / "week5" / "events.jsonl",
            REPO_ROOT / "outputs" / "traces" / "runs.jsonl",
        ]:
            self.assertTrue(relative_path.exists(), msg=str(relative_path))
            self.assertGreater(len(load_jsonl(relative_path)), 0)

    def test_inter_record_references_are_valid_in_healthy_mode(self) -> None:
        scenario = load_scenario("test_data/scenarios/healthy.yaml")
        outputs = generate_scenario_outputs(scenario)

        for record in outputs["week3"]:
            entity_ids = {entity["entity_id"] for entity in record["entities"]}
            for fact in record["extracted_facts"]:
                self.assertTrue(set(fact["entity_refs"]).issubset(entity_ids))

        for snapshot in outputs["week4"]:
            node_ids = {node["node_id"] for node in snapshot["nodes"]}
            for edge in snapshot["edges"]:
                self.assertIn(edge["source"], node_ids)
                self.assertIn(edge["target"], node_ids)

        sequence_groups: dict[str, list[int]] = {}
        for event in outputs["week5"]:
            sequence_groups.setdefault(event["aggregate_id"], []).append(int(event["sequence_number"]))
        for sequence in sequence_groups.values():
            self.assertEqual(sorted(sequence), list(range(1, len(sequence) + 1)))

        for run in outputs["traces"]:
            self.assertEqual(run["total_tokens"], run["prompt_tokens"] + run["completion_tokens"])

    def test_every_supported_violation_type_is_injected_and_detectable(self) -> None:
        healthy = load_scenario("test_data/scenarios/healthy.yaml")
        healthy_outputs = generate_scenario_outputs(healthy)
        cases = [
            ("week1", "confidence_out_of_range"),
            ("week1", "missing_file_path"),
            ("week1", "empty_code_refs"),
            ("week2", "invalid_overall_verdict"),
            ("week2", "score_out_of_range"),
            ("week2", "overall_score_mismatch"),
            ("week3", "confidence_scale_break"),
            ("week3", "invalid_entity_refs"),
            ("week3", "invalid_entity_enum"),
            ("week3", "negative_processing_time"),
            ("week4", "missing_node_ref"),
            ("week4", "invalid_relationship_enum"),
            ("week4", "malformed_git_commit"),
            ("week5", "timestamp_break"),
            ("week5", "sequence_break"),
            ("week5", "unregistered_event_type"),
            ("week5", "payload_schema_mismatch"),
            ("traces", "total_tokens_mismatch"),
            ("traces", "invalid_run_type"),
            ("traces", "end_time_before_start"),
            ("traces", "negative_total_cost"),
        ]
        for system, violation_type in cases:
            with self.subTest(system=system, violation=violation_type):
                broken_scenario = ScenarioConfig(
                    name=f"{system}_{violation_type}",
                    seed=42,
                    counts=healthy.counts,
                    enabled_simulators=healthy.enabled_simulators,
                    violations=(ViolationSpec(system=system, type=violation_type),),
                    healthy=False,
                    clear_existing=False,
                )
                broken_outputs = generate_scenario_outputs(broken_scenario)
                evaluation = validate_dataset(system, healthy_outputs[system], broken_outputs[system])
                self.assertEqual(evaluation["overall_status"], "FAIL", msg=f"{system}/{violation_type} unexpectedly passed")

    def test_generation_script_is_runnable(self) -> None:
        result = subprocess.run(
            [
                "python3",
                str(REPO_ROOT / "scripts" / "generate_all_outputs.py"),
                "--scenario",
                str(REPO_ROOT / "test_data" / "scenarios" / "healthy.yaml"),
                "--clear-existing",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
        self.assertIn('"scenario": "healthy"', result.stdout)


if __name__ == "__main__":
    unittest.main()
