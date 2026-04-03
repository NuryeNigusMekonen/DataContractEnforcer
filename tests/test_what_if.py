from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

import yaml

from contracts.generator import build_contract
from contracts.what_if import simulate_what_if
from scripts.generate_all_outputs import generate_all_outputs_from_scenario_path


REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO_ROOT / "contract_registry/subscriptions.yaml"
ADAPTER_PATH = REPO_ROOT / "contract_registry/adapters.yaml"
LINEAGE_PATH = REPO_ROOT / "outputs/week4/lineage_snapshots.jsonl"


class WhatIfSimulationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        generate_all_outputs_from_scenario_path("test_data/scenarios/healthy.yaml", clear_existing=True)
        cls._tmp_dir_context = tempfile.TemporaryDirectory()
        cls.tmp_dir = Path(cls._tmp_dir_context.name)
        (cls.tmp_dir / "schemas").symlink_to(REPO_ROOT / "schemas", target_is_directory=True)
        (cls.tmp_dir / "rubric").symlink_to(REPO_ROOT / "rubric", target_is_directory=True)
        (cls.tmp_dir / "schema_snapshots").mkdir()
        cls.contract_paths = {
            "week2": cls._write_contract("outputs/week2/verdicts.jsonl", "week2-verdict-records", "week2-verdict-records.yaml"),
            "week3": cls._write_contract(
                "outputs/week3/extractions.jsonl",
                "week3-document-refinery-extractions",
                "week3-document-refinery-extractions.yaml",
            ),
            "week5": cls._write_contract("outputs/week5/events.jsonl", "week5-event-records", "week5-event-records.yaml"),
        }

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp_dir_context.cleanup()

    @classmethod
    def _write_contract(cls, source_path: str, contract_id: str, filename: str) -> Path:
        contract = build_contract(str(REPO_ROOT / source_path), contract_id, str(LINEAGE_PATH), str(REGISTRY_PATH))
        path = cls.tmp_dir / filename
        path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
        return path

    def _simulate(self, *, contract_path: Path, data_path: Path, change_spec_path: Path) -> dict[str, object]:
        previous_cwd = Path.cwd()
        try:
            os.chdir(self.tmp_dir)
            return simulate_what_if(
                contract_path=contract_path,
                data_path=data_path,
                change_spec_path=change_spec_path,
                adapter_config=str(ADAPTER_PATH),
                lineage_path=str(LINEAGE_PATH),
                registry_path=str(REGISTRY_PATH),
            )
        finally:
            os.chdir(previous_cwd)

    def test_week3_confidence_scale_change_is_breaking_but_adaptable(self) -> None:
        report = self._simulate(
            contract_path=self.contract_paths["week3"],
            data_path=REPO_ROOT / "outputs/week3/extractions.jsonl",
            change_spec_path=REPO_ROOT / "test_data/changes/week3_confidence_scale_change.json",
        )

        self.assertEqual(report["baseline_status"], "PASS")
        self.assertEqual(report["raw_changed_status"], "FAIL")
        self.assertTrue(report["adapter_attempted"])
        self.assertEqual(report["adapter_status"], "PASS")
        self.assertEqual(report["compatibility_verdict"], "BREAKING_BUT_ADAPTABLE")
        self.assertIn("week4-brownfield-cartographer", [item["subscriber_id"] for item in report["affected_subscribers"]])
        self.assertTrue(any(item["id"] == "week7-violation-attributor" for item in report["transitive_impacts"]))

    def test_week2_enum_addition_is_forward_compatible(self) -> None:
        report = self._simulate(
            contract_path=self.contract_paths["week2"],
            data_path=REPO_ROOT / "outputs/week2/verdicts.jsonl",
            change_spec_path=REPO_ROOT / "test_data/changes/week2_verdict_enum_addition.json",
        )

        self.assertEqual(report["baseline_status"], "PASS")
        self.assertEqual(report["raw_changed_status"], "PASS")
        self.assertFalse(report["adapter_attempted"])
        self.assertEqual(report["compatibility_verdict"], "FORWARD_COMPATIBLE")
        self.assertIn("week7-ai-contract-extension", [item["subscriber_id"] for item in report["affected_subscribers"]])

    def test_week5_required_field_addition_requires_migration(self) -> None:
        report = self._simulate(
            contract_path=self.contract_paths["week5"],
            data_path=REPO_ROOT / "outputs/week5/events.jsonl",
            change_spec_path=REPO_ROOT / "test_data/changes/week5_required_regulatory_basis.json",
        )

        self.assertEqual(report["baseline_status"], "PASS")
        self.assertEqual(report["raw_changed_status"], "PASS")
        self.assertEqual(report["compatibility_verdict"], "BREAKING_REQUIRES_MIGRATION")
        self.assertIn("migration", report["recommended_action"].lower())


if __name__ == "__main__":
    unittest.main()
