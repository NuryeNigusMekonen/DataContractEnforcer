from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

from contracts.runner import evaluate_contract_records


REPO_ROOT = Path(__file__).resolve().parents[1]


class ValidationRunnerTest(unittest.TestCase):
    def test_report_contains_required_fields_and_missing_columns_become_errors(self) -> None:
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "demo-contract",
            "contract_id": "demo-contract",
            "dataset": "generic",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "Demo Contract"},
            "fields": {
                "present_text": {"type": "string", "required": True},
                "missing_enum": {"type": "string", "required": False, "enum": ["A", "B"]},
                "missing_pattern": {"type": "string", "required": False, "pattern": "^x+$"},
                "missing_numeric": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
            },
            "clauses": [],
        }
        record = {"present_text": "hello"}

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            (tmp_dir / "schema_snapshots").mkdir()
            contract_path = tmp_dir / "contract.yaml"
            data_path = tmp_dir / "data.jsonl"
            output_path = tmp_dir / "report.json"
            contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
            data_path.write_text(json.dumps(record) + "\n", encoding="utf-8")

            result = subprocess.run(
                [
                    "python3",
                    str(REPO_ROOT / "contracts/runner.py"),
                    "--contract",
                    str(contract_path),
                    "--data",
                    str(data_path),
                    "--mode",
                    "AUDIT",
                    "--output",
                    str(output_path),
                ],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
            report = json.loads(output_path.read_text(encoding="utf-8"))
            required_top_level = {
                "report_id",
                "contract_id",
                "snapshot_id",
                "run_timestamp",
                "total_checks",
                "passed",
                "failed",
                "warned",
                "errored",
                "results",
            }
            self.assertTrue(required_top_level.issubset(report.keys()))
            self.assertEqual(report["contract_id"], "demo-contract")
            self.assertEqual(report["errored"], 3)

            for key in ("check_id", "status", "severity", "actual_value", "expected", "message"):
                self.assertIn(key, report["results"][0])

            error_results = {item["check_id"]: item for item in report["results"] if item["status"] == "ERROR"}
            self.assertIn("missing_enum.enum", error_results)
            self.assertIn("missing_pattern.pattern", error_results)
            self.assertIn("missing_numeric.range", error_results)
            self.assertEqual(error_results["missing_enum.enum"]["severity"], "HIGH")
            self.assertEqual(error_results["missing_numeric.range"]["severity"], "CRITICAL")

    def test_confidence_range_and_drift_checks_fire_independently(self) -> None:
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "week3-document-refinery-extractions",
            "contract_id": "week3-document-refinery-extractions",
            "dataset": "week3_extractions",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "Week 3 Extraction Records"},
            "fields": {
                "doc_id": {"type": "string", "required": True},
                "extracted_facts.confidence": {
                    "type": "number",
                    "required": True,
                    "minimum": 0.0,
                    "maximum": 1.0,
                },
            },
            "clauses": [
                {
                    "id": "week3.confidence_unit_scale",
                    "severity": "error",
                    "description": "Confidence must stay on the 0.0-1.0 scale.",
                    "rule": {
                        "type": "numeric_range",
                        "field": "extracted_facts.confidence",
                        "minimum": 0.0,
                        "maximum": 1.0,
                    },
                }
            ],
        }
        record = {
            "doc_id": "doc-001",
            "extracted_facts": [{"confidence": 95}],
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            snapshot_dir = tmp_dir / "schema_snapshots"
            snapshot_dir.mkdir()
            (snapshot_dir / "baselines.json").write_text(
                json.dumps(
                    {
                        "week3-document-refinery-extractions": {
                            "written_at": "2026-04-03T10:00:00Z",
                            "columns": {
                                "extracted_facts.confidence": {"mean": 0.5, "stddev": 0.1},
                            },
                        }
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                report = evaluate_contract_records(
                    contract,
                    [record],
                    mode="AUDIT",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
            finally:
                os.chdir(previous_cwd)

            results = {item["check_id"]: item for item in report["results"]}
            self.assertEqual(results["extracted_facts.confidence.range"]["status"], "FAIL")
            self.assertEqual(results["week3.confidence_unit_scale"]["status"], "FAIL")
            self.assertEqual(results["extracted_facts.confidence.drift"]["status"], "FAIL")
            self.assertTrue(
                str(results["extracted_facts.confidence.drift"]["actual_value"]["baseline_source"]).endswith("baselines.json")
            )

    def test_drift_warning_threshold_and_mode_policies(self) -> None:
        drift_contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "drift-contract",
            "contract_id": "drift-contract",
            "dataset": "generic",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "Drift Contract"},
            "fields": {
                "confidence": {"type": "number", "required": True, "minimum": 0.0, "maximum": 1.0},
            },
            "clauses": [],
        }
        enum_contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "mode-contract",
            "contract_id": "mode-contract",
            "dataset": "generic",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "Mode Contract"},
            "fields": {
                "status": {"type": "string", "required": False, "enum": ["OK"]},
            },
            "clauses": [],
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            snapshot_dir = tmp_dir / "schema_snapshots"
            snapshot_dir.mkdir()
            (snapshot_dir / "baselines.json").write_text(
                json.dumps(
                    {
                        "drift-contract": {
                            "written_at": "2026-04-03T10:00:00Z",
                            "columns": {
                                "confidence": {"mean": 0.5, "stddev": 0.1},
                            },
                        }
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                drift_report = evaluate_contract_records(
                    drift_contract,
                    [{"confidence": 0.75}],
                    mode="AUDIT",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
                audit_report = evaluate_contract_records(
                    enum_contract,
                    [{"status": "BAD"}],
                    mode="AUDIT",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
                warn_report = evaluate_contract_records(
                    enum_contract,
                    [{"status": "BAD"}],
                    mode="WARN",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
                enforce_report = evaluate_contract_records(
                    enum_contract,
                    [{"status": "BAD"}],
                    mode="ENFORCE",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
            finally:
                os.chdir(previous_cwd)

            drift_results = {item["check_id"]: item for item in drift_report["results"]}
            self.assertEqual(drift_results["confidence.drift"]["status"], "WARN")
            self.assertEqual(drift_results["confidence.drift"]["severity"], "WARNING")

            self.assertFalse(audit_report["blocking"])
            self.assertFalse(warn_report["blocking"])
            self.assertTrue(enforce_report["blocking"])


if __name__ == "__main__":
    unittest.main()
