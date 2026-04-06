from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

from contracts.common import sha256_file
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

    def test_uuid_v7_values_pass_uuid_format_validation(self) -> None:
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "trace-contract",
            "contract_id": "trace-contract",
            "dataset": "generic",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "Trace Contract"},
            "fields": {
                "id": {"type": "string", "required": True, "format": "uuid"},
            },
            "clauses": [],
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            (tmp_dir / "schema_snapshots").mkdir()

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                report = evaluate_contract_records(
                    contract,
                    [{"id": "019d4b7b-d628-7b72-96e5-e4e3f338e52d"}],
                    mode="AUDIT",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
            finally:
                os.chdir(previous_cwd)

        results = {item["check_id"]: item for item in report["results"]}
        self.assertEqual(results["id.format"]["status"], "PASS")

    def test_confidence_range_and_drift_checks_fire_independently(self) -> None:
        rules_hash = sha256_file(REPO_ROOT / "artifacts/week3/extraction_rules.yaml")
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
            "extraction_rules_hash": rules_hash,
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
            self.assertEqual(results["week3.extraction_rules_hash_exists"]["status"], "PASS")
            self.assertTrue(
                str(results["extracted_facts.confidence.drift"]["actual_value"]["baseline_source"]).endswith("baselines.json")
            )

    def test_cli_can_disable_adapter_and_block_on_raw_confidence_range_violation(self) -> None:
        rules_hash = sha256_file(REPO_ROOT / "artifacts/week3/extraction_rules.yaml")
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
            "extraction_rules_hash": rules_hash,
            "extracted_facts": [{"confidence": 95}],
        }

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
                    "ENFORCE",
                    "--no-adapter",
                    "--output",
                    str(output_path),
                ],
                cwd=tmp_dir,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 2, msg=result.stderr or result.stdout)
            report = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertFalse(report["adapter"]["attempted"])
            self.assertTrue(report["blocking"])
            results = {item["check_id"]: item for item in report["results"]}
            self.assertEqual(results["extracted_facts.confidence.range"]["status"], "FAIL")
            self.assertEqual(results["week3.confidence_unit_scale"]["status"], "FAIL")

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

    def test_trace_validation_enforces_producer_specific_rules_from_tags(self) -> None:
        trace_contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "langsmith-trace-records",
            "contract_id": "langsmith-trace-records",
            "dataset": "traces",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "LangSmith Trace Records"},
            "fields": {},
            "clauses": [],
        }
        records = [
            {
                "id": "11111111-1111-4111-8111-111111111111",
                "name": "week3-refinery-session",
                "run_type": "chain",
                "inputs": {"doc_batch": "batch-1"},
                "outputs": {"status": "completed"},
                "error": None,
                "start_time": "2026-04-04T10:00:00Z",
                "end_time": "2026-04-04T10:00:01Z",
                "total_tokens": 10,
                "prompt_tokens": 7,
                "completion_tokens": 3,
                "total_cost": 0.001,
                "tags": ["week3", "orchestration"],
                "parent_run_id": None,
                "session_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            },
            {
                "id": "22222222-2222-4222-8222-222222222222",
                "name": "generic-week4-step",
                "run_type": "chain",
                "inputs": {"unexpected": True},
                "outputs": {},
                "error": None,
                "start_time": "2026-04-04T10:00:02Z",
                "end_time": "2026-04-04T10:00:03Z",
                "total_tokens": 8,
                "prompt_tokens": 5,
                "completion_tokens": 3,
                "total_cost": 0.001,
                "tags": ["week4"],
                "parent_run_id": None,
                "session_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            },
            {
                "id": "33333333-3333-4333-8333-333333333333",
                "name": "prepare_output",
                "run_type": "chain",
                "inputs": {
                    "command": {"application_id": "app-1"},
                    "context_snapshot": {"document_count": 1},
                },
                "outputs": {"command": {"application_id": "app-1"}},
                "error": None,
                "start_time": "2026-04-04T10:00:04Z",
                "end_time": "2026-04-04T10:00:05Z",
                "total_tokens": 12,
                "prompt_tokens": 9,
                "completion_tokens": 3,
                "total_cost": 0.001,
                "tags": ["week5", "ledger"],
                "parent_run_id": None,
                "session_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            (tmp_dir / "schema_snapshots").mkdir()

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                report = evaluate_contract_records(
                    trace_contract,
                    records,
                    mode="AUDIT",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
            finally:
                os.chdir(previous_cwd)

        results = {item["check_id"]: item for item in report["results"]}
        self.assertEqual(results["traces.week3.row_shape"]["status"], "PASS")
        self.assertEqual(results["traces.week4.row_shape"]["status"], "FAIL")
        self.assertEqual(results["traces.week5.row_shape"]["status"], "PASS")
        self.assertEqual(results["traces.producer_classification.coverage"]["status"], "PASS")

    def test_trace_validation_infers_producers_from_payload_shape(self) -> None:
        trace_contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": "langsmith-trace-records",
            "contract_id": "langsmith-trace-records",
            "dataset": "traces",
            "schema_version": "1.0.0",
            "info": {"version": "1.0.0", "title": "LangSmith Trace Records"},
            "fields": {},
            "clauses": [],
        }
        records = [
            {
                "id": "44444444-4444-4444-8444-444444444444",
                "name": "route",
                "run_type": "chain",
                "inputs": {
                    "doc_id": "doc-1",
                    "question": "What changed?",
                    "tool_trace": [],
                },
                "outputs": {"route": "semantic", "semantic_query": "What changed?"},
                "error": None,
                "start_time": "2026-04-04T10:10:00Z",
                "end_time": "2026-04-04T10:10:01Z",
                "total_tokens": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_cost": 0.0,
                "tags": ["graph:step:1"],
                "parent_run_id": None,
                "session_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            },
            {
                "id": "55555555-5555-4555-8555-555555555555",
                "name": "trace_lineage",
                "run_type": "chain",
                "inputs": {
                    "arg": "dataset::orders",
                    "direction": "upstream",
                    "tool": "trace_lineage",
                },
                "outputs": {"result": {"node_count": 0}},
                "error": None,
                "start_time": "2026-04-04T10:10:02Z",
                "end_time": "2026-04-04T10:10:03Z",
                "total_tokens": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_cost": 0.0,
                "tags": ["graph:step:2"],
                "parent_run_id": None,
                "session_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            },
            {
                "id": "66666666-6666-4666-8666-666666666666",
                "name": "prepare_output",
                "run_type": "chain",
                "inputs": {
                    "command": {"application_id": "app-2"},
                    "metrics": {"total_nodes_executed": 5},
                    "context_snapshot": {"document_count": 1},
                },
                "outputs": {"command": {"application_id": "app-2"}},
                "error": None,
                "start_time": "2026-04-04T10:10:04Z",
                "end_time": "2026-04-04T10:10:05Z",
                "total_tokens": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_cost": 0.0,
                "tags": ["graph:step:3"],
                "parent_run_id": None,
                "session_id": "ffffffff-ffff-4fff-8fff-ffffffffffff",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            (tmp_dir / "schema_snapshots").mkdir()

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                report = evaluate_contract_records(
                    trace_contract,
                    records,
                    mode="AUDIT",
                    attempt_adapter=False,
                    persist_baselines=False,
                )
            finally:
                os.chdir(previous_cwd)

        results = {item["check_id"]: item for item in report["results"]}
        self.assertEqual(results["traces.week3.row_shape"]["status"], "PASS")
        self.assertEqual(results["traces.week4.row_shape"]["status"], "PASS")
        self.assertEqual(results["traces.week5.row_shape"]["status"], "PASS")
        self.assertEqual(results["traces.producer_classification.coverage"]["status"], "PASS")
        self.assertEqual(
            results["traces.producer_classification.coverage"]["actual_value"],
            {"week3": 1, "week4": 1, "week5": 1, "other": 0},
        )


if __name__ == "__main__":
    unittest.main()
