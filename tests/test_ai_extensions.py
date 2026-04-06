from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from contracts.ai_extensions import (
    check_embedding_drift,
    check_langsmith_trace_schema_contracts,
    enforce_structured_llm_output,
    validate_prompt_inputs,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class AIExtensionsTest(unittest.TestCase):
    def test_embedding_drift_persists_and_reuses_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            baseline_path = tmp_dir / "schema_snapshots" / "embedding_baseline.json"

            first = check_embedding_drift(["alpha beta", "beta gamma"], baseline_path=str(baseline_path))
            second = check_embedding_drift(["alpha beta", "beta gamma"], baseline_path=str(baseline_path))

            self.assertEqual(first["status"], "BASELINE_SET")
            self.assertTrue(baseline_path.exists())
            self.assertEqual(second["status"], "PASS")
            self.assertEqual(second["drift_score"], 0.0)

    def test_prompt_input_validation_uses_document_metadata_and_quarantines_invalid_records(self) -> None:
        records = [
            {
                "doc_id": "11111111-1111-4111-8111-111111111111",
                "document_metadata": {
                    "doc_id": "11111111-1111-4111-8111-111111111111",
                    "source_path": "docs/a.pdf",
                    "content_preview": "valid preview",
                },
                "extracted_facts": [{"source_excerpt": "ignored because metadata exists"}],
            },
            {
                "doc_id": "22222222-2222-4222-8222-222222222222",
                "document_metadata": {
                    "doc_id": "short-id",
                    "source_path": "",
                    "content_preview": "",
                },
                "extracted_facts": [{"source_excerpt": ""}],
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                result = validate_prompt_inputs(records)
            finally:
                os.chdir(previous_cwd)

            quarantine_path = tmp_dir / "outputs" / "quarantine" / "quarantine.jsonl"
            self.assertEqual(result["status"], "WARN")
            self.assertEqual(result["valid_records"], 1)
            self.assertEqual(result["quarantined_records"], 1)
            self.assertEqual(result["quarantine_path"], "outputs/quarantine/quarantine.jsonl")
            self.assertTrue(quarantine_path.exists())

    def test_structured_llm_output_enforcement_uses_json_schema(self) -> None:
        records = [
            {
                "confidence": 0.97,
                "evaluated_at": "2026-04-05T10:00:00Z",
                "overall_score": 4.0,
                "overall_verdict": "PASS",
                "rubric_id": "103305f35ef32d54bad863ce3e10297bea49e2c3bb69f50516cfff85acfea60e",
                "rubric_version": "3.0.0",
                "scores": {
                    "chief_justice_synthesis": {"evidence": ["a.py"], "notes": "ok", "score": 4},
                    "structured_output_enforcement": {"evidence": ["b.py"], "notes": "ok", "score": 4},
                },
                "target_ref": "contracts/runner.py",
                "verdict_id": "11111111-1111-4111-8111-111111111111",
            },
            {
                "confidence": 0.97,
                "evaluated_at": "2026-04-05T10:05:00Z",
                "overall_score": 4.0,
                "overall_verdict": "REVIEW",
                "rubric_id": "103305f35ef32d54bad863ce3e10297bea49e2c3bb69f50516cfff85acfea60e",
                "rubric_version": "3.0.0",
                "scores": {
                    "chief_justice_synthesis": {"evidence": ["a.py"], "notes": "ok", "score": 4},
                    "structured_output_enforcement": {"evidence": ["b.py"], "notes": "ok", "score": 4},
                },
                "target_ref": "contracts/adapter.py",
                "verdict_id": "22222222-2222-4222-8222-222222222222",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            baseline_path = tmp_dir / "schema_snapshots" / "ai_metrics_baseline.json"
            baseline_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_path.write_text(
                json.dumps({"written_at": "2026-04-05T09:00:00Z", "baseline_violation_rate": 0.0}, indent=2),
                encoding="utf-8",
            )

            result = enforce_structured_llm_output(
                records,
                baseline_path=str(baseline_path),
                warn_threshold=0.02,
                fail_threshold=0.9,
            )

        self.assertEqual(result["status"], "WARN")
        self.assertEqual(result["schema_violations"], 1)
        self.assertEqual(result["valid_outputs"], 1)
        self.assertEqual(result["schema_name"], "week2_structured_verdict_output")
        self.assertEqual(result["sample_errors"][0]["field"], "overall_verdict")

    def test_ai_baselines_default_to_scoped_real_directory(self) -> None:
        valid_verdict = {
            "confidence": 0.97,
            "evaluated_at": "2026-04-05T10:00:00Z",
            "overall_score": 4.0,
            "overall_verdict": "PASS",
            "rubric_id": "103305f35ef32d54bad863ce3e10297bea49e2c3bb69f50516cfff85acfea60e",
            "rubric_version": "3.0.0",
            "scores": {
                "chief_justice_synthesis": {"evidence": ["a.py"], "notes": "ok", "score": 4},
            },
            "target_ref": "contracts/runner.py",
            "verdict_id": "11111111-1111-4111-8111-111111111111",
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                drift = check_embedding_drift(["alpha beta gamma"])
                output = enforce_structured_llm_output([valid_verdict])
            finally:
                os.chdir(previous_cwd)

            self.assertEqual(drift["status"], "BASELINE_SET")
            self.assertEqual(output["status"], "PASS")
            self.assertTrue((tmp_dir / "schema_snapshots" / "real" / "embedding_baseline.json").exists())
            self.assertTrue((tmp_dir / "schema_snapshots" / "real" / "ai_metrics_baseline.json").exists())
            self.assertFalse((tmp_dir / "schema_snapshots" / "embedding_baseline.json").exists())
            self.assertFalse((tmp_dir / "schema_snapshots" / "ai_metrics_baseline.json").exists())

    def test_langsmith_trace_schema_contracts_accept_uuid_v7_ids(self) -> None:
        records = [
            {
                "id": "019d4b7b-d628-7b72-96e5-e4e3f338e52d",
                "name": "week3-refinery-session",
                "run_type": "chain",
                "inputs": {"doc_batch": "batch-1"},
                "outputs": {"status": "completed"},
                "error": None,
                "start_time": "2026-04-05T10:00:00Z",
                "end_time": "2026-04-05T10:00:01Z",
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
                "total_cost": 0.001,
                "tags": ["week3"],
                "parent_run_id": None,
                "session_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            }
        ]

        result = check_langsmith_trace_schema_contracts(records)

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["schema_invalid_records"], 0)

    def test_langsmith_trace_schema_contracts_report_schema_and_contract_failures(self) -> None:
        records = [
            {
                "id": "33333333-3333-4333-8333-333333333333",
                "name": "bad-trace",
                "run_type": "workflow",
                "inputs": {},
                "outputs": {},
                "error": None,
                "start_time": "2026-04-05T10:00:00Z",
                "end_time": "2026-04-05T10:00:01Z",
                "prompt_tokens": 3,
                "completion_tokens": 4,
                "total_tokens": 99,
                "total_cost": 0.001,
                "tags": ["week3"],
                "parent_run_id": None,
                "session_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            }
        ]

        result = check_langsmith_trace_schema_contracts(records)

        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["schema_invalid_records"], 1)
        self.assertIn("traces.total_tokens_add_up", result["failing_check_ids"])
        self.assertIn("traces.run_type_allowed", result["failing_check_ids"])
        self.assertEqual(result["sample_errors"][0]["field"], "run_type")

    def test_single_entry_point_runs_all_extensions_and_writes_warn_to_violation_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            extraction_path = tmp_dir / "outputs" / "week3" / "extractions.jsonl"
            verdict_path = tmp_dir / "outputs" / "week2" / "verdicts.jsonl"
            trace_path = tmp_dir / "outputs" / "traces" / "runs.jsonl"
            extraction_path.parent.mkdir(parents=True, exist_ok=True)
            verdict_path.parent.mkdir(parents=True, exist_ok=True)
            trace_path.parent.mkdir(parents=True, exist_ok=True)

            extraction_records = [
                {
                    "doc_id": "11111111-1111-4111-8111-111111111111",
                    "document_metadata": {
                        "doc_id": "11111111-1111-4111-8111-111111111111",
                        "source_path": "docs/a.pdf",
                        "content_preview": "alpha beta gamma",
                    },
                    "extracted_facts": [{"text": "alpha beta gamma", "source_excerpt": "alpha beta gamma"}],
                }
            ]
            extraction_path.write_text("\n".join(json.dumps(item) for item in extraction_records) + "\n", encoding="utf-8")

            verdict_records = [
                {"overall_verdict": "BROKEN", "verdict_id": "v1"},
                {"overall_verdict": "BROKEN", "verdict_id": "v2"},
                {"overall_verdict": "PASS", "verdict_id": "v3"},
            ]
            verdict_path.write_text("\n".join(json.dumps(item) for item in verdict_records) + "\n", encoding="utf-8")

            trace_records = [
                {
                    "id": "44444444-4444-4444-8444-444444444444",
                    "name": "bad-trace",
                    "run_type": "workflow",
                    "inputs": {},
                    "outputs": {},
                    "error": None,
                    "start_time": "2026-04-05T10:00:00Z",
                    "end_time": "2026-04-05T10:00:01Z",
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 9,
                    "total_cost": 0.001,
                    "tags": ["week3"],
                    "parent_run_id": None,
                    "session_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                }
            ]
            trace_path.write_text("\n".join(json.dumps(item) for item in trace_records) + "\n", encoding="utf-8")

            schema_snapshot_dir = tmp_dir / "schema_snapshots"
            schema_snapshot_dir.mkdir()
            (schema_snapshot_dir / "ai_metrics_baseline.json").write_text(
                json.dumps({"written_at": "2026-04-03T10:00:00Z", "baseline_violation_rate": 0.0}, indent=2),
                encoding="utf-8",
            )

            output_path = tmp_dir / "validation_reports" / "ai_extensions.json"
            result = subprocess.run(
                [
                    "python3",
                    str(REPO_ROOT / "contracts/ai_extensions.py"),
                    "--mode",
                    "all",
                    "--extractions",
                    str(extraction_path),
                    "--verdicts",
                    str(verdict_path),
                    "--traces",
                    str(trace_path),
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
            self.assertIn("embedding_drift", report)
            self.assertIn("prompt_input_validation", report)
            self.assertIn("structured_llm_output_enforcement", report)
            self.assertIn("llm_output_schema_rate", report)
            self.assertIn("langsmith_trace_schema_contracts", report)
            self.assertEqual(report["structured_llm_output_enforcement"]["status"], "FAIL")
            self.assertEqual(report["llm_output_schema_rate"]["status"], "FAIL")
            self.assertEqual(report["langsmith_trace_schema_contracts"]["status"], "FAIL")

            violation_log_path = tmp_dir / "violation_log" / "violations.jsonl"
            self.assertTrue(violation_log_path.exists())
            logged = [json.loads(line) for line in violation_log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            output_warns = [entry for entry in logged if entry.get("check_id") == "ai.structured_llm_output_enforcement"]
            self.assertTrue(output_warns)
            self.assertEqual(output_warns[0]["status"], "FAIL")
            self.assertEqual(output_warns[0]["candidate_files"], ["outputs/week2/verdicts.jsonl"])


if __name__ == "__main__":
    unittest.main()
