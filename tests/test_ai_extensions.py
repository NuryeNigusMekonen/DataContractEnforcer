from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from contracts.ai_extensions import check_embedding_drift, validate_prompt_inputs


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

    def test_single_entry_point_runs_all_extensions_and_writes_warn_to_violation_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            extraction_path = tmp_dir / "outputs" / "week3" / "extractions.jsonl"
            verdict_path = tmp_dir / "outputs" / "week2" / "verdicts.jsonl"
            extraction_path.parent.mkdir(parents=True, exist_ok=True)
            verdict_path.parent.mkdir(parents=True, exist_ok=True)

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
            self.assertIn("llm_output_schema_rate", report)
            self.assertEqual(report["llm_output_schema_rate"]["status"], "WARN")
            self.assertGreater(report["llm_output_schema_rate"]["violation_rate"], report["llm_output_schema_rate"]["warn_threshold"])

            violation_log_path = tmp_dir / "violation_log" / "violations.jsonl"
            self.assertTrue(violation_log_path.exists())
            logged = [json.loads(line) for line in violation_log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            output_warns = [entry for entry in logged if entry.get("check_id") == "ai.llm_output_schema_rate"]
            self.assertTrue(output_warns)
            self.assertEqual(output_warns[0]["status"], "WARN")
            self.assertEqual(output_warns[0]["candidate_files"], ["outputs/week2/verdicts.jsonl"])


if __name__ == "__main__":
    unittest.main()
