from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from contracts.report_generator import compute_health_score, generate_report


class ReportGeneratorTest(unittest.TestCase):
    def test_compute_health_score_uses_formula_with_critical_penalty(self) -> None:
        reports = [
            {
                "total_checks": 10,
                "passed": 8,
                "results": [
                    {"status": "FAIL", "severity": "CRITICAL"},
                    {"status": "PASS", "severity": "LOW"},
                ],
            }
        ]
        self.assertEqual(compute_health_score(reports), 60)

    def test_generate_report_is_data_driven_and_builds_required_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            reports_dir = tmp_dir / "validation_reports"
            reports_dir.mkdir()
            violations_path = tmp_dir / "violation_log" / "violations.jsonl"
            violations_path.parent.mkdir(parents=True)

            validation_report = {
                "total_checks": 10,
                "passed": 8,
                "failed": 1,
                "warned": 1,
                "errored": 0,
                "results": [
                    {
                        "check_id": "week3.confidence_unit_scale",
                        "status": "FAIL",
                        "severity": "CRITICAL",
                        "column_name": "extracted_facts.confidence",
                    }
                ],
            }
            schema_report = {
                "changes": [
                    {
                        "field_name": "extracted_facts.confidence",
                        "compatibility_class": "breaking_change",
                        "rationale": "scale changed from unit interval to percentages",
                    }
                ]
            }
            ai_report = {
                "embedding_drift": {"status": "PASS"},
                "prompt_input_validation": {"status": "WARN", "quarantined_records": 2},
                "llm_output_schema_rate": {"status": "WARN", "violation_rate": 0.15, "trend": "rising"},
            }
            (reports_dir / "week3.json").write_text(json.dumps(validation_report, indent=2), encoding="utf-8")
            (reports_dir / "schema_evolution.json").write_text(json.dumps(schema_report, indent=2), encoding="utf-8")
            (reports_dir / "ai_extensions.json").write_text(json.dumps(ai_report, indent=2), encoding="utf-8")

            violations = [
                {
                    "contract_id": "week3-document-refinery-extractions",
                    "check_id": "week3.confidence_unit_scale",
                    "field_name": "extracted_facts.confidence",
                    "status": "FAIL",
                    "severity": "CRITICAL",
                    "records_failing": 4,
                    "blame_chain": [{"file_path": "services/week3-document-refinery/confidence_mapper.py"}],
                    "blast_radius": {"affected_nodes": ["week4-brownfield-cartographer", "week7-violation-attributor"]},
                    "contract_path": "generated_contracts/week3-document-refinery-extractions.yaml",
                }
            ]
            violations_path.write_text("\n".join(json.dumps(item) for item in violations) + "\n", encoding="utf-8")

            report = generate_report(reports_dir=str(reports_dir), violations_path=str(violations_path), mode="weekly")

            self.assertIn("data_health_score", report)
            self.assertIn("top_violations", report)
            self.assertIn("schema_changes_detected", report)
            self.assertIn("ai_system_risk_assessment", report)
            self.assertIn("recommendations", report)

            self.assertEqual(report["data_health_score"], 60)
            self.assertTrue(report["top_violations"])
            self.assertIn("week3-document-refinery-extractions", report["top_violations"][0])
            self.assertIn("extracted_facts.confidence", report["top_violations"][0])
            self.assertIn("week4-brownfield-cartographer", report["top_violations"][0])
            self.assertTrue(report["schema_changes_detected"])
            self.assertEqual(report["ai_system_risk_assessment"]["llm_output_schema_rate"]["status"], "WARN")
            self.assertIn("services/week3-document-refinery/confidence_mapper.py", report["recommendations"][0])
            self.assertIn("week3.confidence_unit_scale", report["recommendations"][0])

            updated_violations = [
                {
                    "contract_id": "week5-event-records",
                    "check_id": "week5.sequence_monotonic",
                    "field_name": "sequence_number",
                    "status": "FAIL",
                    "severity": "HIGH",
                    "records_failing": 2,
                    "blame_chain": [{"file_path": "services/week5-event-ledger/sequence_writer.py"}],
                    "blast_radius": {"affected_nodes": ["week7-validation-runner"]},
                    "contract_path": "generated_contracts/week5-event-records.yaml",
                }
            ]
            violations_path.write_text("\n".join(json.dumps(item) for item in updated_violations) + "\n", encoding="utf-8")
            updated_report = generate_report(reports_dir=str(reports_dir), violations_path=str(violations_path), mode="weekly")
            self.assertIn("sequence_writer.py", updated_report["recommendations"][0])
            self.assertNotEqual(report["top_violations"][0], updated_report["top_violations"][0])


if __name__ == "__main__":
    unittest.main()
