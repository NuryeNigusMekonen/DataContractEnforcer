from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

from contracts.evolution import build_compatibility_report


REPO_ROOT = Path(__file__).resolve().parents[1]


class SchemaEvolutionRunnerTest(unittest.TestCase):
    def test_confidence_percentage_is_upcast_before_validation(self) -> None:
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
                "entities.entity_id": {"type": "string", "required": True},
                "extracted_facts.confidence": {
                    "type": "number",
                    "required": True,
                    "minimum": 0.0,
                    "maximum": 1.0,
                },
                "extracted_facts.entity_refs": {"type": "string", "required": False},
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
            "entities": [{"entity_id": "entity-001"}],
            "extracted_facts": [{"confidence": 93, "entity_refs": ["entity-001"]}],
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
            self.assertEqual(report["schema_evolution"]["compatibility_classification"], "breaking_change")
            self.assertTrue(report["adapter"]["attempted"])
            self.assertTrue(report["adapter"]["applied"])
            self.assertTrue(report["adapter"]["fallback_succeeded"])
            self.assertEqual(report["overall_status"], "PASS")
            self.assertEqual(report["failed"], 0)
            transformed = report["adapter"]["transformed_samples"][0]
            self.assertEqual(transformed["extracted_facts"][0]["confidence"], 0.93)

    def test_build_compatibility_report_marks_narrow_type_scale_shift_as_critical_breaking(self) -> None:
        source_contract = {
            "contract_id": "week3-document-refinery-extractions",
            "schema_version": "1.0.0",
            "fields": {
                "extracted_facts.confidence": {
                    "type": "number",
                    "required": True,
                    "minimum": 0.0,
                    "maximum": 1.0,
                }
            },
            "profiling": {
                "statistics": {
                    "extracted_facts.confidence": {"min": 0.21, "max": 0.97, "mean": 0.66, "stddev": 0.15}
                }
            },
        }
        target_contract = {
            "contract_id": "week3-document-refinery-extractions",
            "schema_version": "2.0.0",
            "fields": {
                "extracted_facts.confidence": {
                    "type": "integer",
                    "required": True,
                    "minimum": 0,
                    "maximum": 100,
                }
            },
            "profiling": {
                "statistics": {
                    "extracted_facts.confidence": {"min": 21, "max": 97, "mean": 66.0, "stddev": 15.0}
                }
            },
        }

        report = build_compatibility_report(source_contract, target_contract, str(REPO_ROOT / "contract_registry/subscriptions.yaml"))
        change = report["changes"][0]
        self.assertEqual(report["compatibility_verdict"], "breaking_change")
        self.assertEqual(change["change_type"], "TYPE_NARROWING_SCALE_SHIFT")
        self.assertEqual(change["severity"], "CRITICAL")

    def test_schema_analyzer_builds_migration_report_with_consumer_failure_modes(self) -> None:
        old_contract = {
            "kind": "DataContract",
            "contract_id": "week3-document-refinery-extractions",
            "id": "week3-document-refinery-extractions",
            "schema_version": "1.0.0",
            "dataset": "week3_extractions",
            "source_path": "outputs/week3/extractions.jsonl",
            "info": {"title": "Week 3 Extraction Records", "version": "1.0.0"},
            "fields": {
                "doc_id": {"type": "string", "required": True},
                "extracted_facts.confidence": {"type": "number", "required": True, "minimum": 0.0, "maximum": 1.0},
            },
            "profiling": {
                "statistics": {
                    "extracted_facts.confidence": {"min": 0.12, "max": 0.94, "mean": 0.61, "stddev": 0.17}
                }
            },
        }
        new_contract = {
            "kind": "DataContract",
            "contract_id": "week3-document-refinery-extractions",
            "id": "week3-document-refinery-extractions",
            "schema_version": "2.0.0",
            "dataset": "week3_extractions",
            "source_path": "outputs/week3/extractions.jsonl",
            "info": {"title": "Week 3 Extraction Records", "version": "2.0.0"},
            "fields": {
                "doc_id": {"type": "string", "required": True},
                "extracted_facts.confidence": {"type": "integer", "required": True, "minimum": 0, "maximum": 100},
                "regulatory_basis": {"type": "string", "required": True},
            },
            "profiling": {
                "statistics": {
                    "extracted_facts.confidence": {"min": 12, "max": 94, "mean": 61.0, "stddev": 17.0}
                }
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            snapshot_dir = tmp_dir / "schema_snapshots" / "week3-document-refinery-extractions"
            snapshot_dir.mkdir(parents=True)
            (snapshot_dir / "20260403T100000Z.yaml").write_text(yaml.safe_dump(old_contract, sort_keys=False), encoding="utf-8")
            (snapshot_dir / "20260403T110000Z.yaml").write_text(yaml.safe_dump(new_contract, sort_keys=False), encoding="utf-8")

            contract_registry_dir = tmp_dir / "contract_registry"
            contract_registry_dir.mkdir()
            (contract_registry_dir / "subscriptions.yaml").write_text(
                (REPO_ROOT / "contract_registry" / "subscriptions.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            outputs_dir = tmp_dir / "outputs" / "week4"
            outputs_dir.mkdir(parents=True)
            (outputs_dir / "lineage_snapshots.jsonl").write_text(
                json.dumps(
                    {
                        "nodes": [
                            {
                                "node_id": "dataset::outputs/week3/extractions.jsonl",
                                "label": "extractions.jsonl",
                                "type": "DATASET",
                                "metadata": {"path": "outputs/week3/extractions.jsonl"},
                            },
                            {
                                "node_id": "service::week4-brownfield-cartographer",
                                "label": "week4-brownfield-cartographer",
                                "type": "SERVICE",
                                "metadata": {"path": "services/week4-brownfield-cartographer/main.py"},
                            },
                        ],
                        "edges": [
                            {
                                "source": "dataset::outputs/week3/extractions.jsonl",
                                "target": "service::week4-brownfield-cartographer",
                                "relationship": "CONSUMES",
                                "confidence": 0.95,
                            }
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            output_path = tmp_dir / "report.json"
            result = subprocess.run(
                [
                    "python3",
                    str(REPO_ROOT / "contracts/schema_analyzer.py"),
                    "--contract-id",
                    "week3-document-refinery-extractions",
                    "--since",
                    "2026-04-03T09:00:00+00:00",
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
            migration_report = json.loads(Path(report["migration_impact_report"]).read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "OK")
            self.assertEqual(report["compatibility_verdict"], "breaking_change")
            self.assertTrue(report["consumer_failure_modes"])
            self.assertIn("rollback_plan", report)
            self.assertTrue(migration_report["schema_diff"])
            self.assertTrue(migration_report["blast_radius"])
            self.assertTrue(migration_report["consumer_failure_modes"])
            self.assertEqual(migration_report["consumer_failure_modes"][0]["subscriber_id"], "week4-brownfield-cartographer")


if __name__ == "__main__":
    unittest.main()
