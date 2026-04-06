from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from contracts.common import build_field_clause
from contracts.generator import build_contract, write_contract_files


class ContractGeneratorTest(unittest.TestCase):
    def test_long_error_strings_do_not_become_enum(self) -> None:
        profile = {
            "type": "string",
            "required": False,
            "cardinality": 2,
            "sample_values": [
                "KeyboardInterrupt()Traceback (most recent call last):\n  File \"app.py\", line 1",
                "DomainError()Traceback (most recent call last):\n  File \"worker.py\", line 9",
            ],
        }

        clause = build_field_clause("error", profile)
        self.assertNotIn("enum", clause)

    def test_generator_profiles_contract_and_persists_baselines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            source_dir = tmp_dir / "outputs" / "week3"
            source_dir.mkdir(parents=True, exist_ok=True)
            source_path = source_dir / "extractions.jsonl"
            source_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "doc_id": "11111111-1111-4111-8111-111111111111",
                                "source_hash": "a" * 64,
                                "processing_time_ms": 5,
                                "extracted_at": "2026-04-03T10:00:00Z",
                                "status": "ready",
                                "entities": [{"entity_id": "entity-001", "type": "OTHER"}],
                                "extracted_facts": [
                                    {
                                        "fact_id": "22222222-2222-4222-8222-222222222222",
                                        "confidence": 0.995,
                                        "page_ref": 1,
                                        "source_excerpt": "A source excerpt",
                                        "entity_refs": ["entity-001"],
                                    }
                                ],
                            }
                        ),
                        json.dumps(
                            {
                                "doc_id": "33333333-3333-4333-8333-333333333333",
                                "source_hash": "b" * 64,
                                "processing_time_ms": 7,
                                "extracted_at": "2026-04-03T10:01:00Z",
                                "status": "ready",
                                "entities": [{"entity_id": "entity-002", "type": "PERSON"}],
                                "extracted_facts": [
                                    {
                                        "fact_id": "44444444-4444-4444-8444-444444444444",
                                        "confidence": 0.997,
                                        "page_ref": 2,
                                        "source_excerpt": "Another source excerpt",
                                        "entity_refs": ["entity-002"],
                                    }
                                ],
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            lineage_path = tmp_dir / "lineage.jsonl"
            lineage_path.write_text(
                json.dumps(
                    {
                        "snapshot_id": "snapshot-001",
                        "captured_at": "2026-04-03T10:05:00Z",
                        "nodes": [
                            {
                                "node_id": "dataset::week3-extractions",
                                "label": "extractions.jsonl",
                                "type": "DATASET",
                                "metadata": {"path": str(source_path)},
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
                                "source": "dataset::week3-extractions",
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

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                contract = build_contract(
                    str(source_path),
                    "week3-document-refinery-extractions",
                    str(lineage_path),
                    None,
                )
                contract_path, dbt_path = write_contract_files(contract, str(tmp_dir / "generated_contracts"))
            finally:
                os.chdir(previous_cwd)

            confidence_clause = contract["fields"]["extracted_facts.confidence"]
            self.assertEqual(confidence_clause["minimum"], 0.0)
            self.assertEqual(confidence_clause["maximum"], 1.0)
            self.assertIn("warning", confidence_clause)

            structural = contract["profiling"]["structural"]
            self.assertIn("doc_id", structural["required_fields"])
            self.assertEqual(structural["types"]["extracted_facts.confidence"], "number")

            confidence_stats = contract["profiling"]["statistics"]["extracted_facts.confidence"]
            self.assertEqual(confidence_stats["min"], 0.995)
            self.assertEqual(confidence_stats["max"], 0.997)
            self.assertAlmostEqual(confidence_stats["mean"], 0.996)
            self.assertIn("stddev", confidence_stats)

            baseline_path = tmp_dir / "schema_snapshots" / "baselines.json"
            self.assertTrue(baseline_path.exists())
            baselines = json.loads(baseline_path.read_text(encoding="utf-8"))
            self.assertIn("week3-document-refinery-extractions", baselines)
            persisted_confidence = baselines["week3-document-refinery-extractions"]["columns"]["extracted_facts.confidence"]
            self.assertAlmostEqual(persisted_confidence["mean"], 0.996)
            self.assertIn("stddev", persisted_confidence)

            self.assertIn("status", contract["fields"])
            self.assertIn("llm_annotation", contract["fields"]["status"])
            self.assertTrue(contract["profiling"]["llm_annotations"])

            downstream_ids = [entry["id"] for entry in contract["downstream_consumers"]]
            self.assertIn("service::week4-brownfield-cartographer", downstream_ids)

            self.assertTrue(contract_path.exists())
            self.assertTrue(dbt_path.exists())
            dbt_payload = yaml.safe_load(dbt_path.read_text(encoding="utf-8"))
            column_names = [column["name"] for column in dbt_payload["models"][0]["columns"]]
            self.assertIn("extracted_facts.confidence", column_names)

    def test_write_contract_files_keeps_both_snapshots_with_same_second_timestamp(self) -> None:
        contract = {
            "contract_id": "week3-document-refinery-extractions",
            "dataset": "week3_extractions",
            "fields": {"doc_id": {"type": "string", "required": True}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                with patch("contracts.generator.utc_now", return_value="2026-04-03T23:15:00Z"):
                    write_contract_files(dict(contract), str(tmp_dir / "generated_contracts"))
                    write_contract_files(dict(contract), str(tmp_dir / "generated_contracts"))
            finally:
                os.chdir(previous_cwd)

            snapshot_dir = tmp_dir / "schema_snapshots" / "week3-document-refinery-extractions"
            snapshot_files = sorted(path.name for path in snapshot_dir.glob("*.yaml"))
            self.assertEqual(
                snapshot_files,
                [
                    "20260403T231500Z-01.yaml",
                    "20260403T231500Z.yaml",
                ],
            )


if __name__ == "__main__":
    unittest.main()
