from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from contracts.attributor import attribute_failure, build_blame_chain, primary_live_summary_record, render_live_summary


class ViolationAttributorTest(unittest.TestCase):
    def test_build_blame_chain_uses_formula_and_limits_to_five(self) -> None:
        candidates = [
            {"file_path": f"services/week3-document-refinery/file_{index}.py", "lineage_hops": 2, "producer_service": "service::producer"}
            for index in range(6)
        ]

        def fake_commit_records(file_path: str, since: str, limit: int = 1) -> list[dict[str, str]]:
            index = int(Path(file_path).stem.split("_")[-1])
            return [
                {
                    "commit_hash": f"hash-{index}",
                    "author": f"author-{index}@local",
                    "commit_timestamp": f"2026-04-0{index + 1}T00:00:00Z",
                    "commit_message": f"commit {index}",
                }
            ]

        with patch("contracts.attributor.commit_records_for", side_effect=fake_commit_records), patch(
            "contracts.attributor.utc_now", return_value="2026-04-06T00:00:00Z"
        ):
            chain = build_blame_chain(candidates, "14 days ago")

        self.assertEqual(len(chain), 5)
        self.assertEqual(chain[0]["commit_hash"], "hash-5")
        self.assertEqual(chain[0]["confidence_score"], 0.6)
        self.assertEqual(chain[1]["commit_hash"], "hash-4")
        self.assertEqual(chain[1]["confidence_score"], 0.5)
        self.assertEqual(chain[-1]["commit_hash"], "hash-1")
        self.assertEqual(chain[-1]["confidence_score"], 0.2)

    def test_attribute_failure_traverses_lineage_and_builds_blast_radius(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            data_path = tmp_dir / "outputs" / "week3" / "extractions.jsonl"
            data_path.parent.mkdir(parents=True, exist_ok=True)
            data_path.write_text('{"doc_id":"doc-001"}\n', encoding="utf-8")

            service_root = tmp_dir / "services" / "week3-document-refinery"
            service_root.mkdir(parents=True, exist_ok=True)
            confidence_file = service_root / "confidence_mapper.py"
            helper_file = service_root / "extractor.py"
            confidence_file.write_text("CONFIDENCE = True\n", encoding="utf-8")
            helper_file.write_text("def extract():\n    return True\n", encoding="utf-8")

            lineage_snapshot = {
                "nodes": [
                    {
                        "node_id": "dataset::outputs/week3/extractions.jsonl",
                        "label": "extractions.jsonl",
                        "type": "DATASET",
                        "metadata": {"path": str(data_path)},
                    },
                    {
                        "node_id": "service::week3-document-refinery",
                        "label": "week3-document-refinery",
                        "type": "SERVICE",
                        "metadata": {"path": str(service_root / "main.py")},
                    },
                    {
                        "node_id": "file::confidence",
                        "label": "confidence_mapper.py",
                        "type": "FILE",
                        "metadata": {"path": str(confidence_file)},
                    },
                    {
                        "node_id": "file::extractor",
                        "label": "extractor.py",
                        "type": "FILE",
                        "metadata": {"path": str(helper_file)},
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
                        "source": "service::week3-document-refinery",
                        "target": "dataset::outputs/week3/extractions.jsonl",
                        "relationship": "PRODUCES",
                        "confidence": 0.98,
                    },
                    {
                        "source": "dataset::outputs/week3/extractions.jsonl",
                        "target": "service::week4-brownfield-cartographer",
                        "relationship": "CONSUMES",
                        "confidence": 0.95,
                    },
                ],
            }
            failure = {
                "check_id": "week3.confidence_unit_scale",
                "status": "FAIL",
                "severity": "CRITICAL",
                "column_name": "extracted_facts.confidence",
                "message": "Confidence drifted to percentages.",
                "records_failing": 4,
                "samples": ["95"],
            }
            report = {
                "contract_id": "week3-document-refinery-extractions",
                "data_path": str(data_path),
                "schema_evolution": {"compatibility_classification": "breaking_change"},
                "adapter": {"attempted": False, "applied": False, "succeeded": True, "fallback_succeeded": False},
            }

            def fake_commit_records(file_path: str, since: str, limit: int = 1) -> list[dict[str, str]]:
                return [
                    {
                        "commit_hash": "abc123",
                        "author": "owner@local",
                        "commit_timestamp": "2026-04-02T00:00:00Z",
                        "commit_message": f"update {Path(file_path).name}",
                    }
                ]

            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_dir)
                with patch("contracts.attributor.commit_records_for", side_effect=fake_commit_records), patch(
                    "contracts.attributor.utc_now", return_value="2026-04-03T00:00:00Z"
                ):
                    record = attribute_failure(
                        failure,
                        lineage_snapshot,
                        "week3-document-refinery-extractions",
                        None,
                        None,
                        report,
                        "14 days ago",
                    )
            finally:
                os.chdir(previous_cwd)

            self.assertIn("violation_id", record)
            self.assertEqual(record["check_id"], "week3.confidence_unit_scale")
            self.assertEqual(record["detected_at"], "2026-04-03T00:00:00Z")
            self.assertLessEqual(len(record["blame_chain"]), 5)
            self.assertEqual(record["blame_chain"][0]["file_path"], str(confidence_file))
            self.assertEqual(record["blame_chain"][0]["commit_hash"], "abc123")
            self.assertEqual(record["blame_chain"][0]["author"], "owner@local")
            self.assertEqual(record["blame_chain"][0]["commit_timestamp"], "2026-04-02T00:00:00Z")
            self.assertIn("update", record["blame_chain"][0]["commit_message"])
            self.assertEqual(record["blame_chain"][0]["confidence_score"], 0.5)
            self.assertEqual(record["blast_radius"]["affected_nodes"], ["service::week4-brownfield-cartographer"])
            self.assertEqual(record["blast_radius"]["affected_pipelines"], ["service::week4-brownfield-cartographer"])

    def test_render_live_summary_includes_failing_check_commit_and_lineage(self) -> None:
        record = {
            "check_id": "extracted_facts.confidence.range",
            "field_name": "extracted_facts.confidence",
            "blame_chain": [
                {
                    "file_path": "create_violation.py",
                    "commit_hash": "abc123",
                    "author": "owner@local",
                    "rank": 1,
                }
            ],
            "blast_radius": {
                "affected_nodes": [
                    "week4-brownfield-cartographer",
                    "week4-lineage-snapshots",
                    "week7-violation-attributor",
                ],
                "lineage": [
                    {"id": "week4-brownfield-cartographer", "hops": 1},
                    {"id": "week4-lineage-snapshots", "hops": 2},
                    {"id": "week7-violation-attributor", "hops": 3},
                ],
            },
        }

        summary = render_live_summary(record)

        self.assertIn("ViolationAttributor live summary", summary)
        self.assertIn("Failing check: extracted_facts.confidence.range", summary)
        self.assertIn("Top cause: create_violation.py", summary)
        self.assertIn("Commit: abc123", summary)
        self.assertIn("Author: owner@local", summary)
        self.assertIn(
            "Lineage traversal: extracted_facts.confidence.range -> week4-brownfield-cartographer -> week4-lineage-snapshots -> week7-violation-attributor",
            summary,
        )
        self.assertIn("Blast radius: week4-brownfield-cartographer, week4-lineage-snapshots, week7-violation-attributor", summary)

    def test_primary_live_summary_record_prefers_confidence_range(self) -> None:
        records = [
            {"check_id": "extracted_facts.confidence.drift"},
            {"check_id": "week3.confidence_unit_scale"},
            {"check_id": "extracted_facts.confidence.range"},
        ]

        primary = primary_live_summary_record(records)

        self.assertEqual(primary, {"check_id": "extracted_facts.confidence.range"})


if __name__ == "__main__":
    unittest.main()
