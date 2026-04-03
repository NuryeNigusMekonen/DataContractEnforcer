from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest.mock import patch

import yaml

from contracts.attributor import compute_blast_radius


REPO_ROOT = Path(__file__).resolve().parents[1]
SUBSCRIPTIONS_PATH = REPO_ROOT / "contract_registry" / "subscriptions.yaml"
LINEAGE_PATH = REPO_ROOT / "outputs" / "week4" / "lineage_snapshots.jsonl"


class ContractRegistryTest(unittest.TestCase):
    def test_subscriptions_cover_required_dependencies_and_required_fields(self) -> None:
        payload = yaml.safe_load(SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))
        subscriptions = payload["subscriptions"]
        self.assertGreaterEqual(len(subscriptions), 4)

        required_fields = {
            "contract_id",
            "subscriber_id",
            "fields_consumed",
            "breaking_fields",
            "validation_mode",
            "registered_at",
            "contact",
        }
        for entry in subscriptions:
            self.assertTrue(required_fields.issubset(entry.keys()))
            for breaking_field in entry["breaking_fields"]:
                self.assertIn("field", breaking_field)
                self.assertIn("reason", breaking_field)

        required_pairs = {
            ("week3-document-refinery-extractions", "week4-brownfield-cartographer"),
            ("week4-lineage-snapshots", "week7-violation-attributor"),
            ("week5-event-records", "week7-validation-runner"),
            ("langsmith-trace-records", "week7-ai-contract-extension"),
        }
        observed_pairs = {(entry["contract_id"], entry["subscriber_id"]) for entry in subscriptions}
        self.assertTrue(required_pairs.issubset(observed_pairs))

    def test_registry_covers_every_inter_system_consumer_edge(self) -> None:
        subscriptions = yaml.safe_load(SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))["subscriptions"]
        observed_pairs = {(entry["contract_id"], entry["subscriber_id"]) for entry in subscriptions}

        dataset_contracts = {
            "outputs/week1/intent_records.jsonl": "week1-intent-records",
            "outputs/week2/verdicts.jsonl": "week2-verdict-records",
            "outputs/week3/extractions.jsonl": "week3-document-refinery-extractions",
            "outputs/week4/lineage_snapshots.jsonl": "week4-lineage-snapshots",
            "outputs/week5/events.jsonl": "week5-event-records",
            "outputs/traces/runs.jsonl": "langsmith-trace-records",
        }
        service_subscribers = {
            "service::week2-digital-courtroom": "week2-digital-courtroom",
            "service::week4-brownfield-cartographer": "week4-brownfield-cartographer",
            "service::week7-validation-runner": "week7-validation-runner",
            "service::week7-violation-attributor": "week7-violation-attributor",
            "service::week7-ai-contract-extension": "week7-ai-contract-extension",
        }

        records = [json.loads(line) for line in LINEAGE_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
        latest = records[-1]
        lineage_pairs = set()
        for edge in latest["edges"]:
            if edge.get("relationship") != "CONSUMES":
                continue
            source = str(edge.get("source", ""))
            target = str(edge.get("target", ""))
            if not source.startswith("dataset::") or target not in service_subscribers:
                continue
            dataset_path = source.replace("dataset::", "", 1)
            contract_id = dataset_contracts.get(dataset_path)
            if not contract_id:
                continue
            lineage_pairs.add((contract_id, service_subscribers[target]))

        self.assertEqual(lineage_pairs, observed_pairs)

    def test_breaking_fields_align_with_runner_checks_and_registry_is_consulted_first(self) -> None:
        subscriptions = yaml.safe_load(SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))["subscriptions"]
        allowed_breaking_fields = {
            "intent_id",
            "code_refs.file",
            "code_refs.confidence",
            "doc_id",
            "extracted_facts.confidence",
            "nodes.node_id",
            "edges.source",
            "edges.target",
            "event_type",
            "sequence_number",
            "recorded_at",
            "run_type",
            "total_tokens",
            "end_time",
            "overall_verdict",
            "scores.score",
        }
        for entry in subscriptions:
            for breaking_field in entry["breaking_fields"]:
                self.assertIn(breaking_field["field"], allowed_breaking_fields)

        call_order: list[str] = []
        registry_matches = [
            {
                "subscriber_id": "week4-brownfield-cartographer",
                "contact": "week4-team@local",
                "matched_breaking_field": "extracted_facts.confidence",
                "reason": "scale drift",
                "fields_consumed": ["extracted_facts.confidence"],
                "validation_mode": "AUDIT",
                "registered_at": "2026-04-02T15:10:00Z",
            }
        ]
        lineage_blast_radius = {
            "violation_id": "v-1",
            "source": "tier1_transitive",
            "contract_id": "week3-document-refinery-extractions",
            "failing_field": "extracted_facts.confidence",
            "affected_nodes": ["week4-brownfield-cartographer", "week7-violation-attributor"],
            "affected_pipelines": ["week4-brownfield-cartographer", "week7-violation-attributor"],
            "affected_subscribers": [],
            "affected_contracts": [],
            "estimated_records": 2,
            "graph_seeds": ["dataset::outputs/week3/extractions.jsonl"],
            "lineage": [
                {"id": "week4-brownfield-cartographer", "kind": "SUBSCRIBER", "hops": 1, "via": []},
                {"id": "week7-violation-attributor", "kind": "SERVICE", "hops": 2, "via": ["week4-brownfield-cartographer"]},
            ],
        }

        def fake_registry(*args, **kwargs):
            call_order.append("registry")
            return registry_matches

        def fake_lineage(*args, **kwargs):
            call_order.append("lineage")
            return dict(lineage_blast_radius)

        with patch("contracts.attributor.registry_blast_radius", side_effect=fake_registry), patch(
            "contracts.attributor.compute_lineage_blast_radius", side_effect=fake_lineage
        ):
            result = compute_blast_radius(
                contract_id="week3-document-refinery-extractions",
                failing_field="extracted_facts.confidence",
                records_failing=2,
                violation_id="v-1",
                lineage_snapshot={},
                registry_path=str(SUBSCRIPTIONS_PATH),
                contract={"contract_id": "week3-document-refinery-extractions"},
            )

        self.assertEqual(call_order, ["registry", "lineage"])
        self.assertEqual(result["contamination_depth"]["week4-brownfield-cartographer"], 1)
        self.assertEqual(result["contamination_depth"]["week7-violation-attributor"], 2)
        self.assertEqual(result["max_contamination_depth"], 2)


if __name__ == "__main__":
    unittest.main()
