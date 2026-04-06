from __future__ import annotations

import unittest

from backend.services.whatif_service import _shape_what_if


class WhatIfServiceTest(unittest.TestCase):
    def test_shape_what_if_handles_null_adapter_details(self) -> None:
        payload = {
            "simulation_id": "sim-123",
            "contract_id": "week2-verdict-records",
            "raw_changed_status": "PASS",
            "adapter_attempted": False,
            "adapter_status": None,
            "adapter_details": None,
            "compatibility_verdict": "FORWARD_COMPATIBLE",
            "affected_subscribers": None,
        }

        result = _shape_what_if(payload, None)

        self.assertEqual(result["adapter_status"], "NOT_ATTEMPTED")
        self.assertEqual(result["adapter_details"], {})
        self.assertEqual(
            result["adapter_summary"],
            {
                "status": "NOT_ATTEMPTED",
                "rules_applied": 0,
                "failure_reason": None,
                "recovered": False,
            },
        )
        self.assertEqual(result["compatibility_verdict"], "FORWARD_COMPATIBLE")
        self.assertEqual(result["final_verdict"], "FORWARD_COMPATIBLE")
        self.assertEqual(result["affected_systems"], [])
        self.assertEqual(result["affected_systems_count"], 0)

    def test_shape_what_if_merges_direct_and_transitive_systems(self) -> None:
        payload = {
            "simulation_id": "sim-456",
            "contract_id": "week5-event-records",
            "raw_changed_status": "PASS",
            "adapter_attempted": True,
            "adapter_status": "FAIL",
            "adapter_details": {},
            "compatibility_verdict": "BREAKING_REQUIRES_MIGRATION",
            "affected_subscribers": [
                {"subscriber_id": "week7-validation-runner"},
            ],
            "transitive_impacts": [
                {"id": "week5-event-records", "kind": "CONTRACT"},
                {"id": "service::week7-validation-runner", "kind": "SERVICE"},
                {"id": "dataset::outputs/week5/events.jsonl", "kind": "TABLE"},
            ],
        }

        result = _shape_what_if(payload, None)

        self.assertEqual(
            result["affected_systems"],
            ["week7-validation-runner", "week5-event-records", "service::week7-validation-runner"],
        )
        self.assertEqual(result["affected_systems_count"], 3)


if __name__ == "__main__":
    unittest.main()
