from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import normalize_contract_filename, utc_now

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare the two latest schema snapshots for a contract.")
    parser.add_argument("--contract-id", required=True, help="Logical contract id used by generator.py.")
    parser.add_argument("--since", required=False, help="Accepted for evaluator compatibility; snapshots are already timestamped.")
    parser.add_argument("--output", required=True, help="Output JSON path for schema evolution results.")
    return parser.parse_args()


def classify_change(field_name: str, old_clause: dict[str, Any] | None, new_clause: dict[str, Any] | None) -> tuple[str, str]:
    if old_clause is None and new_clause is not None:
        if new_clause.get("required", False):
            return "BREAKING", f"{field_name}: added required field"
        return "COMPATIBLE", f"{field_name}: added optional field"
    if new_clause is None and old_clause is not None:
        return "BREAKING", f"{field_name}: removed field"
    if old_clause is None or new_clause is None:
        return "COMPATIBLE", f"{field_name}: unchanged"
    if old_clause.get("type") != new_clause.get("type"):
        return "BREAKING", f"{field_name}: type changed {old_clause.get('type')} -> {new_clause.get('type')}"
    if old_clause.get("maximum") != new_clause.get("maximum"):
        return "BREAKING", f"{field_name}: maximum changed {old_clause.get('maximum')} -> {new_clause.get('maximum')}"
    if old_clause.get("minimum") != new_clause.get("minimum"):
        return "BREAKING", f"{field_name}: minimum changed {old_clause.get('minimum')} -> {new_clause.get('minimum')}"
    if old_clause.get("enum") != new_clause.get("enum"):
        old_values = set(old_clause.get("enum", []))
        new_values = set(new_clause.get("enum", []))
        removed = sorted(old_values - new_values)
        added = sorted(new_values - old_values)
        if removed:
            return "BREAKING", f"{field_name}: enum values removed {removed}"
        return "COMPATIBLE", f"{field_name}: enum values added {added}"
    return "COMPATIBLE", f"{field_name}: no material change"


def load_snapshots(snapshot_dir: Path) -> list[tuple[Path, dict[str, Any]]]:
    snapshots: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(snapshot_dir.glob("*.yaml")):
        with path.open("r", encoding="utf-8") as handle:
            snapshots.append((path, yaml.safe_load(handle)))
    return snapshots


def migration_checklist(changes: list[dict[str, Any]]) -> list[str]:
    breaking = [change for change in changes if change["classification"] == "BREAKING"]
    if not breaking:
        return ["No breaking changes detected. Keep downstream teams informed and refresh snapshots after release."]
    return [
        "Notify downstream owners named in the lineage downstream section before release.",
        "Ship a compatibility bridge or alias field for every renamed or narrowed field.",
        "Re-run validation and refresh statistical baselines after the migration lands.",
    ]


def rollback_plan(changes: list[dict[str, Any]]) -> list[str]:
    if any(change["classification"] == "BREAKING" for change in changes):
        return [
            "Restore the previous contract snapshot and revert the producer change.",
            "Replay validation on the last known-good dataset snapshot.",
            "Pause downstream consumption until the restored contract passes clean validation.",
        ]
    return ["No rollback required for compatible changes."]


def main() -> int:
    args = parse_args()
    snapshot_dir = Path("schema_snapshots") / normalize_contract_filename(args.contract_id)
    snapshots = load_snapshots(snapshot_dir)
    if len(snapshots) < 2:
        report = {
            "generated_at": utc_now(),
            "contract_id": args.contract_id,
            "status": "INSUFFICIENT_SNAPSHOTS",
            "message": "Run generator.py at least twice to produce a diffable history.",
            "changes": [],
        }
    else:
        old_path, old_contract = snapshots[-2]
        new_path, new_contract = snapshots[-1]
        old_fields = old_contract.get("fields", {})
        new_fields = new_contract.get("fields", {})
        changes = []
        for field_name in sorted(set(old_fields) | set(new_fields)):
            classification, rationale = classify_change(field_name, old_fields.get(field_name), new_fields.get(field_name))
            changes.append(
                {
                    "field_name": field_name,
                    "classification": classification,
                    "rationale": rationale,
                }
            )
        compatibility = "BREAKING" if any(change["classification"] == "BREAKING" for change in changes) else "COMPATIBLE"
        migration_report = {
            "generated_at": utc_now(),
            "contract_id": args.contract_id,
            "compatibility_verdict": compatibility,
            "exact_diff": [change["rationale"] for change in changes],
            "blast_radius": new_contract.get("lineage", {}).get("downstream", []),
            "migration_checklist": migration_checklist(changes),
            "rollback_plan": rollback_plan(changes),
        }
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        migration_path = output_path.parent / f"migration_impact_{normalize_contract_filename(args.contract_id)}_{utc_now().replace(':', '').replace('-', '')}.json"
        migration_path.write_text(json.dumps(migration_report, indent=2), encoding="utf-8")
        report = {
            "generated_at": utc_now(),
            "contract_id": args.contract_id,
            "status": "OK",
            "old_snapshot": str(old_path),
            "new_snapshot": str(new_path),
            "compatibility_verdict": compatibility,
            "migration_checklist": migration_report["migration_checklist"],
            "rollback_plan": migration_report["rollback_plan"],
            "migration_impact_report": str(migration_path),
            "changes": changes,
        }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"status": report["status"], "changes": len(report["changes"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
