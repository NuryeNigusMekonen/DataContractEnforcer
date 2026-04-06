from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import normalize_contract_filename, schema_snapshots_dir, utc_now
from contracts.evolution import build_compatibility_report
from contracts.lineage import load_latest_lineage_snapshot, resolve_contract_lineage

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare the two latest schema snapshots for a contract.")
    parser.add_argument("--contract-id", required=True, help="Logical contract id used by generator.py.")
    parser.add_argument("--since", required=False, help="Accepted for evaluator compatibility; snapshots are already timestamped.")
    parser.add_argument("--output", required=True, help="Output JSON path for schema evolution results.")
    return parser.parse_args()

def snapshot_timestamp(path: Path) -> datetime:
    stem = path.stem
    try:
        return datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def load_snapshots(snapshot_dir: Path, since: str | None = None) -> list[tuple[Path, dict[str, Any]]]:
    snapshots: list[tuple[Path, dict[str, Any]]] = []
    since_dt = parse_since(since)
    for path in sorted(snapshot_dir.glob("*.yaml")):
        if since_dt and snapshot_timestamp(path) < since_dt:
            continue
        with path.open("r", encoding="utf-8") as handle:
            snapshots.append((path, yaml.safe_load(handle)))
    return snapshots


def migration_checklist(changes: list[dict[str, Any]]) -> list[str]:
    breaking = [change for change in changes if change["compatibility_class"] == "breaking_change"]
    if not breaking:
        return ["No breaking changes detected. Keep downstream teams informed and refresh snapshots after release."]
    return [
        "Notify downstream owners named in the lineage downstream section before release.",
        "Ship a compatibility bridge or alias field for every renamed or narrowed field.",
        "Re-run validation and refresh statistical baselines after the migration lands.",
    ]


def rollback_plan(changes: list[dict[str, Any]]) -> list[str]:
    if any(change["compatibility_class"] == "breaking_change" for change in changes):
        return [
            "Restore the previous contract snapshot and revert the producer change.",
            "Replay validation on the last known-good dataset snapshot.",
            "Pause downstream consumption until the restored contract passes clean validation.",
        ]
    return ["No rollback required for compatible changes."]


def consumer_failure_modes(
    compatibility_report: dict[str, Any],
    resolved_lineage: dict[str, Any],
) -> list[dict[str, Any]]:
    downstream_by_id = {str(entry.get("id", "")): entry for entry in resolved_lineage.get("downstream", [])}
    failure_modes: list[dict[str, Any]] = []
    for subscriber in compatibility_report.get("notification", {}).get("subscriber_details", []):
        subscriber_id = str(subscriber.get("subscriber_id", ""))
        lineage_entry = downstream_by_id.get(subscriber_id, {})
        impacted_fields = [str(field) for field in subscriber.get("breaking_fields", [])]
        reasons = [str(item.get("reason", "")) for item in subscriber.get("failure_modes", []) if item.get("reason")]
        failure_modes.append(
            {
                "subscriber_id": subscriber_id,
                "contact": str(subscriber.get("contact", "unknown")),
                "validation_mode": str(subscriber.get("validation_mode", "AUDIT")),
                "registered_at": str(subscriber.get("registered_at", "")),
                "impacted_fields": impacted_fields,
                "fields_consumed": list(subscriber.get("fields_consumed", [])),
                "lineage_hops": int(lineage_entry.get("hops", 0)),
                "failure_mode": (
                    "; ".join(reasons)
                    if reasons
                    else f"Consumer may fail on {', '.join(impacted_fields) or 'breaking contract changes'}."
                ),
            }
        )
    return failure_modes


def main() -> int:
    args = parse_args()
    snapshot_dir = schema_snapshots_dir() / normalize_contract_filename(args.contract_id)
    snapshots = load_snapshots(snapshot_dir, args.since)
    default_lineage = "outputs/week4/lineage_snapshots.jsonl"
    lineage_snapshot = load_latest_lineage_snapshot(default_lineage if Path(default_lineage).exists() else None)
    default_registry = "contract_registry/subscriptions.yaml"
    registry_path = default_registry if Path(default_registry).exists() else None
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
        resolved_lineage = resolve_contract_lineage(new_contract, lineage_snapshot, registry_path)
        compatibility_report = build_compatibility_report(old_contract, new_contract, registry_path)
        changes = compatibility_report["changes"]
        compatibility = compatibility_report["compatibility_verdict"]
        migration_report = {
            "generated_at": utc_now(),
            "contract_id": args.contract_id,
            "compatibility_verdict": compatibility,
            "source_version": compatibility_report["source_version"],
            "target_version": compatibility_report["target_version"],
            "change_counts": compatibility_report["change_counts"],
            "renames": compatibility_report["renames"],
            "schema_diff": changes,
            "exact_diff": [change["rationale"] for change in changes if change["change_type"] != "NO_CHANGE"],
            "blast_radius": resolved_lineage.get("downstream", []),
            "upstream_dependencies": resolved_lineage.get("upstream", []),
            "notification": compatibility_report["notification"],
            "primary_breaking_change": compatibility_report["primary_breaking_change"],
            "migration_checklist": migration_checklist(changes),
            "rollback_plan": rollback_plan(changes),
            "consumer_failure_modes": consumer_failure_modes(compatibility_report, resolved_lineage),
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
            "source_version": compatibility_report["source_version"],
            "target_version": compatibility_report["target_version"],
            "notification": compatibility_report["notification"],
            "migration_checklist": migration_report["migration_checklist"],
            "rollback_plan": migration_report["rollback_plan"],
            "consumer_failure_modes": migration_report["consumer_failure_modes"],
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
