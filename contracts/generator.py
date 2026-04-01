from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
import sys
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from contracts.common import (
    apply_dataset_overrides,
    build_field_clause,
    dataset_cross_checks,
    dataset_kind_from,
    dataset_semantic_clauses,
    dbt_type_for,
    load_jsonl,
    normalize_contract_filename,
    profile_records,
    utc_now,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a contract from JSONL data.")
    parser.add_argument("--source", required=True, help="Path to a JSONL source file.")
    parser.add_argument("--contract-id", required=False, help="Logical ID for the generated contract.")
    parser.add_argument("--lineage", required=False, help="Path to a lineage snapshot JSONL file.")
    parser.add_argument("--output", required=True, help="Output directory for contract files.")
    return parser.parse_args()


def inferred_contract_id(source: str) -> str:
    dataset = dataset_kind_from(source, "")
    return {
        "week1_intents": "week1-intent-records",
        "week2_verdicts": "week2-verdict-records",
        "week3_extractions": "week3-document-refinery-extractions",
        "week4_lineage": "week4-lineage-snapshots",
        "week5_events": "week5-event-records",
        "traces": "langsmith-trace-records",
    }.get(dataset, normalize_contract_filename(Path(source).stem))


def contract_title(contract_id: str, dataset: str) -> str:
    titles = {
        "week1_intents": "Week 1 Intent Records",
        "week2_verdicts": "Week 2 Verdict Records",
        "week3_extractions": "Week 3 Document Refinery Extraction Records",
        "week4_lineage": "Week 4 Lineage Snapshots",
        "week5_events": "Week 5 Event Records",
        "traces": "LangSmith Trace Records",
    }
    return titles.get(dataset, contract_id.replace("-", " ").replace("_", " ").title())


def alias_filename_for(dataset: str) -> str | None:
    return {
        "week1_intents": "week1_intent_records",
        "week2_verdicts": "week2_verdicts",
        "week3_extractions": "week3_extractions",
        "week4_lineage": "week4_lineage",
        "week5_events": "week5_events",
        "traces": "langsmith_traces",
    }.get(dataset)


def latest_lineage_snapshot(lineage_path: str | None) -> dict:
    if not lineage_path:
        return {}
    snapshots = load_jsonl(lineage_path)
    return snapshots[-1] if snapshots else {}


def inject_lineage(contract: dict, lineage_snapshot: dict) -> dict:
    if not lineage_snapshot:
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract
    dataset = contract["dataset"]
    keywords = {
        "week3_extractions": ("week3", "extraction"),
        "week5_events": ("week5", "event"),
        "week2_verdicts": ("week2", "verdict"),
        "traces": ("trace", "langsmith"),
    }.get(dataset, ())
    downstream: list[dict] = []
    for edge in lineage_snapshot.get("edges", []):
        source = edge.get("source", "")
        if any(keyword in source.lower() for keyword in keywords):
            downstream.append(
                {
                    "id": edge.get("target"),
                    "relationship": edge.get("relationship"),
                    "fields_consumed": sorted(contract["fields"].keys())[:5],
                }
            )
    contract["lineage"] = {
        "upstream": [],
        "downstream": downstream,
    }
    return contract


def build_contract(source: str, contract_id: str, lineage_path: str | None) -> dict:
    records = load_jsonl(source)
    dataset = dataset_kind_from(source, contract_id)
    profiles = profile_records(records)
    fields = {field_name: build_field_clause(field_name, profile) for field_name, profile in profiles.items()}
    apply_dataset_overrides(dataset, fields)
    title = contract_title(contract_id, dataset)
    semantic_clauses = dataset_semantic_clauses(dataset)
    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "contract_id": contract_id,
        "dataset": dataset,
        "generated_at": utc_now(),
        "source_path": source,
        "record_count": len(records),
        "info": {
            "title": title,
            "version": "1.0.0",
            "owner": "trp1-week7",
            "description": f"Auto-generated contract for {title.lower()} sourced from {source}.",
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish externally.",
        },
        "schema": fields,
        "fields": fields,
        "clauses": semantic_clauses,
        "quality": {
            "type": "GeneratedChecks",
            "minimum_clause_count": max(8, len(semantic_clauses)),
            "semantic_clause_count": len(semantic_clauses),
        },
        "cross_checks": dataset_cross_checks(dataset),
    }
    return inject_lineage(contract, latest_lineage_snapshot(lineage_path))


def write_contract_files(contract: dict, output_dir: str) -> tuple[Path, Path]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = normalize_contract_filename(contract["contract_id"])
    contract_path = target_dir / f"{filename}.yaml"
    dbt_path = target_dir / f"{filename}_dbt.yml"
    with contract_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(contract, handle, sort_keys=False)
    dbt_payload = {
        "version": 2,
        "models": [
            {
                "name": filename,
                "description": f"Generated contract mirror for {contract['contract_id']}",
                "columns": [
                    {"name": field_name, "data_type": dbt_type_for(clause.get("type", "string"))}
                    for field_name, clause in sorted(contract["fields"].items())
                ],
            }
        ],
    }
    with dbt_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(dbt_payload, handle, sort_keys=False)
    alias_name = alias_filename_for(contract["dataset"])
    if alias_name and alias_name != filename:
        shutil.copy(contract_path, target_dir / f"{alias_name}.yaml")
        shutil.copy(dbt_path, target_dir / f"{alias_name}_dbt.yml")
    snapshot_dir = Path("schema_snapshots") / filename
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{utc_now().replace(':', '').replace('-', '')}.yaml"
    shutil.copy(contract_path, snapshot_path)
    return contract_path, dbt_path


def main() -> int:
    args = parse_args()
    contract_id = args.contract_id or inferred_contract_id(args.source)
    lineage_path = args.lineage
    default_lineage = "outputs/week4/lineage_snapshots.jsonl"
    if lineage_path is None and Path(default_lineage).exists():
        lineage_path = default_lineage
    contract = build_contract(args.source, contract_id, lineage_path)
    contract_path, dbt_path = write_contract_files(contract, args.output)
    print(json.dumps({"contract": str(contract_path), "dbt": str(dbt_path), "dataset": contract["dataset"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
