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
from contracts.lineage import load_latest_lineage_snapshot, resolve_contract_lineage

AMBIGUOUS_FIELD_NAMES = {
    "category",
    "kind",
    "label",
    "name",
    "result",
    "score",
    "status",
    "type",
    "value",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a contract from JSONL data.")
    parser.add_argument("--source", required=True, help="Path to a JSONL source file.")
    parser.add_argument("--contract-id", required=False, help="Logical ID for the generated contract.")
    parser.add_argument("--lineage", required=False, help="Path to a lineage snapshot JSONL file.")
    parser.add_argument("--registry", required=False, help="Path to ContractRegistry subscriptions YAML.")
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


def registry_subscriptions(registry_path: str | None, contract_id: str) -> list[dict[str, Any]]:
    if not registry_path:
        return []
    path = Path(registry_path)
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    subscriptions = payload.get("subscriptions", [])
    if not isinstance(subscriptions, list):
        return []
    matched: list[dict[str, Any]] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        if str(subscription.get("contract_id", "")) == contract_id:
            matched.append(subscription)
    return matched


def inject_lineage(contract: dict, lineage_snapshot: dict, registry_path: str | None) -> dict:
    resolved = resolve_contract_lineage(contract, lineage_snapshot, registry_path)
    contract["lineage"] = {
        "upstream": resolved["upstream"],
        "downstream": resolved["downstream"],
        "graph_seeds": resolved["graph_seeds"],
    }
    contract["downstream_consumers"] = [
        entry
        for entry in resolved["downstream"]
        if str(entry.get("kind", "")).upper() in {"SERVICE", "SUBSCRIBER"}
    ]
    return contract


def inject_registry(contract: dict, registry_path: str | None) -> dict:
    subscriptions = registry_subscriptions(registry_path, str(contract.get("contract_id", "")))
    contract["registry"] = {
        "path": registry_path or "",
        "subscriber_count": len(subscriptions),
        "subscriber_ids": [str(subscription.get("subscriber_id", "")) for subscription in subscriptions],
    }
    return contract


def numeric_baseline_path(contract_id: str) -> Path:
    return Path("schema_snapshots") / f"{normalize_contract_filename(contract_id)}_baseline.json"


def aggregated_baseline_path() -> Path:
    return Path("schema_snapshots") / "baselines.json"


def numeric_profile_summary(profiles: dict[str, dict[str, Any]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for field_name, profile in sorted(profiles.items()):
        stats = profile.get("stats")
        if not stats:
            continue
        summary[field_name] = {
            "min": round(float(stats["min"]), 6),
            "max": round(float(stats["max"]), 6),
            "mean": round(float(stats["mean"]), 6),
            "stddev": round(float(stats["stddev"]), 6),
        }
    return summary


def persist_numeric_baselines(contract_id: str, source: str, profiles: dict[str, dict[str, Any]]) -> dict[str, Any]:
    schema_snapshot_dir = Path("schema_snapshots")
    schema_snapshot_dir.mkdir(parents=True, exist_ok=True)
    numeric_columns = numeric_profile_summary(profiles)
    baseline_payload = {
        "written_at": utc_now(),
        "contract_id": contract_id,
        "source_path": source,
        "columns": {
            field_name: {
                "mean": stats["mean"],
                "stddev": stats["stddev"],
            }
            for field_name, stats in numeric_columns.items()
        },
    }
    contract_baseline_path = numeric_baseline_path(contract_id)
    contract_baseline_path.write_text(json.dumps(baseline_payload, indent=2), encoding="utf-8")

    aggregate_path = aggregated_baseline_path()
    aggregate: dict[str, Any] = {}
    if aggregate_path.exists():
        aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
    aggregate[contract_id] = baseline_payload
    aggregate_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
    return {
        "per_contract": str(contract_baseline_path),
        "aggregate": str(aggregate_path),
        "numeric_columns": numeric_columns,
    }


def suspicious_distribution_warning(profile: dict[str, Any]) -> str | None:
    stats = profile.get("stats")
    if not stats:
        return None
    mean = float(stats["mean"])
    if mean > 0.99 or mean < 0.01:
        return (
            f"Suspicious distribution detected: observed mean {mean:.3f} is near an extreme boundary. "
            "Check for scale drift, sparse values, or a nearly constant signal."
        )
    return None


def ambiguous_column_reason(field_name: str, profile: dict[str, Any]) -> str | None:
    leaf_name = field_name.rsplit(".", 1)[-1].lower()
    if leaf_name in AMBIGUOUS_FIELD_NAMES:
        return f"Field name '{leaf_name}' is generic and benefits from semantic clarification."
    if profile.get("type") == "string" and int(profile.get("cardinality", 0)) > 20 and leaf_name.endswith("name"):
        return "High-cardinality free-text naming fields often need semantic guidance for downstream consumers."
    return None


def llm_annotation_call(field_name: str, profile: dict[str, Any], reason: str) -> dict[str, Any]:
    sample_values = [str(value) for value in profile.get("sample_values", [])[:3]]
    prompt = (
        f"Annotate ambiguous contract field '{field_name}' with type '{profile.get('type')}'. "
        f"Reason: {reason}. Sample values: {sample_values or ['<none>']}."
    )
    annotation = (
        f"LLM review: treat '{field_name}' as a business-semantic field and document allowed meanings "
        "before new downstream consumers rely on it."
    )
    return {
        "field": field_name,
        "provider": "local_heuristic_fallback",
        "model": "offline-column-annotator",
        "prompt": prompt,
        "annotation": annotation,
        "reason": reason,
    }


def build_annotated_fields(profiles: dict[str, dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    fields: dict[str, dict[str, Any]] = {}
    annotations: list[dict[str, Any]] = []
    for field_name, profile in profiles.items():
        clause = build_field_clause(field_name, profile)
        warning = suspicious_distribution_warning(profile)
        if warning:
            clause["warning"] = warning
        reason = ambiguous_column_reason(field_name, profile)
        if reason:
            annotation = llm_annotation_call(field_name, profile, reason)
            clause["llm_annotation"] = {
                "provider": annotation["provider"],
                "model": annotation["model"],
                "reason": annotation["reason"],
                "annotation": annotation["annotation"],
            }
            annotations.append(annotation)
        fields[field_name] = clause
    return fields, annotations


def profiling_summary(
    profiles: dict[str, dict[str, Any]],
    baseline_metadata: dict[str, Any],
    llm_annotations: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "structural": {
            "field_names": sorted(profiles.keys()),
            "required_fields": sorted(field_name for field_name, profile in profiles.items() if profile.get("required")),
            "nullable_fields": sorted(field_name for field_name, profile in profiles.items() if not profile.get("required")),
            "types": {field_name: profile.get("type", "string") for field_name, profile in sorted(profiles.items())},
        },
        "statistics": baseline_metadata["numeric_columns"],
        "baseline_files": {
            "per_contract": baseline_metadata["per_contract"],
            "aggregate": baseline_metadata["aggregate"],
        },
        "llm_annotations": llm_annotations,
    }


def build_contract(source: str, contract_id: str, lineage_path: str | None, registry_path: str | None) -> dict:
    records = load_jsonl(source)
    dataset = dataset_kind_from(source, contract_id)
    profiles = profile_records(records)
    baseline_metadata = persist_numeric_baselines(contract_id, source, profiles)
    fields, llm_annotations = build_annotated_fields(profiles)
    apply_dataset_overrides(dataset, fields)
    title = contract_title(contract_id, dataset)
    semantic_clauses = dataset_semantic_clauses(dataset)
    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "contract_id": contract_id,
        "dataset": dataset,
        "schema_version": "1.0.0",
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
        "versioning": {
            "expected_schema_version": "1.0.0",
            "supported_source_versions": ["1.0.0"],
            "versioned_input": True,
        },
        "schema": fields,
        "fields": fields,
        "profiling": profiling_summary(profiles, baseline_metadata, llm_annotations),
        "clauses": semantic_clauses,
        "quality": {
            "type": "GeneratedChecks",
            "minimum_clause_count": max(8, len(semantic_clauses)),
            "semantic_clause_count": len(semantic_clauses),
        },
        "cross_checks": dataset_cross_checks(dataset),
    }
    contract = inject_lineage(contract, load_latest_lineage_snapshot(lineage_path), registry_path)
    return inject_registry(contract, registry_path)


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
    registry_path = args.registry
    default_registry = "contract_registry/subscriptions.yaml"
    if registry_path is None and Path(default_registry).exists():
        registry_path = default_registry
    contract = build_contract(args.source, contract_id, lineage_path, registry_path)
    contract_path, dbt_path = write_contract_files(contract, args.output)
    print(
        json.dumps(
            {
                "contract": str(contract_path),
                "dbt": str(dbt_path),
                "dataset": contract["dataset"],
                "registry_subscribers": contract.get("registry", {}).get("subscriber_count", 0),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
