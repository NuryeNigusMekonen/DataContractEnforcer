from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UUID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SHA1_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
SCHEMA_VERSION_PATTERN = re.compile(r"^\d+\.\d+$")
PASCAL_CASE_PATTERN = re.compile(r"^[A-Z][A-Za-z0-9]*$")


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []
    records: list[dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#"):
                records.append(json.loads(line))
    return records


def write_jsonl(path: str | Path, records: list[dict[str, Any]]) -> None:
    file_path = Path(path)
    ensure_parent_dir(file_path)
    with file_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def ensure_parent_dir(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def infer_scalar_type(values: list[Any]) -> str:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return "string"
    if all(isinstance(value, bool) for value in filtered):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in filtered):
        return "integer"
    if all(is_numeric(value) for value in filtered):
        return "number"
    return "string"


@dataclass
class FieldObservation:
    values: list[Any]
    paths_seen: int


def extract_field_observations(records: list[dict[str, Any]]) -> dict[str, FieldObservation]:
    observations: dict[str, list[Any]] = defaultdict(list)
    counts: dict[str, int] = defaultdict(int)

    def visit(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                next_prefix = f"{prefix}.{key}" if prefix else key
                visit(next_prefix, nested)
            return
        if isinstance(value, list):
            counts[prefix] += 1
            if not value:
                observations[prefix].append(None)
                return
            if all(isinstance(item, dict) for item in value):
                for item in value:
                    for key, nested in item.items():
                        next_prefix = f"{prefix}.{key}" if prefix else key
                        visit(next_prefix, nested)
            else:
                for item in value:
                    observations[prefix].append(item)
            return
        counts[prefix] += 1
        observations[prefix].append(value)

    for record in records:
        visit("", record)

    return {
        field: FieldObservation(values=values, paths_seen=counts.get(field, 0))
        for field, values in observations.items()
        if field
    }


def profile_records(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    observations = extract_field_observations(records)
    total_records = max(len(records), 1)
    for field_name, observation in sorted(observations.items()):
        values = observation.values
        filtered = [value for value in values if value is not None]
        inferred_type = infer_scalar_type(filtered)
        unique_values: list[str] = []
        seen: set[str] = set()
        for value in filtered:
            rendered = stringify(value)
            if rendered not in seen:
                seen.add(rendered)
                unique_values.append(rendered)
        profile: dict[str, Any] = {
            "name": field_name,
            "type": inferred_type,
            "sample_values": unique_values[:5],
            "cardinality": len({json.dumps(value, sort_keys=True) for value in filtered}),
            "required": observation.paths_seen >= total_records and None not in values,
            "observed_values": len(filtered),
        }
        if inferred_type in {"integer", "number"}:
            numeric_values = [float(value) for value in filtered]
            mean = sum(numeric_values) / len(numeric_values)
            variance = sum((value - mean) ** 2 for value in numeric_values) / max(len(numeric_values), 1)
            profile["stats"] = {
                "min": min(numeric_values),
                "max": max(numeric_values),
                "mean": mean,
                "stddev": math.sqrt(variance),
            }
        profiles[field_name] = profile
    return profiles


def stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def dataset_kind_from(source: str, contract_id: str) -> str:
    text = f"{source} {contract_id}".lower()
    if "week3" in text or "extract" in text:
        return "week3_extractions"
    if "week5" in text or "event" in text:
        return "week5_events"
    if "week4" in text or "lineage" in text:
        return "week4_lineage"
    if "week2" in text or "verdict" in text:
        return "week2_verdicts"
    if "trace" in text or "langsmith" in text:
        return "traces"
    if "week1" in text or "intent" in text:
        return "week1_intents"
    return "generic"


def build_field_clause(field_name: str, profile: dict[str, Any]) -> dict[str, Any]:
    stats = profile.get("stats")
    sample_values = [value for value in profile.get("sample_values", []) if isinstance(value, str)]
    clause: dict[str, Any] = {
        "type": profile["type"],
        "required": bool(profile.get("required", False)),
    }
    if field_name.endswith("_id") or field_name == "id":
        if field_name not in {"rubric_id", "source_hash", "metadata.user_id"} and sample_values and all(
            UUID_PATTERN.match(value) for value in sample_values
        ):
            clause["format"] = "uuid"
    if field_name.startswith("payload.") and (field_name.endswith("_id") or field_name.endswith(".stream_id")):
        clause["format"] = None
    if field_name.endswith("_at") or field_name in {"start_time", "end_time", "created_at", "captured_at"}:
        clause["format"] = "date-time"
    if field_name == "git_commit":
        clause["pattern"] = SHA1_PATTERN.pattern
    if field_name in {"rubric_id", "source_hash"}:
        clause["pattern"] = SHA256_PATTERN.pattern
    if field_name == "rubric_version":
        clause["pattern"] = SEMVER_PATTERN.pattern
    if field_name == "schema_version":
        clause["pattern"] = SCHEMA_VERSION_PATTERN.pattern
    if field_name in {"event_type", "aggregate_type"}:
        clause["pattern"] = PASCAL_CASE_PATTERN.pattern
    if "confidence" in field_name and clause["type"] in {"integer", "number"}:
        clause["type"] = "number"
        clause["minimum"] = 0.0
        observed_max = 1.0
        if stats:
            observed_max = float(stats["max"])
        clause["maximum"] = 1.0 if observed_max <= 1.0 else round(observed_max, 3)
        clause["description"] = "Confidence must remain on a 0.0-1.0 scale."
    if stats and field_name in {"processing_time_ms", "total_cost"}:
        clause["minimum"] = 0.0
    if stats and field_name == "sequence_number":
        clause["minimum"] = 1
    if profile["type"] == "string" and profile["cardinality"] <= 10 and profile["sample_values"]:
        if len(profile["sample_values"]) == profile["cardinality"]:
            clause["enum"] = profile["sample_values"]
    return clause


def apply_dataset_overrides(dataset_kind: str, fields: dict[str, dict[str, Any]]) -> None:
    overrides: dict[str, dict[str, Any]] = {
        "week2_verdicts": {
            "overall_verdict": {"enum": ["PASS", "FAIL", "WARN"]},
            "overall_score": {"type": "number", "minimum": 1.0, "maximum": 5.0},
            "scores.score": {"type": "integer", "minimum": 1, "maximum": 5},
            "rubric_version": {"pattern": SEMVER_PATTERN.pattern},
        },
        "week3_extractions": {
            "doc_id": {"format": "uuid"},
            "source_hash": {"pattern": SHA256_PATTERN.pattern},
            "entities.type": {"enum": ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]},
            "processing_time_ms": {"type": "integer", "minimum": 1},
            "extracted_at": {"format": "date-time"},
        },
        "week4_lineage": {
            "git_commit": {"pattern": SHA1_PATTERN.pattern},
            "nodes.node_id": {"format": None},
            "edges.relationship": {"enum": ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"]},
            "edges.confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        },
        "week5_events": {
            "event_id": {"format": "uuid"},
            "aggregate_id": {"format": "uuid"},
            "event_type": {"pattern": PASCAL_CASE_PATTERN.pattern},
            "aggregate_type": {"pattern": PASCAL_CASE_PATTERN.pattern},
            "schema_version": {"pattern": SCHEMA_VERSION_PATTERN.pattern},
            "sequence_number": {"type": "integer", "minimum": 1},
            "occurred_at": {"format": "date-time"},
            "recorded_at": {"format": "date-time"},
            "payload.application_id": {"format": None},
            "payload.applicant_id": {"format": None},
            "payload.document_package_id": {"format": None},
            "payload.agent_id": {"format": None},
            "payload.session_id": {"format": None},
            "payload.orchestrator_agent_id": {"format": None},
        },
        "traces": {
            "id": {"format": "uuid"},
            "run_type": {"enum": ["llm", "chain", "tool", "retriever", "embedding"]},
            "start_time": {"format": "date-time"},
            "end_time": {"format": "date-time"},
            "total_tokens": {"type": "integer", "minimum": 0},
            "total_cost": {"type": "number", "minimum": 0.0},
        },
    }
    for field_name, clause in overrides.get(dataset_kind, {}).items():
        if field_name not in fields:
            continue
        fields[field_name].update(clause)


def dataset_cross_checks(dataset_kind: str) -> list[dict[str, str]]:
    checks = {
        "week2_verdicts": [
            {"id": "week2.overall_score_weighted_mean", "type": "record_rule", "field": "overall_score"},
            {"id": "week2.rubric_hash_exists", "type": "record_rule", "field": "rubric_id"},
        ],
        "week3_extractions": [
            {"id": "week3.entity_refs_exist", "type": "record_rule", "field": "extracted_facts.entity_refs"},
        ],
        "week4_lineage": [
            {"id": "week4.edges_reference_nodes", "type": "record_rule", "field": "edges"},
        ],
        "week5_events": [
            {"id": "week5.recorded_after_occurred", "type": "record_rule", "field": "recorded_at"},
            {"id": "week5.sequence_monotonic", "type": "dataset_rule", "field": "sequence_number"},
            {"id": "week5.payload_matches_schema", "type": "record_rule", "field": "payload"},
        ],
        "traces": [
            {"id": "traces.end_after_start", "type": "record_rule", "field": "end_time"},
            {"id": "traces.total_tokens_add_up", "type": "record_rule", "field": "total_tokens"},
        ],
    }
    return checks.get(dataset_kind, [])


def dbt_type_for(clause_type: str) -> str:
    return {
        "integer": "int",
        "number": "float",
        "string": "string",
        "boolean": "boolean",
    }.get(clause_type, "string")


def normalize_contract_filename(contract_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", contract_id)
