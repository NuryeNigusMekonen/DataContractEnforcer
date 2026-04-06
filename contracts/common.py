from __future__ import annotations

import os
import hashlib
import json
import math
import re
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UUID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SHA1_PATTERN = re.compile(r"^[0-9a-f]{40}$")
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
SCHEMA_VERSION_PATTERN = re.compile(r"^\d+\.\d+$")
PASCAL_CASE_PATTERN = re.compile(r"^[A-Z][A-Za-z0-9]*$")
NON_ENUM_TEXT_FIELDS = {
    "error",
    "message",
    "details",
    "stack",
    "stacktrace",
    "traceback",
    "exception",
}
MAX_ENUM_STRING_LENGTH = 120
SCHEMA_SNAPSHOTS_SCOPE_ENV = "SCHEMA_SNAPSHOTS_SCOPE"


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


def schema_snapshots_dir() -> Path:
    base_dir = Path("schema_snapshots")
    scope = os.environ.get(SCHEMA_SNAPSHOTS_SCOPE_ENV, "").strip()
    return base_dir / scope if scope else base_dir


@contextmanager
def schema_snapshot_scope(scope: str | None):
    previous = os.environ.get(SCHEMA_SNAPSHOTS_SCOPE_ENV)
    if scope:
        os.environ[SCHEMA_SNAPSHOTS_SCOPE_ENV] = scope
    else:
        os.environ.pop(SCHEMA_SNAPSHOTS_SCOPE_ENV, None)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(SCHEMA_SNAPSHOTS_SCOPE_ENV, None)
        else:
            os.environ[SCHEMA_SNAPSHOTS_SCOPE_ENV] = previous


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
    if field_name in {"rubric_id", "source_hash", "extraction_rules_hash"}:
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
        clause["maximum"] = 1.0
        clause["description"] = "Confidence must remain on a 0.0-1.0 scale."
    if stats and field_name in {"processing_time_ms", "total_cost"}:
        clause["minimum"] = 0.0
    if stats and field_name == "sequence_number":
        clause["minimum"] = 1
    if _should_infer_string_enum(field_name, profile):
        clause["enum"] = profile["sample_values"]
    return clause


def _should_infer_string_enum(field_name: str, profile: dict[str, Any]) -> bool:
    if profile.get("type") != "string":
        return False
    cardinality = int(profile.get("cardinality", 0))
    if cardinality <= 0 or cardinality > 10:
        return False
    sample_values = profile.get("sample_values", [])
    if not sample_values or len(sample_values) != cardinality:
        return False
    if not all(isinstance(value, str) for value in sample_values):
        return False
    leaf_name = field_name.rsplit(".", 1)[-1].lower()
    if leaf_name in NON_ENUM_TEXT_FIELDS:
        return False
    for value in sample_values:
        if "\n" in value:
            return False
        if len(value) > MAX_ENUM_STRING_LENGTH:
            return False
    return True


def apply_dataset_overrides(dataset_kind: str, fields: dict[str, dict[str, Any]]) -> None:
    overrides: dict[str, dict[str, Any]] = {
        "week1_intents": {
            "intent_id": {"format": "uuid"},
            "created_at": {"format": "date-time"},
            "code_refs.confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        },
        "week2_verdicts": {
            "overall_verdict": {"enum": ["PASS", "FAIL", "WARN"]},
            "overall_score": {"type": "number", "minimum": 1.0, "maximum": 5.0},
            "scores.score": {"type": "integer", "minimum": 1, "maximum": 5},
            "rubric_version": {"pattern": SEMVER_PATTERN.pattern},
        },
        "week3_extractions": {
            "doc_id": {"format": "uuid"},
            "source_hash": {"pattern": SHA256_PATTERN.pattern},
            "extraction_rules_hash": {"pattern": SHA256_PATTERN.pattern},
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


def dataset_semantic_clauses(dataset_kind: str) -> list[dict[str, Any]]:
    clauses = {
        "week1_intents": [
            {
                "id": "week1.intent_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Each intent record must expose a stable UUID identifier.",
                "rule": {"type": "field_format", "field": "intent_id", "format": "uuid"},
            },
            {
                "id": "week1.created_at_datetime",
                "category": "temporal",
                "severity": "error",
                "description": "Intent timestamps must be valid UTC date-times.",
                "rule": {"type": "field_format", "field": "created_at", "format": "date-time"},
            },
            {
                "id": "week1.code_refs_non_empty",
                "category": "traceability",
                "severity": "error",
                "description": "Each intent record should reference at least one code location.",
                "rule": {"type": "array_min_items", "field": "code_refs", "minimum": 1},
            },
            {
                "id": "week1.code_ref_confidence_unit_scale",
                "category": "quality",
                "severity": "error",
                "description": "Code reference confidence values must remain on the 0.0-1.0 scale.",
                "rule": {"type": "numeric_range", "field": "code_refs.confidence", "minimum": 0.0, "maximum": 1.0},
            },
            {
                "id": "week1.code_ref_file_exists",
                "category": "traceability",
                "severity": "error",
                "description": "Each referenced code path must resolve to an existing file in this repository.",
                "rule": {"type": "repo_path_exists", "field": "code_refs.file"},
            },
        ],
        "week2_verdicts": [
            {
                "id": "week2.verdict_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Each verdict record must expose a stable UUID identifier.",
                "rule": {"type": "field_format", "field": "verdict_id", "format": "uuid"},
            },
            {
                "id": "week2.overall_verdict_enum",
                "category": "domain",
                "severity": "error",
                "description": "Overall verdict must be one of PASS, FAIL, or WARN.",
                "rule": {"type": "field_enum", "field": "overall_verdict", "allowed": ["PASS", "FAIL", "WARN"]},
            },
            {
                "id": "week2.score_range",
                "category": "quality",
                "severity": "error",
                "description": "Each nested criterion score must remain on the 1-5 rubric scale.",
                "rule": {"type": "numeric_range", "field": "scores.score", "minimum": 1, "maximum": 5},
            },
            {
                "id": "week2.overall_score_weighted_mean",
                "category": "quality",
                "severity": "error",
                "description": "overall_score must equal the weighted mean of criterion scores.",
                "rule": {
                    "type": "weighted_mean_equals",
                    "scores_field": "scores",
                    "score_field": "score",
                    "weight_field": "weight",
                    "output_field": "overall_score",
                    "tolerance": 0.001,
                },
            },
            {
                "id": "week2.rubric_id_sha256",
                "category": "integrity",
                "severity": "error",
                "description": "rubric_id values must match SHA-256 formatting.",
                "rule": {"type": "field_pattern", "field": "rubric_id", "pattern": SHA256_PATTERN.pattern},
            },
            {
                "id": "week2.rubric_version_semver",
                "category": "compatibility",
                "severity": "error",
                "description": "rubric_version must use semantic versioning.",
                "rule": {"type": "field_pattern", "field": "rubric_version", "pattern": SEMVER_PATTERN.pattern},
            },
            {
                "id": "week2.confidence_unit_scale",
                "category": "quality",
                "severity": "error",
                "description": "Verdict confidence values must stay on a 0.0-1.0 scale.",
                "rule": {"type": "numeric_range", "field": "confidence", "minimum": 0.0, "maximum": 1.0},
            },
            {
                "id": "week2.evaluated_at_datetime",
                "category": "temporal",
                "severity": "error",
                "description": "Verdict timestamps must be valid UTC date-times.",
                "rule": {"type": "field_format", "field": "evaluated_at", "format": "date-time"},
            },
        ],
        "week3_extractions": [
            {
                "id": "week3.doc_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Each extraction record must carry a stable UUID document identifier.",
                "rule": {"type": "field_format", "field": "doc_id", "format": "uuid"},
            },
            {
                "id": "week3.extracted_at_datetime",
                "category": "temporal",
                "severity": "error",
                "description": "Extraction timestamps must be valid UTC date-times for traceability and replay.",
                "rule": {"type": "field_format", "field": "extracted_at", "format": "date-time"},
            },
            {
                "id": "week3.fact_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Every extracted fact must expose a UUID fact identifier.",
                "rule": {"type": "field_format", "field": "extracted_facts.fact_id", "format": "uuid"},
            },
            {
                "id": "week3.confidence_unit_scale",
                "category": "quality",
                "severity": "error",
                "description": "Confidence values must stay on the 0.0 to 1.0 unit interval and never drift to percentages.",
                "rule": {
                    "type": "numeric_range",
                    "field": "extracted_facts.confidence",
                    "minimum": 0.0,
                    "maximum": 1.0,
                },
            },
            {
                "id": "week3.page_ref_positive",
                "category": "provenance",
                "severity": "error",
                "description": "Each extracted fact must point to a positive page number in the source document.",
                "rule": {"type": "numeric_range", "field": "extracted_facts.page_ref", "minimum": 1},
            },
            {
                "id": "week3.source_excerpt_present",
                "category": "provenance",
                "severity": "error",
                "description": "Facts must preserve a non-empty source excerpt to support audit review.",
                "rule": {"type": "string_length", "field": "extracted_facts.source_excerpt", "minimum": 1},
            },
            {
                "id": "week3.source_hash_sha256",
                "category": "integrity",
                "severity": "error",
                "description": "Source hashes must be SHA-256 digests to support immutable provenance checks.",
                "rule": {
                    "type": "field_pattern",
                    "field": "source_hash",
                    "pattern": SHA256_PATTERN.pattern,
                },
            },
            {
                "id": "week3.extraction_rules_hash_sha256",
                "category": "integrity",
                "severity": "error",
                "description": "Extraction records must pin the exact SHA-256 hash of extraction_rules.yaml used at generation time.",
                "rule": {
                    "type": "field_pattern",
                    "field": "extraction_rules_hash",
                    "pattern": SHA256_PATTERN.pattern,
                },
            },
            {
                "id": "week3.processing_time_positive",
                "category": "operational",
                "severity": "error",
                "description": "Processing duration must be recorded as a positive millisecond value.",
                "rule": {"type": "numeric_range", "field": "processing_time_ms", "minimum": 1},
            },
            {
                "id": "week3.source_path_lineage",
                "category": "lineage",
                "severity": "warn",
                "description": "Source paths should resolve to refinery chunk or extraction artifacts rather than arbitrary files.",
                "rule": {
                    "type": "field_pattern",
                    "field": "source_path",
                    "pattern": r".*/(artifacts/)?week3/.refinery/(extracted|chunks)/.+",
                },
            },
            {
                "id": "week3.token_counts_non_negative",
                "category": "operational",
                "severity": "warn",
                "description": "Recorded token counters must never be negative even for OCR-derived runs.",
                "rule": {
                    "type": "multi_field_numeric_range",
                    "fields": ["token_count.input", "token_count.output"],
                    "minimum": 0,
                },
            },
        ],
        "week5_events": [
            {
                "id": "week5.event_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Each event must expose a stable UUID event identifier.",
                "rule": {"type": "field_format", "field": "event_id", "format": "uuid"},
            },
            {
                "id": "week5.aggregate_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Each event must link back to a UUID aggregate identifier.",
                "rule": {"type": "field_format", "field": "aggregate_id", "format": "uuid"},
            },
            {
                "id": "week5.aggregate_type_enum",
                "category": "domain",
                "severity": "error",
                "description": "Aggregate types must stay within the modeled bounded context set.",
                "rule": {
                    "type": "field_enum",
                    "field": "aggregate_type",
                    "allowed": [
                        "LoanApplication",
                        "AgentSession",
                        "DocumentPackage",
                        "ComplianceRecord",
                        "AuditLedger",
                    ],
                },
            },
            {
                "id": "week5.event_type_pascal_case",
                "category": "domain",
                "severity": "error",
                "description": "Event names should remain PascalCase so downstream schema routing stays deterministic.",
                "rule": {
                    "type": "field_pattern",
                    "field": "event_type",
                    "pattern": PASCAL_CASE_PATTERN.pattern,
                },
            },
            {
                "id": "week5.occurred_at_datetime",
                "category": "temporal",
                "severity": "error",
                "description": "Business occurrence timestamps must be valid date-times.",
                "rule": {"type": "field_format", "field": "occurred_at", "format": "date-time"},
            },
            {
                "id": "week5.recorded_at_not_before_occurred_at",
                "category": "temporal",
                "severity": "error",
                "description": "Ledger write time must not precede the business event time.",
                "rule": {
                    "type": "temporal_order",
                    "left_field": "occurred_at",
                    "right_field": "recorded_at",
                    "operator": "<=",
                },
            },
            {
                "id": "week5.sequence_number_positive",
                "category": "ordering",
                "severity": "error",
                "description": "Sequence numbers must start at 1 and stay positive.",
                "rule": {"type": "numeric_range", "field": "sequence_number", "minimum": 1},
            },
            {
                "id": "week5.sequence_monotonic_per_aggregate",
                "category": "ordering",
                "severity": "error",
                "description": "Sequence numbers must be monotonic within each aggregate stream.",
                "rule": {
                    "type": "group_monotonic",
                    "group_by": ["aggregate_id"],
                    "field": "sequence_number",
                },
            },
            {
                "id": "week5.schema_version_supported",
                "category": "compatibility",
                "severity": "error",
                "description": "Schema versions must stay within supported major.minor contract versions.",
                "rule": {
                    "type": "field_enum",
                    "field": "schema_version",
                    "allowed": ["1.0", "2.0"],
                },
            },
            {
                "id": "week5.correlation_id_uuid",
                "category": "traceability",
                "severity": "error",
                "description": "Each event must retain a correlation id for cross-service tracing.",
                "rule": {"type": "field_format", "field": "metadata.correlation_id", "format": "uuid"},
            },
            {
                "id": "week5.application_submission_amount_positive",
                "category": "domain",
                "severity": "error",
                "description": "ApplicationSubmitted events must carry a positive requested amount.",
                "rule": {
                    "type": "conditional_numeric_range",
                    "when": {"field": "event_type", "equals": "ApplicationSubmitted"},
                    "field": "payload.requested_amount_usd",
                    "minimum": 0.01,
                },
            },
            {
                "id": "week5.document_events_reference_pdf",
                "category": "document",
                "severity": "warn",
                "description": "Document-related events should reference PDF assets so extraction lineage can be reproduced.",
                "rule": {
                    "type": "conditional_pattern",
                    "when": {
                        "field": "event_type",
                        "in": ["DocumentUploadRequested", "DocumentUploaded", "DocumentAdded"],
                    },
                    "field": "payload.document_path",
                    "pattern": r".+\.pdf$",
                },
            },
        ],
        "week4_lineage": [
            {
                "id": "week4.snapshot_id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Lineage snapshots must expose a stable UUID snapshot_id.",
                "rule": {"type": "field_format", "field": "snapshot_id", "format": "uuid"},
            },
            {
                "id": "week4.git_commit_sha1",
                "category": "integrity",
                "severity": "error",
                "description": "git_commit must be an exact 40-character lowercase SHA-1 hash.",
                "rule": {"type": "field_pattern", "field": "git_commit", "pattern": SHA1_PATTERN.pattern},
            },
            {
                "id": "week4.edge_relationship_enum",
                "category": "lineage",
                "severity": "error",
                "description": "edge.relationship must remain within the six allowed lineage operations.",
                "rule": {
                    "type": "field_enum",
                    "field": "edges.relationship",
                    "allowed": ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"],
                },
            },
            {
                "id": "week4.edge_endpoints_exist",
                "category": "lineage",
                "severity": "error",
                "description": "Every edge source/target must reference a node_id in the same snapshot.",
                "rule": {
                    "type": "edge_endpoints_exist",
                    "nodes_field": "nodes",
                    "edges_field": "edges",
                    "node_id_field": "node_id",
                    "source_field": "source",
                    "target_field": "target",
                },
            },
            {
                "id": "week4.edge_confidence_unit_scale",
                "category": "quality",
                "severity": "error",
                "description": "Edge confidence values must remain on the 0.0-1.0 scale.",
                "rule": {"type": "numeric_range", "field": "edges.confidence", "minimum": 0.0, "maximum": 1.0},
            },
            {
                "id": "week4.captured_at_datetime",
                "category": "temporal",
                "severity": "error",
                "description": "Snapshot capture timestamps must be valid UTC date-times.",
                "rule": {"type": "field_format", "field": "captured_at", "format": "date-time"},
            },
        ],
        "traces": [
            {
                "id": "traces.id_uuid",
                "category": "identifier",
                "severity": "error",
                "description": "Every trace record must expose a UUID id.",
                "rule": {"type": "field_format", "field": "id", "format": "uuid"},
            },
            {
                "id": "traces.start_end_temporal_order",
                "category": "temporal",
                "severity": "error",
                "description": "end_time must be strictly after start_time.",
                "rule": {"type": "temporal_order", "left_field": "start_time", "right_field": "end_time", "operator": "<"},
            },
            {
                "id": "traces.total_tokens_sum",
                "category": "quality",
                "severity": "error",
                "description": "total_tokens must equal prompt_tokens + completion_tokens.",
                "rule": {
                    "type": "sum_equals",
                    "fields": ["prompt_tokens", "completion_tokens"],
                    "output_field": "total_tokens",
                    "tolerance": 0.0,
                },
            },
            {
                "id": "traces.run_type_enum",
                "category": "domain",
                "severity": "error",
                "description": "run_type must be one of llm, chain, tool, retriever, embedding.",
                "rule": {
                    "type": "field_enum",
                    "field": "run_type",
                    "allowed": ["llm", "chain", "tool", "retriever", "embedding"],
                },
            },
            {
                "id": "traces.total_cost_non_negative",
                "category": "quality",
                "severity": "error",
                "description": "total_cost values must be non-negative USD amounts.",
                "rule": {"type": "numeric_range", "field": "total_cost", "minimum": 0.0},
            },
        ],
    }
    return clauses.get(dataset_kind, [])


def dataset_cross_checks(dataset_kind: str) -> list[dict[str, str]]:
    checks = {
        "week2_verdicts": [
            {"id": "week2.overall_score_weighted_mean", "type": "record_rule", "field": "overall_score"},
            {"id": "week2.rubric_hash_exists", "type": "record_rule", "field": "rubric_id"},
        ],
        "week3_extractions": [
            {"id": "week3.entity_refs_exist", "type": "record_rule", "field": "extracted_facts.entity_refs"},
            {"id": "week3.extraction_rules_hash_exists", "type": "record_rule", "field": "extraction_rules_hash"},
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
