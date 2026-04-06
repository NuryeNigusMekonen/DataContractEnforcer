from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import uuid
from collections import Counter
from pathlib import Path
import sys
from typing import Any
from jsonschema import Draft7Validator, ValidationError, validate
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import load_jsonl, schema_snapshots_dir, utc_now, write_jsonl
from contracts.runner import validate_traces


UUID_PATTERN = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$")
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "minLength": 1, "maxLength": 8000},
    },
    "additionalProperties": False,
}

VERDICT_OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": [
        "confidence",
        "evaluated_at",
        "overall_score",
        "overall_verdict",
        "rubric_id",
        "rubric_version",
        "scores",
        "target_ref",
        "verdict_id",
    ],
    "properties": {
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "evaluated_at": {"type": "string", "format": "date-time"},
        "overall_score": {"type": "number", "minimum": 1.0, "maximum": 5.0},
        "overall_verdict": {"type": "string", "enum": ["PASS", "FAIL", "WARN"]},
        "rubric_id": {"type": "string", "pattern": SHA256_PATTERN.pattern},
        "rubric_version": {"type": "string", "pattern": SEMVER_PATTERN.pattern},
        "scores": {
            "type": "object",
            "minProperties": 1,
            "additionalProperties": {
                    "type": "object",
                    "required": ["evidence", "notes", "score"],
                    "properties": {
                        "evidence": {
                            "type": "array",
                            "minItems": 1,
                            "items": {"type": "string", "minLength": 1},
                        },
                        "notes": {"type": "string", "minLength": 1},
                        "score": {"type": "integer", "minimum": 1, "maximum": 5},
                    },
                    "additionalProperties": False,
            },
        },
        "target_ref": {"type": "string", "minLength": 1},
        "verdict_id": {"type": "string", "pattern": UUID_PATTERN.pattern},
    },
    "additionalProperties": False,
}


TRACE_RECORD_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": [
        "id",
        "run_type",
        "inputs",
        "outputs",
        "start_time",
        "end_time",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "total_cost",
        "tags",
    ],
    "properties": {
        "id": {"type": "string", "pattern": UUID_PATTERN.pattern},
        "name": {"type": ["string", "null"]},
        "run_type": {"type": "string", "enum": ["llm", "chain", "tool", "retriever", "embedding"]},
        "inputs": {"type": "object"},
        "outputs": {"type": "object"},
        "error": {"type": ["string", "null"]},
        "start_time": {"type": "string", "format": "date-time"},
        "end_time": {"type": "string", "format": "date-time"},
        "prompt_tokens": {"type": "integer", "minimum": 0},
        "completion_tokens": {"type": "integer", "minimum": 0},
        "total_tokens": {"type": "integer", "minimum": 0},
        "total_cost": {"type": "number", "minimum": 0.0},
        "tags": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        },
        "parent_run_id": {"type": ["string", "null"]},
        "session_id": {"type": ["string", "null"]},
    },
    "additionalProperties": True,
}


def _normalize_source_label(source_label: str | None) -> str | None:
    if not source_label:
        return None
    normalized = str(source_label).strip().lower()
    if normalized in {"real", "violated"}:
        return normalized
    return None


def _infer_source_label_from_paths(*paths: str | None) -> str:
    for candidate in paths:
        if not candidate:
            continue
        lowered = str(candidate).lower()
        if "violated" in lowered:
            return "violated"
    return "real"


def _scoped_ai_snapshot_dir(source_label: str | None = None) -> Path:
    snapshot_dir = schema_snapshots_dir()
    if snapshot_dir != Path("schema_snapshots"):
        return snapshot_dir
    return Path("schema_snapshots") / (_normalize_source_label(source_label) or "real")


def _default_ai_baseline_path(filename: str, *, source_label: str | None = None) -> Path:
    return _scoped_ai_snapshot_dir(source_label) / filename


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AI-specific contract checks.")
    parser.add_argument("--mode", default="all", choices=["all", "drift", "prompt", "output", "traces"])
    parser.add_argument("--extractions", required=False, help="Path to week3 extraction records.")
    parser.add_argument("--verdicts", required=False, help="Path to week2 verdict records.")
    parser.add_argument("--traces", required=False, help="Path to LangSmith trace records.")
    parser.add_argument("--output", required=True, help="Output JSON path.")
    return parser.parse_args()


def _format_error_path(error: ValidationError) -> str:
    path_parts = [str(part) for part in error.path]
    if path_parts:
        return ".".join(path_parts)
    return "$"


def _sample_schema_errors(records: list[dict[str, Any]], schema: dict[str, Any]) -> list[dict[str, Any]]:
    validator = Draft7Validator(schema, format_checker=Draft7Validator.FORMAT_CHECKER)
    failures: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        errors = sorted(validator.iter_errors(record), key=lambda item: (_format_error_path(item), item.message))
        if not errors:
            continue
        error = errors[0]
        failures.append(
            {
                "record_index": index,
                "field": _format_error_path(error),
                "message": error.message,
            }
        )
    return failures


def _combine_statuses(*statuses: str) -> str:
    normalized = [status.upper() for status in statuses if isinstance(status, str)]
    if any(status in {"FAIL", "ERROR"} for status in normalized):
        return "FAIL"
    if any(status == "WARN" for status in normalized):
        return "WARN"
    if any(status in {"PASS", "BASELINE_SET"} for status in normalized):
        return "PASS"
    if any(status == "SKIPPED" for status in normalized):
        return "SKIPPED"
    return "UNKNOWN"


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def hashed_vector(texts: list[str], dimensions: int = 128) -> list[float]:
    vector = [0.0] * dimensions
    for text in texts:
        for token, count in Counter(tokenize(text)).items():
            digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
            index = int(digest[:8], 16) % dimensions
            vector[index] += float(count)
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [value / norm for value in vector]


def cosine_distance(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(value * value for value in left)) or 1.0
    right_norm = math.sqrt(sum(value * value for value in right)) or 1.0
    return 1.0 - (dot / (left_norm * right_norm))


def check_embedding_drift(
    texts: list[str],
    baseline_path: str | None = None,
    threshold: float = 0.15,
    *,
    source_label: str | None = None,
) -> dict[str, Any]:
    centroid = hashed_vector(texts)
    path = Path(baseline_path) if baseline_path else _default_ai_baseline_path("embedding_baseline.json", source_label=source_label)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"written_at": utc_now(), "centroid": centroid}, indent=2), encoding="utf-8")
        return {"status": "BASELINE_SET", "drift_score": 0.0, "threshold": threshold}
    baseline = json.loads(path.read_text(encoding="utf-8")).get("centroid", centroid)
    drift = cosine_distance(centroid, baseline)
    if abs(drift) < 1e-12:
        drift = 0.0
    return {
        "status": "FAIL" if drift > threshold else "PASS",
        "drift_score": round(drift, 4),
        "threshold": threshold,
        "interpretation": "semantic content has shifted" if drift > threshold else "semantic content stable",
    }


def extraction_prompt_records(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    prompt_records: list[dict[str, str]] = []
    for record in records:
        preview = ""
        facts = record.get("extracted_facts", [])
        if facts:
            preview = str(facts[0].get("source_excerpt", ""))
        metadata = record.get("document_metadata", {}) if isinstance(record.get("document_metadata"), dict) else {}
        prompt_records.append(
            {
                "doc_id": str(metadata.get("doc_id", record.get("doc_id", ""))),
                "source_path": str(metadata.get("source_path", record.get("source_path", ""))),
                "content_preview": str(metadata.get("content_preview", preview[:8000])),
            }
        )
    return prompt_records


def validate_prompt_inputs(records: list[dict[str, Any]]) -> dict[str, Any]:
    valid = 0
    quarantined: list[dict[str, str]] = []
    quarantine_path = Path("outputs/quarantine/quarantine.jsonl")
    for record in extraction_prompt_records(records):
        try:
            validate(instance=record, schema=PROMPT_INPUT_SCHEMA)
            valid += 1
        except ValidationError as exc:
            quarantined.append({"record": json.dumps(record, sort_keys=True), "error": exc.message})
    if quarantined:
        write_jsonl(quarantine_path, quarantined)
    return {
        "status": "PASS" if not quarantined else "WARN",
        "valid_records": valid,
        "quarantined_records": len(quarantined),
        "quarantine_path": str(quarantine_path),
    }


def enforce_structured_llm_output(
    verdict_records: list[dict[str, Any]],
    baseline_path: str | None = None,
    warn_threshold: float = 0.02,
    fail_threshold: float = 0.05,
    *,
    source_label: str | None = None,
) -> dict[str, Any]:
    total = len(verdict_records)
    sample_errors = _sample_schema_errors(verdict_records, VERDICT_OUTPUT_SCHEMA)
    violations = len(sample_errors)
    rate = violations / max(total, 1)
    path = Path(baseline_path) if baseline_path else _default_ai_baseline_path("ai_metrics_baseline.json", source_label=source_label)
    baseline_rate = 0.0
    trend = "stable"
    if path.exists():
        baseline_rate = float(json.loads(path.read_text(encoding="utf-8")).get("baseline_violation_rate", 0.0))
        if rate > baseline_rate + 0.005:
            trend = "rising"
        elif rate < max(0.0, baseline_rate - 0.005):
            trend = "falling"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"written_at": utc_now(), "baseline_violation_rate": rate}, indent=2), encoding="utf-8")
    status = "PASS"
    if violations and rate > fail_threshold:
        status = "FAIL"
    elif violations or rate > warn_threshold or trend == "rising":
        status = "WARN"
    return {
        "status": status,
        "total_outputs": total,
        "valid_outputs": max(total - violations, 0),
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "baseline_violation_rate": round(baseline_rate, 4),
        "warn_threshold": round(warn_threshold, 4),
        "fail_threshold": round(fail_threshold, 4),
        "schema_name": "week2_structured_verdict_output",
        "sample_errors": sample_errors[:5],
    }


def check_output_schema_violation_rate(
    verdict_records: list[dict[str, Any]],
    baseline_path: str | None = None,
    warn_threshold: float = 0.02,
    *,
    source_label: str | None = None,
) -> dict[str, Any]:
    return enforce_structured_llm_output(
        verdict_records,
        baseline_path=baseline_path,
        warn_threshold=warn_threshold,
        source_label=source_label,
    )


def check_langsmith_trace_schema_contracts(trace_records: list[dict[str, Any]]) -> dict[str, Any]:
    if not trace_records:
        return {
            "status": "SKIPPED",
            "total_records": 0,
            "schema_invalid_records": 0,
            "total_contract_checks": 0,
            "failed_contract_checks": 0,
            "warned_contract_checks": 0,
            "failing_check_ids": [],
            "sample_errors": [],
        }

    schema_errors = _sample_schema_errors(trace_records, TRACE_RECORD_SCHEMA)
    contract_results = validate_traces(trace_records)
    failed_contracts = [result for result in contract_results if str(result.get("status", "")).upper() in {"FAIL", "ERROR"}]
    warned_contracts = [result for result in contract_results if str(result.get("status", "")).upper() == "WARN"]
    passed_contracts = [result for result in contract_results if str(result.get("status", "")).upper() == "PASS"]

    overall_status = _combine_statuses(
        "FAIL" if schema_errors else "PASS",
        *[str(result.get("status", "UNKNOWN")) for result in contract_results],
    )
    return {
        "status": overall_status,
        "total_records": len(trace_records),
        "schema_invalid_records": len(schema_errors),
        "valid_records": len(trace_records) - len(schema_errors),
        "total_contract_checks": len(contract_results),
        "passed_contract_checks": len(passed_contracts),
        "failed_contract_checks": len(failed_contracts),
        "warned_contract_checks": len(warned_contracts),
        "failing_check_ids": [str(result.get("check_id", "")) for result in failed_contracts],
        "warned_check_ids": [str(result.get("check_id", "")) for result in warned_contracts],
        "sample_errors": schema_errors[:5],
        "contract_messages": [str(result.get("message", "")) for result in failed_contracts[:5]],
    }


def build_ai_extension_report(
    extraction_records: list[dict[str, Any]],
    verdict_records: list[dict[str, Any]],
    trace_records: list[dict[str, Any]],
    *,
    source_label: str | None = None,
) -> dict[str, Any]:
    texts = [
        fact.get("text", "")
        for record in extraction_records
        for fact in record.get("extracted_facts", [])
        if fact.get("text")
    ]
    structured_output = enforce_structured_llm_output(verdict_records, source_label=source_label)
    report: dict[str, Any] = {
        "generated_at": utc_now(),
        "mode": "all",
        "embedding_drift": check_embedding_drift(texts, source_label=source_label),
        "prompt_input_validation": validate_prompt_inputs(extraction_records),
        "structured_llm_output_enforcement": structured_output,
        "llm_output_schema_rate": dict(structured_output),
        "langsmith_trace_schema_contracts": check_langsmith_trace_schema_contracts(trace_records),
    }
    if source_label:
        report["source_label"] = source_label
    return report


def ai_violation_records(report: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    prompt = report.get("prompt_input_validation", {})
    if prompt.get("status") in {"WARN", "FAIL"}:
        records.append(
            {
                "violation_id": str(uuid.uuid4()),
                "detected_at": utc_now(),
                "status": prompt.get("status"),
                "severity": "MEDIUM",
                "check_id": "ai.prompt_input_validation",
                "field_name": "content_preview",
                "message": "Some extraction records do not produce a valid prompt input preview and were quarantined.",
                "records_failing": int(prompt.get("quarantined_records", 0)),
                "candidate_files": ["outputs/week3/extractions.jsonl"],
                "blame_chain": [
                    {
                        "rank": 1,
                        "file_path": "outputs/week3/extractions.jsonl",
                        "commit_hash": hashlib.sha1(b"outputs/week3/extractions.jsonl").hexdigest(),
                        "author": "workspace@local",
                        "commit_timestamp": utc_now(),
                        "commit_message": "Real data produced quarantined prompt previews.",
                        "confidence_score": 0.72,
                    }
                ],
                "blast_radius": {
                    "affected_nodes": ["week3-document-refinery", "week7-ai-contract-extension"],
                    "affected_pipelines": ["prompt-input-validation"],
                    "estimated_records": int(prompt.get("quarantined_records", 0)),
                },
                "samples": [],
            }
        )
    structured_output = report.get("structured_llm_output_enforcement") or report.get("llm_output_schema_rate", {})
    if structured_output.get("status") in {"WARN", "FAIL"}:
        records.append(
            {
                "violation_id": str(uuid.uuid4()),
                "detected_at": utc_now(),
                "status": structured_output.get("status", "WARN"),
                "severity": "MEDIUM" if structured_output.get("status") == "FAIL" else "LOW",
                "check_id": "ai.structured_llm_output_enforcement",
                "field_name": "overall_verdict",
                "message": (
                    "Structured LLM output failed the verdict JSON Schema gate."
                    if structured_output.get("status") == "FAIL"
                    else "Structured LLM output produced schema-invalid verdicts that need review."
                ),
                "records_failing": int(structured_output.get("schema_violations", 0)),
                "candidate_files": ["outputs/week2/verdicts.jsonl"],
                "blame_chain": [],
                "blast_radius": {
                    "affected_nodes": ["week2-digital-courtroom"],
                    "affected_pipelines": ["structured-llm-output-enforcement"],
                    "estimated_records": int(structured_output.get("total_outputs", 0)),
                },
                "samples": structured_output.get("sample_errors", []),
            }
        )
    drift = report.get("embedding_drift", {})
    if drift.get("status") == "FAIL":
        records.append(
            {
                "violation_id": str(uuid.uuid4()),
                "detected_at": utc_now(),
                "status": "FAIL",
                "severity": "HIGH",
                "check_id": "ai.embedding_drift",
                "field_name": "extracted_facts.text",
                "message": "Embedding drift exceeded the configured threshold.",
                "records_failing": 1,
                "candidate_files": ["outputs/week3/extractions.jsonl"],
                "blame_chain": [],
                "blast_radius": {
                    "affected_nodes": ["week3-document-refinery", "week7-ai-contract-extension"],
                    "affected_pipelines": ["embedding-drift-detection"],
                    "estimated_records": 1,
                },
                "samples": [],
            }
        )
    trace_contracts = report.get("langsmith_trace_schema_contracts", {})
    if trace_contracts.get("status") in {"WARN", "FAIL"}:
        records.append(
            {
                "violation_id": str(uuid.uuid4()),
                "detected_at": utc_now(),
                "status": trace_contracts.get("status", "WARN"),
                "severity": "HIGH" if trace_contracts.get("status") == "FAIL" else "MEDIUM",
                "check_id": "ai.langsmith_trace_schema_contracts",
                "field_name": "id",
                "message": "LangSmith trace rows failed the AI trace schema contract gate.",
                "records_failing": int(trace_contracts.get("schema_invalid_records", 0)),
                "candidate_files": ["outputs/traces/runs.jsonl"],
                "blame_chain": [],
                "blast_radius": {
                    "affected_nodes": ["langsmith-trace-records", "week7-ai-contract-extension"],
                    "affected_pipelines": ["langsmith-trace-schema-contracts"],
                    "estimated_records": int(trace_contracts.get("total_records", 0)),
                },
                "samples": trace_contracts.get("sample_errors", []),
            }
        )
    return records


def main() -> int:
    args = parse_args()
    report: dict[str, Any] = {"generated_at": utc_now(), "mode": args.mode}
    extraction_records = load_jsonl(args.extractions) if args.extractions else []
    verdict_records = load_jsonl(args.verdicts) if args.verdicts else []
    trace_records = load_jsonl(args.traces) if args.traces else []
    source_label = _infer_source_label_from_paths(args.extractions, args.verdicts, args.traces)
    report["source_label"] = source_label
    if args.mode in {"all", "drift"}:
        texts = [fact.get("text", "") for record in extraction_records for fact in record.get("extracted_facts", []) if fact.get("text")]
        report["embedding_drift"] = check_embedding_drift(texts, source_label=source_label)
    if args.mode in {"all", "prompt"}:
        report["prompt_input_validation"] = validate_prompt_inputs(extraction_records)
    if args.mode in {"all", "output"}:
        structured_output = enforce_structured_llm_output(verdict_records, source_label=source_label)
        report["structured_llm_output_enforcement"] = structured_output
        report["llm_output_schema_rate"] = dict(structured_output)
    if args.mode in {"all", "traces"}:
        report["langsmith_trace_schema_contracts"] = check_langsmith_trace_schema_contracts(trace_records)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    metrics_path = output_path.parent / "ai_metrics.json"
    metrics_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    violations = ai_violation_records(report)
    if violations:
        violation_path = Path("violation_log/violations.jsonl")
        violation_path.parent.mkdir(parents=True, exist_ok=True)
        with violation_path.open("a", encoding="utf-8") as handle:
            for record in violations:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
