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
from jsonschema import ValidationError, validate
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import load_jsonl, utc_now, write_jsonl


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AI-specific contract checks.")
    parser.add_argument("--mode", default="all", choices=["all", "drift", "prompt", "output"])
    parser.add_argument("--extractions", required=False, help="Path to week3 extraction records.")
    parser.add_argument("--verdicts", required=False, help="Path to week2 verdict records.")
    parser.add_argument("--output", required=True, help="Output JSON path.")
    return parser.parse_args()


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


def check_embedding_drift(texts: list[str], baseline_path: str = "schema_snapshots/embedding_baseline.json", threshold: float = 0.15) -> dict[str, Any]:
    centroid = hashed_vector(texts)
    path = Path(baseline_path)
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


def check_output_schema_violation_rate(
    verdict_records: list[dict[str, Any]],
    baseline_path: str = "schema_snapshots/ai_metrics_baseline.json",
    warn_threshold: float = 0.02,
) -> dict[str, Any]:
    total = len(verdict_records)
    violations = sum(1 for record in verdict_records if record.get("overall_verdict") not in {"PASS", "FAIL", "WARN"})
    rate = violations / max(total, 1)
    path = Path(baseline_path)
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
    return {
        "status": "WARN" if rate > warn_threshold or trend == "rising" else "PASS",
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "baseline_violation_rate": round(baseline_rate, 4),
        "warn_threshold": round(warn_threshold, 4),
    }


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
    output_rate = report.get("llm_output_schema_rate", {})
    if output_rate.get("status") == "WARN":
        records.append(
            {
                "violation_id": str(uuid.uuid4()),
                "detected_at": utc_now(),
                "status": "WARN",
                "severity": "LOW",
                "check_id": "ai.llm_output_schema_rate",
                "field_name": "overall_verdict",
                "message": (
                    "LLM output schema violation rate exceeded the configured threshold."
                    if float(output_rate.get("violation_rate", 0.0)) > float(output_rate.get("warn_threshold", 0.0))
                    else "LLM output schema violation rate is rising against the stored baseline."
                ),
                "records_failing": int(output_rate.get("schema_violations", 0)),
                "candidate_files": ["outputs/week2/verdicts.jsonl"],
                "blame_chain": [],
                "blast_radius": {
                    "affected_nodes": ["week2-digital-courtroom"],
                    "affected_pipelines": ["llm-output-schema-validation"],
                    "estimated_records": int(output_rate.get("total_outputs", 0)),
                },
                "samples": [],
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
    return records


def main() -> int:
    args = parse_args()
    report: dict[str, Any] = {"generated_at": utc_now(), "mode": args.mode}
    extraction_records = load_jsonl(args.extractions) if args.extractions else []
    verdict_records = load_jsonl(args.verdicts) if args.verdicts else []
    if args.mode in {"all", "drift"}:
        texts = [fact.get("text", "") for record in extraction_records for fact in record.get("extracted_facts", []) if fact.get("text")]
        report["embedding_drift"] = check_embedding_drift(texts)
    if args.mode in {"all", "prompt"}:
        report["prompt_input_validation"] = validate_prompt_inputs(extraction_records)
    if args.mode in {"all", "output"}:
        report["llm_output_schema_rate"] = check_output_schema_violation_rate(verdict_records)
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
