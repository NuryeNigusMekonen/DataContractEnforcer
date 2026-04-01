from __future__ import annotations

import argparse
import json
import re
import uuid
from collections import defaultdict
from pathlib import Path
import sys
from typing import Any
import yaml
from jsonschema import ValidationError, validate

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import (
    PASCAL_CASE_PATTERN,
    SHA1_PATTERN,
    SHA256_PATTERN,
    SEMVER_PATTERN,
    SCHEMA_VERSION_PATTERN,
    UUID_PATTERN,
    extract_field_observations,
    infer_scalar_type,
    load_jsonl,
    parse_timestamp,
    sha256_file,
    stringify,
    utc_now,
    normalize_contract_filename,
)


SEVERITY_BY_CHECK_TYPE = {
    "required": "CRITICAL",
    "type": "CRITICAL",
    "enum": "HIGH",
    "format": "HIGH",
    "pattern": "HIGH",
    "range": "CRITICAL",
    "cross_record": "CRITICAL",
    "cross_dataset": "HIGH",
    "drift": "MEDIUM",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate JSONL data against a generated contract.")
    parser.add_argument("--contract", required=True, help="Path to a YAML contract.")
    parser.add_argument("--data", required=True, help="Path to a JSONL data file.")
    parser.add_argument("--output", required=False, help="Path for the validation report.")
    return parser.parse_args()


def make_result(
    *,
    check_id: str,
    check_type: str,
    column_name: str,
    status: str,
    expected: Any = None,
    actual_value: Any = None,
    records_failing: int = 0,
    samples: list[str] | None = None,
    message: str = "",
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "check_type": check_type,
        "column_name": column_name,
        "status": status,
        "severity": SEVERITY_BY_CHECK_TYPE.get(check_type, "LOW"),
        "expected": expected,
        "actual_value": actual_value,
        "records_failing": records_failing,
        "samples": samples or [],
        "sample_failing": samples or [],
        "message": message,
    }


def matches_format(value: str, fmt: str) -> bool:
    if fmt == "uuid":
        return bool(UUID_PATTERN.match(value))
    if fmt == "date-time":
        return parse_timestamp(value) is not None
    return True


def matches_pattern(value: str, pattern: str) -> bool:
    return bool(re.match(pattern, value))


def validate_field_rules(fields: dict[str, dict[str, Any]], records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    observations = extract_field_observations(records)
    results: list[dict[str, Any]] = []
    for field_name, clause in sorted(fields.items()):
        observation = observations.get(field_name)
        values = observation.values if observation else []
        filtered = [value for value in values if value is not None]
        def emit_missing_error(check_suffix: str, check_type: str, message: str) -> None:
            results.append(
                make_result(
                    check_id=f"{field_name}.{check_suffix}",
                    check_type=check_type,
                    column_name=field_name,
                    status="ERROR",
                    expected="field present so check can execute",
                    actual_value="column missing",
                    records_failing=len(records),
                    message=message,
                )
            )
        if clause.get("required"):
            missing = len(records) if observation is None else max(0, len(records) - observation.paths_seen)
            status = "FAIL" if missing else "PASS"
            results.append(
                make_result(
                    check_id=f"{field_name}.required",
                    check_type="required",
                    column_name=field_name,
                    status=status,
                    expected="field present on every record",
                    actual_value=f"{missing} missing",
                    records_failing=missing,
                    message="Required field presence check.",
                )
            )
        if observation is None:
            if "enum" in clause:
                emit_missing_error("enum", "enum", "Enum check could not execute because the column is missing.")
            if "format" in clause:
                emit_missing_error("format", "format", "Format check could not execute because the column is missing.")
            if "pattern" in clause:
                emit_missing_error("pattern", "pattern", "Pattern check could not execute because the column is missing.")
            if "minimum" in clause or "maximum" in clause:
                emit_missing_error("range", "range", "Range check could not execute because the column is missing.")
            continue
        if filtered and clause.get("type"):
            inferred = infer_scalar_type(filtered)
            expected_type = clause["type"]
            compatible = inferred == expected_type or (expected_type == "number" and inferred == "integer")
            results.append(
                make_result(
                    check_id=f"{field_name}.type",
                    check_type="type",
                    column_name=field_name,
                    status="PASS" if compatible else "FAIL",
                    expected=expected_type,
                    actual_value=inferred,
                    records_failing=0 if compatible else len(filtered),
                    message="Observed type must match contract type.",
                )
            )
        if filtered and "enum" in clause:
            invalid = [value for value in filtered if value not in clause["enum"]]
            results.append(
                make_result(
                    check_id=f"{field_name}.enum",
                    check_type="enum",
                    column_name=field_name,
                    status="PASS" if not invalid else "FAIL",
                    expected=clause["enum"],
                    actual_value=stringify(invalid[0]) if invalid else "all conforming",
                    records_failing=len(invalid),
                    samples=[stringify(value) for value in invalid[:5]],
                    message="Enum values must match the contract.",
                )
            )
        if filtered and "format" in clause:
            invalid = [value for value in filtered if not isinstance(value, str) or not matches_format(value, clause["format"])]
            results.append(
                make_result(
                    check_id=f"{field_name}.format",
                    check_type="format",
                    column_name=field_name,
                    status="PASS" if not invalid else "FAIL",
                    expected=clause["format"],
                    actual_value=stringify(invalid[0]) if invalid else "all conforming",
                    records_failing=len(invalid),
                    samples=[stringify(value) for value in invalid[:5]],
                    message="String format must parse correctly.",
                )
            )
        if filtered and "pattern" in clause:
            invalid = [value for value in filtered if not isinstance(value, str) or not matches_pattern(value, clause["pattern"])]
            results.append(
                make_result(
                    check_id=f"{field_name}.pattern",
                    check_type="pattern",
                    column_name=field_name,
                    status="PASS" if not invalid else "FAIL",
                    expected=clause["pattern"],
                    actual_value=stringify(invalid[0]) if invalid else "all conforming",
                    records_failing=len(invalid),
                    samples=[stringify(value) for value in invalid[:5]],
                    message="Regex contract clause.",
                )
            )
        if filtered and ("minimum" in clause or "maximum" in clause):
            numeric = [float(value) for value in filtered if isinstance(value, (int, float))]
            below = [value for value in numeric if "minimum" in clause and value < float(clause["minimum"])]
            above = [value for value in numeric if "maximum" in clause and value > float(clause["maximum"])]
            failing = below + above
            status = "PASS" if not failing else "FAIL"
            results.append(
                make_result(
                    check_id=f"{field_name}.range",
                    check_type="range",
                    column_name=field_name,
                    status=status,
                    expected={"minimum": clause.get("minimum"), "maximum": clause.get("maximum")},
                    actual_value={
                        "min": min(numeric) if numeric else None,
                        "max": max(numeric) if numeric else None,
                    },
                    records_failing=len(failing),
                    samples=[stringify(value) for value in failing[:5]],
                    message="Numeric range must remain within contract bounds.",
                )
            )
    return results


def find_rubric_path(rubric_id: str) -> Path | None:
    candidates: list[Path] = []
    for directory in [Path("rubric"), Path("rubrics")]:
        if directory.exists():
            candidates.extend(path for path in directory.iterdir() if path.is_file())
    for path in candidates:
        if sha256_file(path) == rubric_id:
            return path
    return None


def validate_week2(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    hash_failures = 0
    hash_samples: list[str] = []
    score_failures = 0
    for record in records:
        rubric_id = record.get("rubric_id", "")
        if not rubric_id or find_rubric_path(rubric_id) is None:
            hash_failures += 1
            hash_samples.append(rubric_id)
        score_values = [item.get("score") for item in record.get("scores", {}).values() if isinstance(item, dict)]
        expected = round(sum(score_values) / len(score_values), 3) if score_values else None
        actual = round(float(record.get("overall_score", 0)), 3)
        if expected is None or expected != actual:
            score_failures += 1
    results.append(
        make_result(
            check_id="week2.rubric_hash_exists",
            check_type="cross_dataset",
            column_name="rubric_id",
            status="PASS" if hash_failures == 0 else "FAIL",
            expected="rubric_id matches an existing rubric file SHA-256",
            actual_value="all matched" if hash_failures == 0 else f"{hash_failures} missing",
            records_failing=hash_failures,
            samples=hash_samples[:5],
            message="Rubric hashes must resolve to a rubric file in the repo.",
        )
    )
    results.append(
        make_result(
            check_id="week2.overall_score_weighted_mean",
            check_type="cross_record",
            column_name="overall_score",
            status="PASS" if score_failures == 0 else "FAIL",
            expected="overall_score equals the arithmetic mean of score values",
            actual_value=f"{score_failures} mismatches",
            records_failing=score_failures,
            message="Overall score must align to the per-criterion scores.",
        )
    )
    return results


def validate_week3(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failures = 0
    samples: list[str] = []
    for record in records:
        entity_ids = {entity.get("entity_id") for entity in record.get("entities", [])}
        for fact in record.get("extracted_facts", []):
            missing_refs = [entity_ref for entity_ref in fact.get("entity_refs", []) if entity_ref not in entity_ids]
            if missing_refs:
                failures += len(missing_refs)
                samples.extend(missing_refs)
    return [
        make_result(
            check_id="week3.entity_refs_exist",
            check_type="cross_record",
            column_name="extracted_facts.entity_refs",
            status="PASS" if failures == 0 else "FAIL",
            expected="Every entity_ref exists in the sibling entities array",
            actual_value="all refs resolved" if failures == 0 else f"{failures} missing refs",
            records_failing=failures,
            samples=samples[:5],
            message="Nested entity references must resolve within the same record.",
        )
    ]


def validate_week4(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failures = 0
    samples: list[str] = []
    for record in records:
        node_ids = {node.get("node_id") for node in record.get("nodes", [])}
        for edge in record.get("edges", []):
            for endpoint in (edge.get("source"), edge.get("target")):
                if endpoint not in node_ids:
                    failures += 1
                    samples.append(str(endpoint))
    return [
        make_result(
            check_id="week4.edges_reference_nodes",
            check_type="cross_record",
            column_name="edges",
            status="PASS" if failures == 0 else "FAIL",
            expected="Every edge endpoint references a node_id in the same snapshot",
            actual_value="all endpoints resolved" if failures == 0 else f"{failures} broken endpoints",
            records_failing=failures,
            samples=samples[:5],
            message="Lineage edges must point at in-snapshot nodes.",
        )
    ]


def event_schema_path(record: dict[str, Any]) -> Path:
    event_type = record.get("event_type", "")
    schema_version = record.get("schema_version", "")
    return Path("schemas/events") / f"{event_type}-{schema_version}.json"


def validate_week5(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    after_failures = 0
    monotonic_failures = 0
    payload_failures = 0
    sequence_by_aggregate: dict[str, list[int]] = defaultdict(list)
    payload_samples: list[str] = []
    for record in records:
        occurred = parse_timestamp(record.get("occurred_at"))
        recorded = parse_timestamp(record.get("recorded_at"))
        if occurred is None or recorded is None or recorded < occurred:
            after_failures += 1
        sequence_by_aggregate[str(record.get("aggregate_id"))].append(int(record.get("sequence_number", 0)))
        schema_path = event_schema_path(record)
        try:
            if not schema_path.exists():
                raise FileNotFoundError(schema_path)
            with schema_path.open("r", encoding="utf-8") as handle:
                schema = json.load(handle)
            validate(instance=record.get("payload", {}), schema=schema)
        except (ValidationError, FileNotFoundError, json.JSONDecodeError) as exc:
            payload_failures += 1
            payload_samples.append(str(exc))
    for sequences in sequence_by_aggregate.values():
        expected = list(range(1, len(sequences) + 1))
        if sorted(sequences) != expected:
            monotonic_failures += 1
    return [
        make_result(
            check_id="week5.recorded_after_occurred",
            check_type="cross_record",
            column_name="recorded_at",
            status="PASS" if after_failures == 0 else "FAIL",
            expected="recorded_at >= occurred_at",
            actual_value=f"{after_failures} invalid records",
            records_failing=after_failures,
            message="Event records must preserve causal ordering.",
        ),
        make_result(
            check_id="week5.sequence_monotonic",
            check_type="cross_dataset",
            column_name="sequence_number",
            status="PASS" if monotonic_failures == 0 else "FAIL",
            expected="Per aggregate_id, sequence numbers are contiguous starting at 1",
            actual_value=f"{monotonic_failures} aggregates invalid",
            records_failing=monotonic_failures,
            message="Sequence numbers must be gap free per aggregate.",
        ),
        make_result(
            check_id="week5.payload_matches_schema",
            check_type="cross_record",
            column_name="payload",
            status="PASS" if payload_failures == 0 else "FAIL",
            expected="Payload validates against event schema",
            actual_value=f"{payload_failures} invalid payloads",
            records_failing=payload_failures,
            samples=payload_samples[:5],
            message="Event payloads must validate against the schema registry.",
        ),
    ]


def validate_traces(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    time_failures = 0
    token_failures = 0
    for record in records:
        start = parse_timestamp(record.get("start_time"))
        end = parse_timestamp(record.get("end_time"))
        if start is None or end is None or end <= start:
            time_failures += 1
        total = int(record.get("total_tokens", 0))
        prompt = int(record.get("prompt_tokens", 0))
        completion = int(record.get("completion_tokens", 0))
        if total != prompt + completion:
            token_failures += 1
    return [
        make_result(
            check_id="traces.end_after_start",
            check_type="cross_record",
            column_name="end_time",
            status="PASS" if time_failures == 0 else "FAIL",
            expected="end_time > start_time",
            actual_value=f"{time_failures} invalid runs",
            records_failing=time_failures,
            message="Trace timing must be strictly increasing.",
        ),
        make_result(
            check_id="traces.total_tokens_add_up",
            check_type="cross_record",
            column_name="total_tokens",
            status="PASS" if token_failures == 0 else "FAIL",
            expected="total_tokens = prompt_tokens + completion_tokens",
            actual_value=f"{token_failures} mismatches",
            records_failing=token_failures,
            message="Token accounting must balance.",
        ),
    ]


def numeric_baseline_path(contract_id: str) -> Path:
    return Path("schema_snapshots") / f"{contract_id}_baseline.json"


def aggregated_baseline_path() -> Path:
    return Path("schema_snapshots") / "baselines.json"


def compute_numeric_stats(records: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    observations = extract_field_observations(records)
    stats: dict[str, dict[str, float]] = {}
    for field_name, observation in observations.items():
        numeric = [float(value) for value in observation.values if isinstance(value, (int, float))]
        if numeric:
            mean = sum(numeric) / len(numeric)
            variance = sum((value - mean) ** 2 for value in numeric) / len(numeric)
            stats[field_name] = {"mean": mean, "stddev": variance ** 0.5}
    return stats


def drift_results(contract_id: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    current = compute_numeric_stats(records)
    baseline_path = numeric_baseline_path(contract_id)
    if not baseline_path.exists():
        baseline_path.write_text(json.dumps({"written_at": utc_now(), "columns": current}, indent=2), encoding="utf-8")
        aggregate_path = aggregated_baseline_path()
        aggregate = {}
        if aggregate_path.exists():
            aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
        aggregate[contract_id] = {"written_at": utc_now(), "columns": current}
        aggregate_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
        return [
            make_result(
                check_id="baseline.initialized",
                check_type="drift",
                column_name="*",
                status="PASS",
                expected="baseline file created",
                actual_value=str(baseline_path),
                message="No prior baseline existed, so this run established it.",
            )
        ]
    baseline = json.loads(baseline_path.read_text(encoding="utf-8")).get("columns", {})
    results: list[dict[str, Any]] = []
    for field_name, current_stats in sorted(current.items()):
        stored = baseline.get(field_name)
        if not stored:
            continue
        denominator = max(float(stored.get("stddev", 0.0)), 1e-9)
        z_score = abs(float(current_stats["mean"]) - float(stored["mean"])) / denominator
        if z_score > 3:
            status = "FAIL"
        elif z_score > 2:
            status = "WARN"
        else:
            status = "PASS"
        results.append(
            make_result(
                check_id=f"{field_name}.drift",
                check_type="drift",
                column_name=field_name,
                status=status,
                expected={"mean": stored["mean"], "stddev": stored["stddev"]},
                actual_value={"mean": current_stats["mean"], "z_score": round(z_score, 3)},
                records_failing=0 if status == "PASS" else 1,
                message="Numeric mean drifted against the stored baseline.",
            )
        )
    return results


def dataset_specific_results(dataset: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if dataset == "week2_verdicts":
        return validate_week2(records)
    if dataset == "week3_extractions":
        return validate_week3(records)
    if dataset == "week4_lineage":
        return validate_week4(records)
    if dataset == "week5_events":
        return validate_week5(records)
    if dataset == "traces":
        return validate_traces(records)
    return []


def summarize(results: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"PASS": 0, "WARN": 0, "FAIL": 0, "ERROR": 0}
    for result in results:
        summary[result["status"]] = summary.get(result["status"], 0) + 1
    return summary


def default_output_path(contract_id: str) -> Path:
    return Path("validation_reports") / f"{normalize_contract_filename(contract_id)}_{utc_now().replace(':', '').replace('-', '')}.json"


def main() -> int:
    args = parse_args()
    with Path(args.contract).open("r", encoding="utf-8") as handle:
        contract = yaml.safe_load(handle)
    records = load_jsonl(args.data)
    results = validate_field_rules(contract.get("fields", {}), records)
    results.extend(dataset_specific_results(contract.get("dataset", "generic"), records))
    results.extend(drift_results(contract.get("contract_id", "contract"), records))
    summary = summarize(results)
    report = {
        "report_id": str(uuid.uuid4()),
        "snapshot_id": sha256_file(args.data),
        "run_timestamp": utc_now(),
        "generated_at": utc_now(),
        "contract_id": contract.get("contract_id"),
        "dataset": contract.get("dataset"),
        "data_path": args.data,
        "record_count": len(records),
        "total_checks": len(results),
        "passed": summary.get("PASS", 0),
        "failed": summary.get("FAIL", 0),
        "warned": summary.get("WARN", 0),
        "errored": summary.get("ERROR", 0),
        "summary": summary,
        "results": results,
    }
    output_path = Path(args.output) if args.output else default_output_path(str(contract.get("contract_id", "contract")))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
