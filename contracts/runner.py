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
    apply_dataset_overrides,
    build_field_clause,
    extract_field_observations,
    infer_scalar_type,
    load_jsonl,
    profile_records,
    parse_timestamp,
    sha256_file,
    stringify,
    utc_now,
    normalize_contract_filename,
)
from contracts.adapter import SchemaAdapter
from contracts.evolution import build_compatibility_report, contract_version


SEVERITY_BY_CHECK_TYPE = {
    "required": "CRITICAL",
    "type": "CRITICAL",
    "enum": "HIGH",
    "format": "HIGH",
    "pattern": "HIGH",
    "range": "CRITICAL",
    "contract_clause": "HIGH",
    "cross_record": "CRITICAL",
    "cross_dataset": "HIGH",
    "drift": "MEDIUM",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate JSONL data against a generated contract.")
    parser.add_argument("--contract", required=True, help="Path to a YAML contract.")
    parser.add_argument("--data", required=True, help="Path to a JSONL data file.")
    parser.add_argument(
        "--mode",
        default="AUDIT",
        choices=["AUDIT", "WARN", "ENFORCE", "audit", "warn", "enforce"],
        help="Validation mode: AUDIT logs only, WARN blocks on CRITICAL failures, ENFORCE blocks on CRITICAL/HIGH failures.",
    )
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
    severity: str | None = None,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "check_type": check_type,
        "column_name": column_name,
        "status": status,
        "severity": severity or SEVERITY_BY_CHECK_TYPE.get(check_type, "LOW"),
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


def normalize_clause_severity(severity: str | None) -> str:
    mapping = {
        "error": "CRITICAL",
        "warn": "WARNING",
        "warning": "WARNING",
        "info": "LOW",
    }
    return mapping.get(str(severity or "").lower(), "HIGH")


def raw_values_for_path(record: dict[str, Any], field_path: str) -> list[Any]:
    parts = field_path.split(".")
    current: list[Any] = [record]
    for part in parts:
        next_values: list[Any] = []
        for value in current:
            if isinstance(value, dict):
                if part in value:
                    next_values.append(value[part])
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and part in item:
                        next_values.append(item[part])
        current = next_values
    return current


def scalar_values_for_path(record: dict[str, Any], field_path: str) -> list[Any]:
    flattened: list[Any] = []

    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
            return
        flattened.append(value)

    for value in raw_values_for_path(record, field_path):
        visit(value)
    return [value for value in flattened if value is not None]


def first_scalar_for_path(record: dict[str, Any], field_path: str) -> Any:
    values = scalar_values_for_path(record, field_path)
    return values[0] if values else None


def record_matches_condition(record: dict[str, Any], condition: dict[str, Any] | None) -> bool:
    if not condition:
        return True
    observed = scalar_values_for_path(record, str(condition.get("field", "")))
    if not observed:
        return False
    if "equals" in condition:
        return any(value == condition["equals"] for value in observed)
    if "in" in condition:
        allowed = set(condition["in"])
        return any(value in allowed for value in observed)
    return False


def clause_result(
    clause: dict[str, Any],
    *,
    column_name: str,
    status: str,
    expected: Any,
    actual_value: Any,
    records_failing: int,
    samples: list[str] | None,
    message: str,
) -> dict[str, Any]:
    return make_result(
        check_id=str(clause.get("id", "contract_clause")),
        check_type="contract_clause",
        column_name=column_name,
        status=status,
        expected=expected,
        actual_value=actual_value,
        records_failing=records_failing,
        samples=samples,
        message=message,
        severity=normalize_clause_severity(str(clause.get("severity", ""))),
    )


def validate_contract_clauses(clauses: list[dict[str, Any]], records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for clause in clauses:
        rule = clause.get("rule", {})
        rule_type = rule.get("type")
        if not rule_type:
            continue
        if rule_type == "field_format":
            field = str(rule["field"])
            invalid: list[str] = []
            for record in records:
                for value in scalar_values_for_path(record, field):
                    if not isinstance(value, str) or not matches_format(value, str(rule["format"])):
                        invalid.append(stringify(value))
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not invalid else "FAIL",
                    expected={"format": rule["format"]},
                    actual_value="all conforming" if not invalid else invalid[0],
                    records_failing=len(invalid),
                    samples=invalid[:5],
                    message=str(clause.get("description", "Field format clause.")),
                )
            )
            continue
        if rule_type == "field_pattern":
            field = str(rule["field"])
            invalid = []
            for record in records:
                for value in scalar_values_for_path(record, field):
                    if not isinstance(value, str) or not matches_pattern(value, str(rule["pattern"])):
                        invalid.append(stringify(value))
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not invalid else "FAIL",
                    expected={"pattern": rule["pattern"]},
                    actual_value="all conforming" if not invalid else invalid[0],
                    records_failing=len(invalid),
                    samples=invalid[:5],
                    message=str(clause.get("description", "Pattern clause.")),
                )
            )
            continue
        if rule_type == "field_enum":
            field = str(rule["field"])
            allowed = list(rule.get("allowed", []))
            invalid = []
            for record in records:
                for value in scalar_values_for_path(record, field):
                    if value not in allowed:
                        invalid.append(stringify(value))
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not invalid else "FAIL",
                    expected={"allowed": allowed},
                    actual_value="all conforming" if not invalid else invalid[0],
                    records_failing=len(invalid),
                    samples=invalid[:5],
                    message=str(clause.get("description", "Enum clause.")),
                )
            )
            continue
        if rule_type == "numeric_range":
            field = str(rule["field"])
            minimum = rule.get("minimum")
            maximum = rule.get("maximum")
            invalid = []
            numeric: list[float] = []
            for record in records:
                for value in scalar_values_for_path(record, field):
                    if not isinstance(value, (int, float)):
                        invalid.append(stringify(value))
                        continue
                    numeric.append(float(value))
                    if minimum is not None and float(value) < float(minimum):
                        invalid.append(stringify(value))
                    if maximum is not None and float(value) > float(maximum):
                        invalid.append(stringify(value))
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not invalid else "FAIL",
                    expected={"minimum": minimum, "maximum": maximum},
                    actual_value={"min": min(numeric) if numeric else None, "max": max(numeric) if numeric else None},
                    records_failing=len(invalid),
                    samples=invalid[:5],
                    message=str(clause.get("description", "Numeric range clause.")),
                )
            )
            continue
        if rule_type == "array_min_items":
            field = str(rule["field"])
            minimum = int(rule.get("minimum", 0))
            failing: list[str] = []
            for index, record in enumerate(records):
                observed = raw_values_for_path(record, field)
                array_value = observed[0] if observed else None
                if not isinstance(array_value, list) or len(array_value) < minimum:
                    failing.append(f"record[{index}]")
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not failing else "FAIL",
                    expected={"minimum_items": minimum},
                    actual_value="all conforming" if not failing else f"{len(failing)} records below minimum",
                    records_failing=len(failing),
                    samples=failing[:5],
                    message=str(clause.get("description", "Array cardinality clause.")),
                )
            )
            continue
        if rule_type == "string_length":
            field = str(rule["field"])
            minimum = int(rule.get("minimum", 0))
            invalid = []
            for record in records:
                for value in scalar_values_for_path(record, field):
                    if not isinstance(value, str) or len(value) < minimum:
                        invalid.append(stringify(value))
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not invalid else "FAIL",
                    expected={"minimum_length": minimum},
                    actual_value="all conforming" if not invalid else invalid[0],
                    records_failing=len(invalid),
                    samples=invalid[:5],
                    message=str(clause.get("description", "String length clause.")),
                )
            )
            continue
        if rule_type == "multi_field_numeric_range":
            fields = [str(field) for field in rule.get("fields", [])]
            minimum = rule.get("minimum")
            maximum = rule.get("maximum")
            invalid = []
            numeric: list[float] = []
            for field in fields:
                for record in records:
                    for value in scalar_values_for_path(record, field):
                        if not isinstance(value, (int, float)):
                            invalid.append(f"{field}={stringify(value)}")
                            continue
                        numeric.append(float(value))
                        if minimum is not None and float(value) < float(minimum):
                            invalid.append(f"{field}={stringify(value)}")
                        if maximum is not None and float(value) > float(maximum):
                            invalid.append(f"{field}={stringify(value)}")
            results.append(
                clause_result(
                    clause,
                    column_name=",".join(fields),
                    status="PASS" if not invalid else "FAIL",
                    expected={"minimum": minimum, "maximum": maximum},
                    actual_value={"checked_fields": fields, "count": len(numeric)},
                    records_failing=len(invalid),
                    samples=invalid[:5],
                    message=str(clause.get("description", "Multi-field numeric range clause.")),
                )
            )
            continue
        if rule_type == "temporal_order":
            left_field = str(rule["left_field"])
            right_field = str(rule["right_field"])
            failing = []
            for index, record in enumerate(records):
                left = parse_timestamp(first_scalar_for_path(record, left_field))
                right = parse_timestamp(first_scalar_for_path(record, right_field))
                if left is None or right is None or right < left:
                    failing.append(f"record[{index}]")
            results.append(
                clause_result(
                    clause,
                    column_name=f"{left_field},{right_field}",
                    status="PASS" if not failing else "FAIL",
                    expected={rule.get("operator", "<="): [left_field, right_field]},
                    actual_value="all conforming" if not failing else f"{len(failing)} records invalid",
                    records_failing=len(failing),
                    samples=failing[:5],
                    message=str(clause.get("description", "Temporal ordering clause.")),
                )
            )
            continue
        if rule_type == "group_monotonic":
            field = str(rule["field"])
            group_by = [str(item) for item in rule.get("group_by", [])]
            sequences: dict[tuple[Any, ...], list[int]] = defaultdict(list)
            for record in records:
                key = tuple(first_scalar_for_path(record, group_field) for group_field in group_by)
                value = first_scalar_for_path(record, field)
                if isinstance(value, int):
                    sequences[key].append(value)
                elif isinstance(value, float):
                    sequences[key].append(int(value))
            failing = []
            for key, sequence in sequences.items():
                expected = list(range(1, len(sequence) + 1))
                if sorted(sequence) != expected:
                    failing.append("|".join(str(item) for item in key))
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not failing else "FAIL",
                    expected={"group_by": group_by, "rule": "contiguous_from_1"},
                    actual_value="all conforming" if not failing else f"{len(failing)} groups invalid",
                    records_failing=len(failing),
                    samples=failing[:5],
                    message=str(clause.get("description", "Group monotonicity clause.")),
                )
            )
            continue
        if rule_type == "conditional_numeric_range":
            field = str(rule["field"])
            minimum = rule.get("minimum")
            maximum = rule.get("maximum")
            failing = []
            for index, record in enumerate(records):
                if not record_matches_condition(record, rule.get("when")):
                    continue
                observed = scalar_values_for_path(record, field)
                if not observed:
                    failing.append(f"record[{index}] missing {field}")
                    continue
                for value in observed:
                    if not isinstance(value, (int, float)):
                        failing.append(f"record[{index}]={stringify(value)}")
                        continue
                    if minimum is not None and float(value) < float(minimum):
                        failing.append(f"record[{index}]={stringify(value)}")
                    if maximum is not None and float(value) > float(maximum):
                        failing.append(f"record[{index}]={stringify(value)}")
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not failing else "FAIL",
                    expected={"when": rule.get("when"), "minimum": minimum, "maximum": maximum},
                    actual_value="all conforming" if not failing else failing[0],
                    records_failing=len(failing),
                    samples=failing[:5],
                    message=str(clause.get("description", "Conditional numeric range clause.")),
                )
            )
            continue
        if rule_type == "conditional_pattern":
            field = str(rule["field"])
            pattern = str(rule["pattern"])
            failing = []
            for index, record in enumerate(records):
                if not record_matches_condition(record, rule.get("when")):
                    continue
                observed = scalar_values_for_path(record, field)
                if not observed:
                    failing.append(f"record[{index}] missing {field}")
                    continue
                for value in observed:
                    if not isinstance(value, str) or not matches_pattern(value, pattern):
                        failing.append(f"record[{index}]={stringify(value)}")
            results.append(
                clause_result(
                    clause,
                    column_name=field,
                    status="PASS" if not failing else "FAIL",
                    expected={"when": rule.get("when"), "pattern": pattern},
                    actual_value="all conforming" if not failing else failing[0],
                    records_failing=len(failing),
                    samples=failing[:5],
                    message=str(clause.get("description", "Conditional pattern clause.")),
                )
            )
            continue
    return results


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
    score_range_failures = 0
    score_range_samples: list[str] = []
    for record in records:
        rubric_id = record.get("rubric_id", "")
        if not rubric_id or find_rubric_path(rubric_id) is None:
            hash_failures += 1
            hash_samples.append(rubric_id)
        score_values = []
        for criterion, item in record.get("scores", {}).items():
            if not isinstance(item, dict):
                continue
            score = item.get("score")
            score_values.append(score)
            if not isinstance(score, int) or not 1 <= score <= 5:
                score_range_failures += 1
                score_range_samples.append(f"{criterion}={stringify(score)}")
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
            check_id="week2.score_range_valid",
            check_type="cross_record",
            column_name="scores.*.score",
            status="PASS" if score_range_failures == 0 else "FAIL",
            expected="every nested score is an integer between 1 and 5",
            actual_value="all matched" if score_range_failures == 0 else f"{score_range_failures} out-of-range scores",
            records_failing=score_range_failures,
            samples=score_range_samples[:5],
            message="Nested criterion scores must remain on the rubric's 1-5 scale.",
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


def validate_week1(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    empty_code_ref_failures = 0
    missing_file_failures = 0
    confidence_failures = 0
    samples_empty: list[str] = []
    samples_missing: list[str] = []
    samples_confidence: list[str] = []
    for index, record in enumerate(records):
        code_refs = record.get("code_refs")
        if not isinstance(code_refs, list) or not code_refs:
            empty_code_ref_failures += 1
            samples_empty.append(f"record[{index}]")
            continue
        for code_ref in code_refs:
            file_path = code_ref.get("file") if isinstance(code_ref, dict) else None
            if not isinstance(file_path, str) or not file_path.strip():
                missing_file_failures += 1
                samples_missing.append(f"record[{index}]")
            confidence = code_ref.get("confidence") if isinstance(code_ref, dict) else None
            if not isinstance(confidence, (int, float)) or float(confidence) < 0.0 or float(confidence) > 1.0:
                confidence_failures += 1
                samples_confidence.append(f"record[{index}]={stringify(confidence)}")
    return [
        make_result(
            check_id="week1.code_refs_non_empty",
            check_type="cross_record",
            column_name="code_refs",
            status="PASS" if empty_code_ref_failures == 0 else "FAIL",
            expected="Every intent record contains at least one code_ref",
            actual_value="all matched" if empty_code_ref_failures == 0 else f"{empty_code_ref_failures} empty code_refs arrays",
            records_failing=empty_code_ref_failures,
            samples=samples_empty[:5],
            message="Intent records must retain code references for downstream traceability.",
        ),
        make_result(
            check_id="week1.code_ref_file_present",
            check_type="cross_record",
            column_name="code_refs.file",
            status="PASS" if missing_file_failures == 0 else "FAIL",
            expected="Every code_ref contains a non-empty file path",
            actual_value="all matched" if missing_file_failures == 0 else f"{missing_file_failures} missing file paths",
            records_failing=missing_file_failures,
            samples=samples_missing[:5],
            message="Each referenced code location must keep its file path.",
        ),
        make_result(
            check_id="week1.code_ref_confidence_scale",
            check_type="cross_record",
            column_name="code_refs.confidence",
            status="PASS" if confidence_failures == 0 else "FAIL",
            expected="All confidence values are numeric and between 0.0 and 1.0",
            actual_value="all matched" if confidence_failures == 0 else f"{confidence_failures} invalid confidence values",
            records_failing=confidence_failures,
            samples=samples_confidence[:5],
            message="Code reference confidence values must stay on the unit interval.",
        ),
    ]


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
    return Path("schema_snapshots") / f"{normalize_contract_filename(contract_id)}_baseline.json"


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


def load_numeric_baseline(contract_id: str) -> tuple[dict[str, Any], str | None]:
    aggregate_path = aggregated_baseline_path()
    if aggregate_path.exists():
        aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
        normalized_contract_id = normalize_contract_filename(contract_id)
        for candidate in (contract_id, normalized_contract_id):
            baseline = aggregate.get(candidate)
            if isinstance(baseline, dict):
                columns = baseline.get("columns", {})
                if isinstance(columns, dict):
                    return columns, str(aggregate_path)
    baseline_path = numeric_baseline_path(contract_id)
    if baseline_path.exists():
        columns = json.loads(baseline_path.read_text(encoding="utf-8")).get("columns", {})
        if isinstance(columns, dict):
            return columns, str(baseline_path)
    return {}, None


def drift_results(contract_id: str, records: list[dict[str, Any]], *, persist_baselines: bool = True) -> list[dict[str, Any]]:
    current = compute_numeric_stats(records)
    baseline, baseline_source = load_numeric_baseline(contract_id)
    if not baseline:
        if not persist_baselines:
            return [
                make_result(
                    check_id="baseline.unavailable",
                    check_type="drift",
                    column_name="*",
                    status="PASS",
                    expected="existing baseline file",
                    actual_value="baseline missing; what-if run did not persist one",
                    message="No drift baseline was available, and this validation run was read-only.",
                )
            ]
        baseline_path = numeric_baseline_path(contract_id)
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
                actual_value={
                    "mean": current_stats["mean"],
                    "z_score": round(z_score, 3),
                    "baseline_source": baseline_source,
                },
                records_failing=0 if status == "PASS" else 1,
                message="Numeric mean drifted against the stored baseline.",
                severity="WARNING" if status == "WARN" else None,
            )
        )
    return results


def dataset_specific_results(dataset: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if dataset == "week1_intents":
        return validate_week1(records)
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


def overall_status(summary: dict[str, int]) -> str:
    if summary.get("ERROR", 0) > 0 or summary.get("FAIL", 0) > 0:
        return "FAIL"
    if summary.get("WARN", 0) > 0:
        return "WARN"
    return "PASS"


def should_block(mode: str, results: list[dict[str, Any]]) -> bool:
    normalized_mode = mode.upper()
    if normalized_mode == "AUDIT":
        return False
    failures = [result for result in results if result.get("status") in {"FAIL", "ERROR"}]
    if normalized_mode == "WARN":
        return any(str(result.get("severity", "LOW")).upper() == "CRITICAL" for result in failures)
    if normalized_mode == "ENFORCE":
        return any(str(result.get("severity", "HIGH")).upper() in {"CRITICAL", "HIGH"} for result in failures)
    return False


def build_validation_report(contract: dict[str, Any], evaluation: dict[str, Any], *, data_path: str, snapshot_id: str) -> dict[str, Any]:
    return {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract.get("contract_id"),
        "snapshot_id": snapshot_id,
        "run_timestamp": utc_now(),
        "generated_at": utc_now(),
        "mode": evaluation["mode"],
        "blocking": evaluation["blocking"],
        "overall_status": evaluation["overall_status"],
        "dataset": contract.get("dataset"),
        "expected_contract_version": evaluation["expected_contract_version"],
        "data_path": data_path,
        "record_count": evaluation["record_count"],
        "raw_record_count": evaluation["raw_record_count"],
        "total_checks": evaluation["total_checks"],
        "passed": evaluation["passed"],
        "failed": evaluation["failed"],
        "warned": evaluation["warned"],
        "errored": evaluation["errored"],
        "summary": evaluation["summary"],
        "schema_evolution": evaluation["schema_evolution"],
        "adapter": evaluation["adapter"],
        "results": evaluation["results"],
    }


def default_output_path(contract_id: str) -> Path:
    return Path("validation_reports") / f"{normalize_contract_filename(contract_id)}_{utc_now().replace(':', '').replace('-', '')}.json"


def registry_path() -> str | None:
    candidate = Path("contract_registry/subscriptions.yaml")
    return str(candidate) if candidate.exists() else None


def observed_contract(contract: dict[str, Any], records: list[dict[str, Any]], schema_version: str) -> dict[str, Any]:
    dataset = str(contract.get("dataset", "generic"))
    target_fields = contract.get("fields", {}) if isinstance(contract.get("fields"), dict) else {}
    profiles = profile_records(records)
    fields = {field_name: build_field_clause(field_name, profile) for field_name, profile in profiles.items()}
    apply_dataset_overrides(dataset, fields)
    for field_name, clause in fields.items():
        target_clause = target_fields.get(field_name, {})
        if "enum" not in target_clause:
            clause.pop("enum", None)
    return {
        "id": contract.get("id"),
        "contract_id": contract.get("contract_id"),
        "dataset": dataset,
        "schema_version": schema_version,
        "info": {
            "version": schema_version,
            "title": contract.get("info", {}).get("title", contract.get("contract_id", "contract")),
        },
        "fields": fields,
        "profiling": {
            "statistics": {
                field_name: profile.get("stats", {})
                for field_name, profile in profiles.items()
                if profile.get("stats")
            }
        },
    }


def adapter_result(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("attempted") and not payload.get("succeeded"):
        return make_result(
            check_id="schema.adapter",
            check_type="contract_clause",
            column_name="*",
            status="FAIL",
            expected=f"adapter available for {payload.get('source_version')} -> {payload.get('target_version')}",
            actual_value=payload.get("failure_reason", "adapter failed"),
            records_failing=1,
            message="Schema adaptation failed before validation could continue.",
            severity="HIGH",
        )
    if payload.get("attempted") and not payload.get("applied"):
        return make_result(
            check_id="schema.adapter",
            check_type="contract_clause",
            column_name="*",
            status="FAIL",
            expected=f"{payload.get('source_version')} -> {payload.get('target_version')}",
            actual_value="no records were transformed",
            records_failing=1,
            message="Schema adaptation was attempted, but no transformable records matched the configured rules.",
            severity="HIGH",
        )
    if payload.get("applied"):
        return make_result(
            check_id="schema.adapter",
            check_type="contract_clause",
            column_name="*",
            status="PASS",
            expected=f"{payload.get('source_version')} -> {payload.get('target_version')}",
            actual_value="adapter rules applied",
            records_failing=0,
            message="Schema adapter transformed incoming records into the expected contract shape.",
            severity="LOW",
        )
    return make_result(
        check_id="schema.adapter",
        check_type="contract_clause",
        column_name="*",
        status="PASS",
        expected="no adaptation required",
        actual_value="adapter not applied",
        records_failing=0,
        message="Incoming records already matched the expected contract version.",
        severity="LOW",
    )


def compatibility_context(contract: dict[str, Any], records: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any], SchemaAdapter]:
    expected_version = contract_version(contract)
    adapter = SchemaAdapter(str(contract.get("contract_id", "")))
    detection = adapter.detect_source_version(records, expected_version)
    observed = observed_contract(contract, records, detection["detected_schema_version"])
    compatibility = build_compatibility_report(observed, contract, registry_path())
    return detection, compatibility, adapter


def evaluate_contract_records(
    contract: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    mode: str = "AUDIT",
    data_path: str = "",
    attempt_adapter: bool = True,
    persist_baselines: bool = True,
) -> dict[str, Any]:
    normalized_mode = str(mode).upper()
    detection, compatibility, adapter = compatibility_context(contract, records)
    expected_version = contract_version(contract)

    if attempt_adapter:
        adapter_payload = adapter.transform_records(records, detection["detected_schema_version"], expected_version)
    else:
        adapter_payload = {
            "attempted": False,
            "applied": False,
            "succeeded": True,
            "fallback_succeeded": compatibility["compatibility_verdict"] != "breaking_change",
            "source_version": detection["detected_schema_version"],
            "target_version": expected_version,
            "failure_reason": "",
            "rule_logs": [],
            "original_samples": [],
            "transformed_samples": [],
            "records": records,
        }

    validation_records = adapter_payload["records"] if adapter_payload.get("succeeded") else records
    post_transform_compatibility = None
    if attempt_adapter and adapter_payload.get("applied"):
        post_observed = observed_contract(contract, validation_records, expected_version)
        post_transform_compatibility = build_compatibility_report(post_observed, contract, registry_path())
        adapter_payload["fallback_succeeded"] = post_transform_compatibility["compatibility_verdict"] != "breaking_change"
    elif not attempt_adapter:
        adapter_payload["fallback_succeeded"] = False

    results = [adapter_result(adapter_payload)]
    results.extend(validate_field_rules(contract.get("fields", {}), validation_records))
    results.extend(validate_contract_clauses(contract.get("clauses", []), validation_records))
    results.extend(dataset_specific_results(contract.get("dataset", "generic"), validation_records))
    results.extend(drift_results(contract.get("contract_id", "contract"), validation_records, persist_baselines=persist_baselines))
    summary = summarize(results)
    blocking = should_block(normalized_mode, results)
    return {
        "mode": normalized_mode,
        "blocking": blocking,
        "overall_status": overall_status(summary),
        "expected_contract_version": expected_version,
        "record_count": len(validation_records),
        "raw_record_count": len(records),
        "total_checks": len(results),
        "passed": summary.get("PASS", 0),
        "failed": summary.get("FAIL", 0),
        "warned": summary.get("WARN", 0),
        "errored": summary.get("ERROR", 0),
        "summary": summary,
        "schema_evolution": {
            "original_schema_version": detection["original_schema_version"],
            "detected_schema_version": detection["detected_schema_version"],
            "compatibility_classification": compatibility["compatibility_verdict"],
            "change_counts": compatibility["change_counts"],
            "changes": compatibility["changes"],
            "renames": compatibility["renames"],
            "primary_breaking_change": compatibility["primary_breaking_change"],
            "notification": compatibility["notification"],
            "post_transform_compatibility": None
            if post_transform_compatibility is None
            else post_transform_compatibility["compatibility_verdict"],
        },
        "adapter": {
            "attempted": adapter_payload["attempted"],
            "applied": adapter_payload["applied"],
            "succeeded": adapter_payload["succeeded"],
            "fallback_succeeded": adapter_payload["fallback_succeeded"],
            "source_version": adapter_payload["source_version"],
            "target_version": adapter_payload["target_version"],
            "failure_reason": adapter_payload["failure_reason"],
            "rules_applied": adapter.summarize_rule_logs(adapter_payload),
            "original_samples": adapter_payload["original_samples"][:3],
            "transformed_samples": adapter_payload["transformed_samples"][:3],
        },
        "results": results,
        "data_path": data_path,
    }


def main() -> int:
    args = parse_args()
    mode = str(args.mode).upper()
    with Path(args.contract).open("r", encoding="utf-8") as handle:
        contract = yaml.safe_load(handle)
    raw_records = load_jsonl(args.data)
    evaluation = evaluate_contract_records(
        contract,
        raw_records,
        mode=mode,
        data_path=args.data,
        attempt_adapter=True,
        persist_baselines=True,
    )
    report = build_validation_report(contract, evaluation, data_path=args.data, snapshot_id=sha256_file(args.data))
    output_path = Path(args.output) if args.output else default_output_path(str(contract.get("contract_id", "contract")))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"mode": evaluation["mode"], "blocking": evaluation["blocking"], "summary": report["summary"]}, indent=2))
    return 2 if evaluation["blocking"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
