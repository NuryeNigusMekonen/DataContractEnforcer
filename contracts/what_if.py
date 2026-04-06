from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import sys
import uuid
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.adapter import SchemaAdapter
from contracts.common import ensure_parent_dir, load_jsonl, utc_now
from contracts.evolution import build_compatibility_report, contract_version, normalize_version
from contracts.lineage import load_latest_lineage_snapshot, resolve_contract_lineage
from contracts.runner import evaluate_contract_records


Record = dict[str, Any]
RANGE_PATTERN = re.compile(r"(?P<min>-?\d+(?:\.\d+)?)\s*-\s*(?P<max>-?\d+(?:\.\d+)?)")
TYPE_PATTERN = re.compile(r"\b(int|integer|float|number|string|boolean)\b", re.IGNORECASE)
MULTIPLY_PATTERN = re.compile(r"x\s*->\s*x\s*\*\s*(?P<factor>-?\d+(?:\.\d+)?)", re.IGNORECASE)
DIVIDE_PATTERN = re.compile(r"x\s*->\s*x\s*/\s*(?P<divisor>-?\d+(?:\.\d+)?)", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a read-only what-if contract simulation.")
    parser.add_argument("--contract", required=True, help="Path to the current generated contract.")
    parser.add_argument("--data", required=True, help="Path to the current JSONL dataset snapshot.")
    parser.add_argument("--change-spec", required=True, help="Path to the proposed change spec (JSON or YAML).")
    parser.add_argument("--output", required=True, help="Path for the machine-readable what-if report.")
    parser.add_argument("--adapter-config", default="", help="Optional adapter rule config.")
    parser.add_argument("--subscriber", default="", help="Optional direct subscriber filter.")
    parser.add_argument(
        "--lineage",
        default="outputs/week4/lineage_snapshots.jsonl",
        help="Optional lineage snapshot export used only for transitive enrichment.",
    )
    parser.add_argument(
        "--registry",
        default="contract_registry/subscriptions.yaml",
        help="Contract registry used as the authoritative direct blast-radius source.",
    )
    return parser.parse_args()


def load_contract(path: str | Path) -> dict[str, Any]:
    contract_path = Path(path)
    if not contract_path.exists():
        raise FileNotFoundError(f"contract not found: {contract_path}")
    return yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}


def load_change_spec(path: str | Path) -> dict[str, Any]:
    spec_path = Path(path)
    if not spec_path.exists():
        raise FileNotFoundError(f"change spec not found: {spec_path}")
    if spec_path.suffix.lower() == ".json":
        return json.loads(spec_path.read_text(encoding="utf-8"))
    return yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}


def _iter_slots(value: Any, parts: list[str]) -> list[tuple[dict[str, Any], str]]:
    if not parts:
        return []
    if len(parts) == 1:
        if isinstance(value, dict) and parts[0] in value:
            return [(value, parts[0])]
        if isinstance(value, list):
            slots: list[tuple[dict[str, Any], str]] = []
            for item in value:
                slots.extend(_iter_slots(item, parts))
            return slots
        return []
    head, tail = parts[0], parts[1:]
    slots: list[tuple[dict[str, Any], str]] = []
    if isinstance(value, dict) and head in value:
        slots.extend(_iter_slots(value[head], tail))
    elif isinstance(value, list):
        for item in value:
            slots.extend(_iter_slots(item, parts))
    return slots


def _rename_path(record: Record, from_path: str, to_path: str) -> int:
    source_parts = from_path.split(".")
    target_parts = to_path.split(".")
    if source_parts[:-1] != target_parts[:-1]:
        return 0
    renamed = 0
    for container, key in _iter_slots(record, source_parts):
        target_key = target_parts[-1]
        if target_key in container:
            continue
        container[target_key] = container.pop(key)
        renamed += 1
    return renamed


def _remove_path(record: Record, path: str) -> int:
    removed = 0
    for container, key in _iter_slots(record, path.split(".")):
        if key in container:
            container.pop(key)
            removed += 1
    return removed


def _set_default(record: Record, path: str, value: Any) -> int:
    parts = path.split(".")
    current: Any = record
    for part in parts[:-1]:
        if isinstance(current, dict):
            current = current.setdefault(part, {})
            continue
        return 0
    if not isinstance(current, dict):
        return 0
    leaf = parts[-1]
    if leaf in current:
        return 0
    current[leaf] = deepcopy(value)
    return 1


def _set_path(record: Record, path: str, value: Any) -> int:
    parts = path.split(".")
    current: Any = record
    for part in parts[:-1]:
        if isinstance(current, dict):
            current = current.setdefault(part, {})
            continue
        return 0
    if not isinstance(current, dict):
        return 0
    current[parts[-1]] = deepcopy(value)
    return 1


def _scale_path(record: Record, path: str, factor: float) -> int:
    changed = 0
    for container, key in _iter_slots(record, path.split(".")):
        value = container.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        container[key] = round(float(value) * factor, 6)
        changed += 1
    return changed


def _coerce_scalar(value: Any, target_type: str) -> Any:
    normalized = str(target_type).lower()
    if normalized in {"int", "integer"}:
        return int(float(value))
    if normalized in {"float", "number"}:
        return float(value)
    if normalized == "string":
        return str(value)
    if normalized == "boolean":
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes"}
        return bool(value)
    return value


def _coerce_path(record: Record, path: str, target_type: str) -> int:
    changed = 0
    for container, key in _iter_slots(record, path.split(".")):
        value = container.get(key)
        if value is None:
            continue
        try:
            coerced = _coerce_scalar(value, target_type)
        except (TypeError, ValueError):
            continue
        if coerced != value:
            container[key] = coerced
            changed += 1
    return changed


def _replace_enum_values(record: Record, path: str, replace_values: set[str], new_value: Any) -> int:
    changed = 0
    for container, key in _iter_slots(record, path.split(".")):
        value = container.get(key)
        if str(value) in replace_values:
            container[key] = deepcopy(new_value)
            changed += 1
    return changed


def _parse_descriptor(descriptor: Any) -> dict[str, Any]:
    if isinstance(descriptor, dict):
        return deepcopy(descriptor)
    text = str(descriptor or "")
    payload: dict[str, Any] = {}
    range_match = RANGE_PATTERN.search(text)
    if range_match:
        payload["minimum"] = float(range_match.group("min"))
        payload["maximum"] = float(range_match.group("max"))
    type_match = TYPE_PATTERN.search(text)
    if type_match:
        token = type_match.group(1).lower()
        payload["type"] = "integer" if token in {"int", "integer"} else "number" if token in {"float", "number"} else token
    return payload


def _scale_factor(change_spec: dict[str, Any]) -> float:
    if "factor" in change_spec:
        return float(change_spec["factor"])
    transform = str(change_spec.get("sample_transform", "")).strip()
    multiply = MULTIPLY_PATTERN.search(transform)
    if multiply:
        return float(multiply.group("factor"))
    divide = DIVIDE_PATTERN.search(transform)
    if divide:
        divisor = float(divide.group("divisor"))
        return 1.0 / divisor if divisor else 1.0
    return 1.0


def _change_field(change_spec: dict[str, Any]) -> str:
    return str(change_spec.get("field") or change_spec.get("from_field") or "")


def _target_field(change_spec: dict[str, Any]) -> str:
    return str(change_spec.get("to_field") or change_spec.get("new_field") or change_spec.get("target_field") or "")


def _should_mutate_records(change_spec: dict[str, Any]) -> bool:
    if "apply_to_records" in change_spec:
        return bool(change_spec.get("apply_to_records"))
    change_type = str(change_spec.get("change_type", "")).lower()
    if change_type in {"numeric_scale_change", "field_rename", "remove_field", "type_change"}:
        return True
    if change_type == "add_field":
        return any(key in change_spec for key in {"default_value", "value", "field_value"})
    if change_type == "enum_change":
        return bool(change_spec.get("replace_values")) and "sample_new_value" in change_spec
    return False


def apply_hypothetical_change(records: list[Record], change_spec: dict[str, Any]) -> dict[str, Any]:
    """Apply an in-memory payload mutation for the proposed change."""
    changed_records = deepcopy(records)
    notes: list[str] = []
    operations = 0
    change_type = str(change_spec.get("change_type", "")).lower()
    field = _change_field(change_spec)
    target_field = _target_field(change_spec)

    if not _should_mutate_records(change_spec):
        notes.append("Change spec is schema-only for payload replay; records were left unchanged.")
        return {"records": changed_records, "operations": 0, "notes": notes}

    if change_type == "field_rename" and field and target_field:
        for record in changed_records:
            operations += _rename_path(record, field, target_field)
        notes.append(f"Renamed payload field {field} -> {target_field}.")
    elif change_type == "numeric_scale_change" and field:
        factor = _scale_factor(change_spec)
        for record in changed_records:
            operations += _scale_path(record, field, factor)
        notes.append(f"Scaled {field} by factor {factor}.")
    elif change_type == "type_change" and field:
        descriptor = _parse_descriptor(change_spec.get("to_type") or change_spec.get("to"))
        target_type = str(descriptor.get("type") or change_spec.get("target_type") or "")
        if target_type:
            for record in changed_records:
                operations += _coerce_path(record, field, target_type)
            notes.append(f"Coerced {field} to {target_type}.")
    elif change_type == "add_field" and field:
        value = deepcopy(change_spec.get("default_value", change_spec.get("value", change_spec.get("field_value"))))
        for record in changed_records:
            operations += _set_default(record, field, value)
        notes.append(f"Injected default value for {field}.")
    elif change_type == "remove_field" and field:
        for record in changed_records:
            operations += _remove_path(record, field)
        notes.append(f"Removed {field} from the simulated payload.")
    elif change_type == "enum_change" and field:
        replace_values = {str(value) for value in change_spec.get("replace_values", [])}
        new_value = change_spec.get("sample_new_value")
        if replace_values and new_value is not None:
            for record in changed_records:
                operations += _replace_enum_values(record, field, replace_values, new_value)
            notes.append(f"Replaced enum values {sorted(replace_values)} on {field} with {new_value}.")
    elif change_type == "range_change" and field:
        factor = _scale_factor(change_spec)
        if factor != 1.0:
            for record in changed_records:
                operations += _scale_path(record, field, factor)
            notes.append(f"Applied range-related scaling to {field}.")

    target_version = normalize_version(str(change_spec.get("target_version") or ""))
    if target_version and any(isinstance(record, dict) and "schema_version" in record for record in changed_records):
        for record in changed_records:
            if isinstance(record, dict) and "schema_version" in record:
                record["schema_version"] = target_version
    elif target_version and change_spec.get("inject_schema_version"):
        for record in changed_records:
            operations += _set_default(record, "schema_version", target_version)

    if operations == 0 and not notes:
        notes.append("No payload records changed; the simulation is contract-shape-only.")
    return {"records": changed_records, "operations": operations, "notes": notes}


def _field_schema(change_spec: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(change_spec.get("field_schema"), dict):
        return deepcopy(change_spec["field_schema"])
    clause = deepcopy(existing or {})
    from_descriptor = _parse_descriptor(change_spec.get("from"))
    to_descriptor = _parse_descriptor(change_spec.get("to"))
    if "type" not in clause and "type" in from_descriptor:
        clause["type"] = from_descriptor["type"]
    clause.update(to_descriptor)
    if "required" in change_spec:
        clause["required"] = bool(change_spec.get("required"))
    if "enum_values" in change_spec:
        clause["enum"] = list(change_spec["enum_values"])
    if "to_values" in change_spec:
        clause["enum"] = list(change_spec["to_values"])
    if "added_values" in change_spec:
        existing_enum = clause.get("enum", [])
        clause["enum"] = list(dict.fromkeys([*existing_enum, *change_spec["added_values"]]))
    if "removed_values" in change_spec:
        removed = {json.dumps(value, sort_keys=True) for value in change_spec["removed_values"]}
        clause["enum"] = [
            value for value in clause.get("enum", [])
            if json.dumps(value, sort_keys=True) not in removed
        ]
    if "minimum" in change_spec:
        clause["minimum"] = change_spec["minimum"]
    if "maximum" in change_spec:
        clause["maximum"] = change_spec["maximum"]
    if "target_type" in change_spec:
        clause["type"] = change_spec["target_type"]
    return clause


def _rewrite_field_reference(rule: dict[str, Any], from_field: str, to_field: str) -> None:
    if rule.get("field") == from_field:
        rule["field"] = to_field
    if isinstance(rule.get("fields"), list):
        rule["fields"] = [to_field if field == from_field else field for field in rule["fields"]]


def apply_hypothetical_contract(contract: dict[str, Any], change_spec: dict[str, Any]) -> dict[str, Any]:
    """Build an in-memory contract view for the proposed producer change."""
    hypothetical = deepcopy(contract)
    hypothetical["fields"] = deepcopy(contract.get("fields", {}))
    hypothetical["schema"] = deepcopy(contract.get("schema", hypothetical["fields"]))
    field = _change_field(change_spec)
    target_field = _target_field(change_spec)
    change_type = str(change_spec.get("change_type", "")).lower()
    fields = hypothetical["fields"]
    schema = hypothetical["schema"]

    target_version = change_spec.get("target_version")
    if target_version:
        normalized = normalize_version(str(target_version))
        hypothetical["schema_version"] = normalized
        info = hypothetical.setdefault("info", {})
        if isinstance(info, dict):
            info["version"] = normalized

    if change_type == "field_rename" and field and target_field and field in fields:
        clause = fields.pop(field)
        fields[target_field] = clause
        if field in schema:
            schema[target_field] = schema.pop(field)
        for clause_entry in hypothetical.get("clauses", []):
            if isinstance(clause_entry, dict):
                _rewrite_field_reference(clause_entry.get("rule", {}), field, target_field)
        for cross_check in hypothetical.get("cross_checks", []):
            if isinstance(cross_check, dict) and cross_check.get("field") == field:
                cross_check["field"] = target_field
    elif change_type in {"numeric_scale_change", "range_change", "type_change"} and field:
        existing = fields.get(field, {})
        updated = _field_schema(change_spec, existing)
        if updated:
            fields[field] = updated
            schema[field] = deepcopy(updated)
    elif change_type == "add_field" and field:
        fields[field] = _field_schema(change_spec, fields.get(field))
        schema[field] = deepcopy(fields[field])
    elif change_type == "remove_field" and field:
        fields.pop(field, None)
        schema.pop(field, None)
        hypothetical["clauses"] = [
            clause for clause in hypothetical.get("clauses", [])
            if not isinstance(clause, dict) or clause.get("rule", {}).get("field") != field
        ]
        hypothetical["cross_checks"] = [
            check for check in hypothetical.get("cross_checks", [])
            if not isinstance(check, dict) or check.get("field") != field
        ]
    elif change_type == "enum_change" and field:
        existing = fields.get(field, {})
        updated = _field_schema(change_spec, existing)
        fields[field] = updated
        schema[field] = deepcopy(updated)

    return hypothetical


def load_adapter_rules(path: str | None, contract_id: str) -> list[dict[str, Any]]:
    if not path:
        return []
    config_path = Path(path)
    if not config_path.exists():
        return []
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    adapters = payload.get("adapters", payload)
    if not isinstance(adapters, dict):
        return []
    contract_rules = adapters.get(contract_id, [])
    if isinstance(contract_rules, list):
        return [deepcopy(rule) for rule in contract_rules if isinstance(rule, dict)]
    if not isinstance(contract_rules, dict):
        return []
    flattened: list[dict[str, Any]] = []
    for version_pair, rules in contract_rules.items():
        if not isinstance(rules, list):
            continue
        source_version, _, target_version = str(version_pair).partition("->")
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            payload_rule = deepcopy(rule)
            payload_rule.setdefault("source_version", normalize_version(source_version))
            payload_rule.setdefault("target_version", normalize_version(target_version))
            flattened.append(payload_rule)
    return flattened


def infer_auto_adapter_rules(
    contract: dict[str, Any],
    compatibility: dict[str, Any],
    change_spec: dict[str, Any],
) -> list[dict[str, Any]]:
    source_version = normalize_version(str(change_spec.get("target_version") or change_spec.get("source_version") or "2.0.0"))
    target_version = contract_version(contract)
    change_type = str(change_spec.get("change_type", "")).lower()
    field = _change_field(change_spec)
    target_field = _target_field(change_spec)
    rules: list[dict[str, Any]] = []

    if normalize_version(source_version) != normalize_version(target_version):
        rules.append(
            {
                "source_version": source_version,
                "target_version": target_version,
                "type": "version_normalize",
                "field": "schema_version",
                "segments": 2,
                "description": "Normalize payload schema_version token from semantic (x.y.z) to contract style (x.y).",
            }
        )

    if change_type == "field_rename" and field and target_field:
        rules.append(
            {
                "source_version": source_version,
                "target_version": target_version,
                "type": "field_rename",
                "from": target_field,
                "to": field,
                "description": f"Reverse producer rename {field} -> {target_field} for current consumers.",
            }
        )

    if change_type in {"numeric_scale_change", "range_change"} and field:
        factor = _scale_factor(change_spec)
        if factor not in {0.0, 1.0}:
            rules.append(
                {
                    "source_version": source_version,
                    "target_version": target_version,
                    "type": "numeric_scaling",
                    "field": field,
                    "factor": 1.0 / float(factor),
                    "description": "Reverse numeric scale shift into the currently deployed unit range.",
                }
            )

    if change_type == "type_change" and field:
        existing = contract.get("fields", {}).get(field, {})
        if isinstance(existing, dict):
            target_type = str(existing.get("type", "")).strip()
            if target_type:
                rules.append(
                    {
                        "source_version": source_version,
                        "target_version": target_version,
                        "type": "type_coercion",
                        "field": field,
                        "target_type": target_type,
                        "description": f"Coerce {field} back to {target_type} for current consumer compatibility.",
                    }
                )

    if change_type == "add_field" and field:
        rules.append(
            {
                "source_version": source_version,
                "target_version": target_version,
                "type": "remove_field",
                "field": field,
                "description": f"Strip newly added field {field} for down-level consumers.",
            }
        )

    if change_type == "remove_field" and field:
        existing = contract.get("fields", {}).get(field, {})
        if isinstance(existing, dict):
            default_value = change_spec.get("default_value")
            if default_value is None:
                enum_values = existing.get("enum")
                if isinstance(enum_values, list) and enum_values:
                    default_value = enum_values[0]
            if default_value is not None:
                rules.append(
                    {
                        "source_version": source_version,
                        "target_version": target_version,
                        "type": "default_value",
                        "field": field,
                        "value": default_value,
                        "description": f"Backfill removed field {field} using a safe default for current consumers.",
                    }
                )

    if change_type == "enum_change" and field:
        added_values = [str(value) for value in change_spec.get("added_values", [])]
        current = contract.get("fields", {}).get(field, {})
        fallback_value = None
        if isinstance(current, dict):
            enum_values = current.get("enum")
            if isinstance(enum_values, list) and enum_values:
                fallback_value = enum_values[0]
        if added_values and fallback_value is not None:
            rules.append(
                {
                    "source_version": source_version,
                    "target_version": target_version,
                    "type": "enum_replace",
                    "field": field,
                    "replace_values": added_values,
                    "new_value": fallback_value,
                    "description": f"Map newly added enum values for {field} back to an existing consumer-safe value.",
                }
            )

    renames = compatibility.get("renames", [])
    if isinstance(renames, list):
        for rename in renames:
            if not isinstance(rename, dict):
                continue
            from_field = str(rename.get("from", ""))
            to_field = str(rename.get("to", ""))
            if not from_field or not to_field:
                continue
            if change_type == "field_rename" and field == from_field and target_field == to_field:
                continue
            rules.append(
                {
                    "source_version": source_version,
                    "target_version": target_version,
                    "type": "field_rename",
                    "from": to_field,
                    "to": from_field,
                    "description": f"Auto-detected rename rollback for {to_field} -> {from_field}.",
                }
            )

    return rules


def run_baseline_validation(contract: dict[str, Any], records: list[Record], data_path: str | Path) -> dict[str, Any]:
    return evaluate_contract_records(
        contract,
        records,
        mode="ENFORCE",
        data_path=str(data_path),
        attempt_adapter=False,
        persist_baselines=False,
    )


def run_changed_validation(contract: dict[str, Any], changed_records: list[Record], data_path: str | Path) -> dict[str, Any]:
    return evaluate_contract_records(
        contract,
        changed_records,
        mode="ENFORCE",
        data_path=f"{data_path}#what-if-raw",
        attempt_adapter=False,
        persist_baselines=False,
    )


def run_adapter_validation(
    contract: dict[str, Any],
    changed_records: list[Record],
    change_spec: dict[str, Any],
    adapter_rules: list[dict[str, Any]] | None = None,
    allow_noop_success: bool = False,
) -> dict[str, Any]:
    contract_id = str(contract.get("contract_id", ""))
    source_version = normalize_version(str(change_spec.get("target_version") or change_spec.get("source_version") or "2.0.0"))
    target_version = contract_version(contract)
    adapter = SchemaAdapter(contract_id, extra_rules=adapter_rules or [])
    payload = adapter.transform_records(changed_records, source_version, target_version)
    if not payload["succeeded"]:
        return {
            "adapter": {
                "attempted": payload["attempted"],
                "applied": payload["applied"],
                "succeeded": payload["succeeded"],
                "fallback_succeeded": False,
                "source_version": payload["source_version"],
                "target_version": payload["target_version"],
                "failure_reason": payload["failure_reason"],
                "rules_applied": adapter.summarize_rule_logs(payload),
                "original_samples": payload.get("original_samples", [])[:3],
                "transformed_samples": payload.get("transformed_samples", [])[:3],
            },
            "evaluation": {
                "overall_status": "FAIL",
                "record_count": len(changed_records),
                "raw_record_count": len(changed_records),
                "summary": {"PASS": 0, "WARN": 0, "FAIL": 1, "ERROR": 0},
                "results": [],
            },
        }
    if not payload["applied"] and not allow_noop_success:
        return {
            "adapter": {
                "attempted": payload["attempted"],
                "applied": payload["applied"],
                "succeeded": payload["succeeded"],
                "fallback_succeeded": False,
                "source_version": payload["source_version"],
                "target_version": payload["target_version"],
                "failure_reason": payload["failure_reason"],
                "rules_applied": adapter.summarize_rule_logs(payload),
                "original_samples": payload.get("original_samples", [])[:3],
                "transformed_samples": payload.get("transformed_samples", [])[:3],
            },
            "evaluation": {
                "overall_status": "FAIL",
                "record_count": len(changed_records),
                "raw_record_count": len(changed_records),
                "summary": {"PASS": 0, "WARN": 0, "FAIL": 1, "ERROR": 0},
                "results": [],
            },
        }
    evaluation = evaluate_contract_records(
        contract,
        payload["records"],
        mode="ENFORCE",
        data_path=f"{contract.get('source_path', '')}#what-if-adapted",
        attempt_adapter=False,
        persist_baselines=False,
    )
    return {
        "adapter": {
            "attempted": payload["attempted"],
            "applied": payload["applied"],
            "succeeded": payload["succeeded"],
            "fallback_succeeded": evaluation["overall_status"] == "PASS" if payload["succeeded"] else False,
            "source_version": payload["source_version"],
            "target_version": payload["target_version"],
            "failure_reason": payload["failure_reason"],
            "rules_applied": adapter.summarize_rule_logs(payload),
            "original_samples": payload.get("original_samples", [])[:3],
            "transformed_samples": payload.get("transformed_samples", [])[:3],
        },
        "evaluation": evaluation,
    }


def _field_path_matches(candidate: str, changed_field: str) -> bool:
    left = str(candidate or "").strip()
    right = str(changed_field or "").strip()
    if not left or not right:
        return False
    if left == right:
        return True
    return right.startswith(f"{left}.") or left.startswith(f"{right}.")


def compute_registry_blast_radius(
    contract_id: str,
    changed_fields: list[str],
    *,
    registry_path: str | None,
    subscriber_filter: str = "",
    adapter_recoverable: bool = False,
) -> list[dict[str, Any]]:
    if not registry_path or not Path(registry_path).exists():
        return []
    payload = yaml.safe_load(Path(registry_path).read_text(encoding="utf-8")) or {}
    subscriptions = payload.get("subscriptions", [])
    if not isinstance(subscriptions, list):
        return []
    changed = set(changed_fields)
    subscribers: list[dict[str, Any]] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        if str(subscription.get("contract_id", "")) != contract_id:
            continue
        subscriber_id = str(subscription.get("subscriber_id", ""))
        if subscriber_filter and subscriber_id != subscriber_filter:
            continue
        fields_consumed = [str(field) for field in subscription.get("fields_consumed", [])]
        breaking_fields: list[str] = []
        for item in subscription.get("breaking_fields", []):
            if isinstance(item, dict):
                breaking_fields.append(str(item.get("field", "")))
            else:
                breaking_fields.append(str(item))
        impacted_breaking = [
            field for field in breaking_fields
            if any(_field_path_matches(field, changed_field) for changed_field in changed)
        ]
        impacted_consumed = [
            field for field in fields_consumed
            if any(_field_path_matches(field, changed_field) for changed_field in changed)
        ]
        if changed and not impacted_breaking and not impacted_consumed:
            continue
        subscribers.append(
            {
                "subscriber_id": subscriber_id,
                "fields_consumed": fields_consumed,
                "breaking_fields": impacted_breaking or impacted_consumed or breaking_fields,
                "contamination_depth": 0,
                "recoverable_via_adapter": adapter_recoverable,
                "validation_mode": str(subscription.get("validation_mode", "AUDIT")),
                "contact": str(subscription.get("contact", "unknown")),
            }
        )
    return subscribers


def enrich_with_lineage(
    blast_radius: list[dict[str, Any]],
    contract: dict[str, Any],
    *,
    lineage_path: str | None,
    registry_path: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    lineage_snapshot = load_latest_lineage_snapshot(lineage_path)
    resolved = resolve_contract_lineage(contract, lineage_snapshot, registry_path)
    downstream = resolved.get("downstream", [])
    direct_index = {entry["subscriber_id"]: index for index, entry in enumerate(blast_radius)}
    transitive: list[dict[str, Any]] = []
    for entry in downstream:
        node_id = str(entry.get("id", ""))
        contamination_depth = max(int(entry.get("hops", 0)) - 1, 0)
        if node_id in direct_index:
            blast_radius[direct_index[node_id]]["contamination_depth"] = contamination_depth
            blast_radius[direct_index[node_id]]["contamination_path"] = list(entry.get("via", []))
            continue
        transitive.append(
            {
                "id": node_id,
                "kind": entry.get("kind", "UNKNOWN"),
                "relationship": entry.get("relationship"),
                "contamination_depth": contamination_depth,
                "via": list(entry.get("via", [])),
                "relationship_path": list(entry.get("relationship_path", [])),
                "source": entry.get("source", "contract_graph"),
            }
        )
    return blast_radius, transitive


def summarize_validation(evaluation: dict[str, Any]) -> dict[str, Any]:
    results = evaluation.get("results", [])
    failures = [result for result in results if result.get("status") in {"FAIL", "ERROR"}]
    critical_failures = [result for result in failures if str(result.get("severity", "")).upper() == "CRITICAL"]
    if not results and any(key in evaluation for key in ("failed", "errored")):
        failed_count = int(evaluation.get("failed", 0) or 0) + int(evaluation.get("errored", 0) or 0)
        critical_count = int(evaluation.get("errored", 0) or 0)
        return {
            "status": evaluation.get("overall_status", "FAIL"),
            "failed_checks": failed_count,
            "critical_failures": critical_count,
            "summary": evaluation.get("summary", {}),
            "blocking": bool(evaluation.get("blocking", False)),
        }
    return {
        "status": evaluation.get("overall_status", "FAIL"),
        "failed_checks": len(failures),
        "critical_failures": len(critical_failures),
        "summary": evaluation.get("summary", {}),
        "blocking": bool(evaluation.get("blocking", False)),
    }


def changed_fields_from_report(compatibility: dict[str, Any]) -> list[str]:
    return [
        str(change.get("field_name", ""))
        for change in compatibility.get("changes", [])
        if change.get("change_type") != "NO_CHANGE"
    ]


def classify_what_if_result(
    baseline: dict[str, Any],
    raw_changed: dict[str, Any],
    adapted: dict[str, Any] | None,
    compatibility: dict[str, Any],
) -> str:
    compatibility_verdict = str(compatibility.get("compatibility_verdict", ""))
    adapter_recovered = bool(
        adapted
        and adapted.get("evaluation", {}).get("overall_status") == "PASS"
        and (
            adapted.get("adapter", {}).get("applied")
            or adapted.get("adapter", {}).get("fallback_succeeded")
        )
    )
    if baseline.get("overall_status") != "PASS":
        return "BREAKING"
    if adapter_recovered:
        if compatibility_verdict == "breaking_change":
            return "BREAKING_BUT_ADAPTABLE"
    if raw_changed.get("overall_status") == "PASS":
        verdict = compatibility_verdict or "backward_compatible"
        change_count = len([item for item in compatibility.get("changes", []) if item.get("change_type") != "NO_CHANGE"])
        if verdict == "forward_compatible":
            return "FORWARD_COMPATIBLE"
        if verdict == "backward_compatible":
            return "COMPATIBLE" if change_count == 0 else "BACKWARD_COMPATIBLE"
        return "BREAKING_REQUIRES_MIGRATION"
    if adapter_recovered:
        return "BREAKING_BUT_ADAPTABLE"
    if compatibility_verdict == "breaking_change":
        return "BREAKING_REQUIRES_MIGRATION"
    return "BREAKING"


def generate_recommendation(verdict: str, affected_subscribers: list[dict[str, Any]], adapted: dict[str, Any] | None) -> str:
    if verdict == "BREAKING_BUT_ADAPTABLE":
        return "Deploy only with adapter, notify subscribers, refresh statistical baselines"
    if verdict in {"FORWARD_COMPATIBLE", "BACKWARD_COMPATIBLE", "COMPATIBLE"}:
        return "Proceed with subscriber notice, monitor validation drift, and schedule a controlled contract version bump"
    if verdict == "BREAKING_REQUIRES_MIGRATION":
        if affected_subscribers:
            return "Do not promote the contract change until affected subscribers approve a migration plan."
        return "Do not promote the contract change until a migration plan is approved."
    if adapted and adapted.get("adapter", {}).get("attempted"):
        return "Adapter recovery failed; keep the current contract live and coordinate a consumer migration"
    return "Reject the proposed change or revise it into a backward-compatible form"


def simulate_what_if(
    *,
    contract_path: str | Path,
    data_path: str | Path,
    change_spec_path: str | Path,
    adapter_config: str | None = None,
    subscriber_filter: str = "",
    lineage_path: str | None = "outputs/week4/lineage_snapshots.jsonl",
    registry_path: str | None = "contract_registry/subscriptions.yaml",
    baseline_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    contract = load_contract(contract_path)
    current_records = load_jsonl(data_path)
    change_spec = load_change_spec(change_spec_path)

    baseline = baseline_evaluation or run_baseline_validation(contract, current_records, data_path)
    changed_payload = apply_hypothetical_change(current_records, change_spec)
    hypothetical_contract = apply_hypothetical_contract(contract, change_spec)
    compatibility = build_compatibility_report(contract, hypothetical_contract, registry_path)
    raw_changed = run_changed_validation(contract, changed_payload["records"], data_path)

    adapted: dict[str, Any] | None = None
    compatibility_breaking = str(compatibility.get("compatibility_verdict", "")) == "breaking_change"
    configured_rules = load_adapter_rules(adapter_config, str(contract.get("contract_id", ""))) if adapter_config else []
    auto_rules = infer_auto_adapter_rules(contract, compatibility, change_spec) if compatibility_breaking else []
    adapter_rules = [*configured_rules, *auto_rules]
    adapter_needed = raw_changed.get("overall_status") != "PASS" or compatibility_breaking or bool(configured_rules)
    if adapter_needed:
        adapted = run_adapter_validation(
            contract,
            changed_payload["records"],
            change_spec,
            adapter_rules,
            allow_noop_success=bool(compatibility_breaking and raw_changed.get("overall_status") == "PASS"),
        )

    verdict = classify_what_if_result(baseline, raw_changed, adapted, compatibility)
    changed_fields = changed_fields_from_report(compatibility)
    adapter_recoverable = verdict == "BREAKING_BUT_ADAPTABLE"
    affected_subscribers = compute_registry_blast_radius(
        str(contract.get("contract_id", "")),
        changed_fields,
        registry_path=registry_path,
        subscriber_filter=subscriber_filter,
        adapter_recoverable=adapter_recoverable,
    )
    affected_subscribers, transitive_impacts = enrich_with_lineage(
        affected_subscribers,
        contract,
        lineage_path=lineage_path,
        registry_path=registry_path,
    )

    notes = [
        *changed_payload["notes"],
        f"Compatibility diff classified the proposed change as {compatibility.get('compatibility_verdict', 'unknown')}.",
    ]
    primary_breaking = compatibility.get("primary_breaking_change")
    if primary_breaking:
        notes.append(str(primary_breaking.get("rationale", "Primary breaking change detected.")))
    if (
        adapted
        and adapted.get("evaluation", {}).get("overall_status") == "PASS"
        and adapted.get("adapter", {}).get("applied")
    ):
        notes.append("Adapter rules restored the payload to the currently expected contract shape.")
    elif (
        adapted
        and adapted.get("evaluation", {}).get("overall_status") == "PASS"
        and adapted.get("adapter", {}).get("fallback_succeeded")
    ):
        notes.append("Adapter rollback rules are available for rollout even though this sample did not require payload rewrites.")
    elif adapted and adapted.get("adapter", {}).get("attempted"):
        notes.append("Adapter was attempted but could not recover compatibility for the simulated payload.")
    if auto_rules:
        notes.append(f"Auto-generated {len(auto_rules)} safe adapter rule(s) from the compatibility diff.")

    return {
        "simulation_id": str(uuid.uuid4()),
        "contract_id": str(contract.get("contract_id", "")),
        "run_timestamp": utc_now(),
        "proposed_change": {
            "field": _change_field(change_spec),
            "change_type": str(change_spec.get("change_type", "")),
            "from": change_spec.get("from"),
            "to": change_spec.get("to"),
            "target_version": normalize_version(str(change_spec.get("target_version") or hypothetical_contract.get("schema_version") or contract_version(contract))),
        },
        "baseline_status": baseline.get("overall_status", "FAIL"),
        "baseline_summary": summarize_validation(baseline),
        "raw_changed_status": raw_changed.get("overall_status", "FAIL"),
        "raw_changed_summary": summarize_validation(raw_changed),
        "adapter_attempted": bool(adapted and adapted.get("adapter", {}).get("attempted")),
        "adapter_status": None if not adapted else adapted.get("evaluation", {}).get("overall_status", "FAIL"),
        "adapter_details": None if not adapted else adapted.get("adapter"),
        "compatibility_verdict": verdict,
        "compatibility_report": compatibility,
        "affected_subscribers": affected_subscribers,
        "transitive_impacts": transitive_impacts,
        "recommended_action": generate_recommendation(verdict, affected_subscribers, adapted),
        "notes": notes,
        "current_state": {
            "contract_version": contract_version(contract),
            "status": baseline.get("overall_status", "FAIL"),
            "record_count": baseline.get("record_count", 0),
        },
        "hypothetical_changed_state": {
            "contract_version": hypothetical_contract.get("schema_version", contract_version(hypothetical_contract)),
            "status": raw_changed.get("overall_status", "FAIL"),
            "record_count": raw_changed.get("record_count", 0),
            "payload_operations": changed_payload.get("operations", 0),
        },
        "hypothetical_changed_state_plus_adapter": None
        if not adapted
        else {
            "status": adapted.get("evaluation", {}).get("overall_status", "FAIL"),
            "record_count": adapted.get("evaluation", {}).get("record_count", 0),
            "adapter_applied": adapted.get("adapter", {}).get("applied", False),
        },
    }


def structured_error(code: str, message: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "simulation_id": str(uuid.uuid4()),
        "run_timestamp": utc_now(),
        "status": "ERROR",
        "error_code": code,
        "message": message,
        "details": details or {},
    }


def main() -> int:
    args = parse_args()
    try:
        report = simulate_what_if(
            contract_path=args.contract,
            data_path=args.data,
            change_spec_path=args.change_spec,
            adapter_config=args.adapter_config or None,
            subscriber_filter=args.subscriber,
            lineage_path=args.lineage or None,
            registry_path=args.registry or None,
        )
        output_path = Path(args.output)
        ensure_parent_dir(output_path)
        output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {
                    "simulation_id": report["simulation_id"],
                    "contract_id": report["contract_id"],
                    "compatibility_verdict": report["compatibility_verdict"],
                    "output": str(output_path),
                },
                indent=2,
            )
        )
        return 0
    except FileNotFoundError as exc:
        payload = structured_error("missing_file", str(exc))
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        payload = structured_error("invalid_spec", f"Unable to parse input file: {exc}")
    except Exception as exc:  # pragma: no cover - defensive CLI guard
        payload = structured_error("what_if_failed", str(exc))

    output_path = Path(args.output)
    ensure_parent_dir(output_path)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"status": "ERROR", "output": str(output_path), "error": payload["message"]}, indent=2))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
