from __future__ import annotations

from copy import deepcopy
import json
from typing import Any, Callable

from contracts.evolution import normalize_version


Record = dict[str, Any]


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


def _path_present(record: Record, path: str) -> bool:
    return bool(_iter_slots(record, path.split(".")))


def _rename_path_preserving_context(record: Record, from_path: str, to_path: str) -> int:
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


def _scale_numeric(record: Record, path: str, factor: float, predicate: Callable[[float], bool] | None = None) -> int:
    changed = 0
    for container, key in _iter_slots(record, path.split(".")):
        value = container.get(key)
        if not isinstance(value, (int, float)):
            continue
        numeric = float(value)
        if predicate and not predicate(numeric):
            continue
        container[key] = round(numeric * factor, 6)
        changed += 1
    return changed


def _inject_default(record: Record, path: str, value: Any) -> int:
    parts = path.split(".")
    current: Any = record
    for part in parts[:-1]:
        if isinstance(current, dict):
            current = current.setdefault(part, {})
        else:
            return 0
    if not isinstance(current, dict):
        return 0
    leaf = parts[-1]
    if leaf in current:
        return 0
    current[leaf] = deepcopy(value)
    return 1


class SchemaAdapter:
    def __init__(self, contract_id: str, extra_rules: list[dict[str, Any]] | None = None):
        self.contract_id = contract_id
        self.extra_rules = deepcopy(extra_rules or [])

    def detect_source_version(self, records: list[Record], target_version: str) -> dict[str, Any]:
        explicit_versions = [
            normalize_version(str(record.get("schema_version")))
            for record in records
            if isinstance(record, dict) and record.get("schema_version") is not None
        ]
        if explicit_versions:
            majority = max(set(explicit_versions), key=explicit_versions.count)
            return {
                "original_schema_version": majority,
                "detected_schema_version": majority,
                "detection_method": "payload_schema_version",
                "reason": "source records declare schema_version",
            }

        if self.contract_id == "week3-document-refinery-extractions":
            for record in records:
                for fact in record.get("extracted_facts", []):
                    confidence = fact.get("confidence")
                    if isinstance(confidence, (int, float)) and float(confidence) > 1.0:
                        return {
                            "original_schema_version": "unknown",
                            "detected_schema_version": "2.0.0",
                            "detection_method": "heuristic_confidence_scale",
                            "reason": "confidence values appear to be on a 0-100 scale",
                        }

        return {
            "original_schema_version": "unknown",
            "detected_schema_version": normalize_version(target_version),
            "detection_method": "default_expected_version",
            "reason": "no version mismatch indicators were found",
        }

    def _rules_for(self, source_version: str, target_version: str) -> list[dict[str, Any]]:
        source = normalize_version(source_version)
        target = normalize_version(target_version)
        builtin_rules = {
            "week3-document-refinery-extractions": {
                ("2.0.0", "1.0.0"): [
                    {
                        "type": "numeric_scaling",
                        "field": "extracted_facts.confidence",
                        "factor": 0.01,
                        "predicate": lambda value: value > 1.0,
                        "description": "Convert percentage confidence back to a 0.0-1.0 scale.",
                    }
                ]
            },
            "week5-event-records": {
                ("0.9.0", "1.0.0"): [
                    {
                        "type": "field_rename",
                        "from": "event_name",
                        "to": "event_type",
                        "description": "Map legacy event_name to event_type.",
                    },
                    {
                        "type": "default_value",
                        "field": "schema_version",
                        "value": "1.0",
                        "description": "Inject the default event schema version for legacy events.",
                    },
                ]
            },
            "langsmith-trace-records": {
                ("0.9.0", "1.0.0"): [
                    {
                        "type": "default_value",
                        "field": "tags",
                        "value": [],
                        "description": "Populate empty tags for older trace payloads that omitted them.",
                    }
                ]
            },
        }
        rules = deepcopy(builtin_rules.get(self.contract_id, {}).get((source, target), []))
        seen = {
            json.dumps(
                {
                    key: value
                    for key, value in rule.items()
                    if key not in {"predicate", "source_version", "target_version"}
                },
                sort_keys=True,
                default=str,
            )
            for rule in rules
        }
        for rule in self.extra_rules:
            if not isinstance(rule, dict):
                continue
            if normalize_version(str(rule.get("source_version"))) != source:
                continue
            if normalize_version(str(rule.get("target_version"))) != target:
                continue
            signature = json.dumps(
                {
                    key: value
                    for key, value in rule.items()
                    if key not in {"predicate", "source_version", "target_version"}
                },
                sort_keys=True,
                default=str,
            )
            if signature in seen:
                continue
            seen.add(signature)
            rules.append(deepcopy(rule))
        return rules

    def transform(self, record: Record, source_version: str, target_version: str) -> tuple[Record, list[dict[str, Any]]]:
        transformed = deepcopy(record)
        operations: list[dict[str, Any]] = []
        for rule in self._rules_for(source_version, target_version):
            changed = 0
            if rule["type"] in {"field_rename", "alias"}:
                changed = _rename_path_preserving_context(transformed, rule["from"], rule["to"])
            elif rule["type"] == "numeric_scaling":
                changed = _scale_numeric(
                    transformed,
                    rule["field"],
                    float(rule["factor"]),
                    rule.get("predicate"),
                )
            elif rule["type"] == "default_value":
                changed = _inject_default(transformed, rule["field"], rule["value"])
            elif rule["type"] == "optional_field":
                changed = 1 if not _path_present(transformed, rule["field"]) else 0
            if changed:
                operations.append(
                    {
                        "rule_type": rule["type"],
                        "description": rule["description"],
                        "changed_values": changed,
                    }
                )
        return transformed, operations

    def transform_records(self, records: list[Record], source_version: str, target_version: str) -> dict[str, Any]:
        attempted = normalize_version(source_version) != normalize_version(target_version)
        transformed_records: list[Record] = []
        record_logs: list[dict[str, Any]] = []
        changed_samples: list[dict[str, Any]] = []
        rule_available = bool(self._rules_for(source_version, target_version))

        if attempted and not rule_available:
            return {
                "attempted": True,
                "applied": False,
                "succeeded": False,
                "fallback_succeeded": False,
                "source_version": normalize_version(source_version),
                "target_version": normalize_version(target_version),
                "failure_reason": f"no adapter rules available for {self.contract_id} {source_version} -> {target_version}",
                "rule_logs": [],
                "original_samples": [records[0]] if records else [],
                "transformed_samples": [],
                "records": records,
            }

        for index, record in enumerate(records):
            transformed, operations = self.transform(record, source_version, target_version)
            transformed_records.append(transformed)
            if operations:
                record_logs.append({"record_index": index, "operations": operations})
                if len(changed_samples) < 3:
                    changed_samples.append(
                        {
                            "record_index": index,
                            "original": record,
                            "transformed": transformed,
                        }
                    )

        applied = bool(record_logs)
        return {
            "attempted": attempted,
            "applied": applied,
            "succeeded": True if not attempted else rule_available,
            "fallback_succeeded": applied,
            "source_version": normalize_version(source_version),
            "target_version": normalize_version(target_version),
            "failure_reason": "",
            "rule_logs": record_logs,
            "original_samples": [sample["original"] for sample in changed_samples],
            "transformed_samples": [sample["transformed"] for sample in changed_samples],
            "records": transformed_records,
        }

    def summarize_rule_logs(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for item in payload.get("rule_logs", []):
            for operation in item.get("operations", []):
                summaries.append(
                    {
                        "record_index": item["record_index"],
                        "rule_type": operation["rule_type"],
                        "description": operation["description"],
                        "changed_values": operation["changed_values"],
                    }
                )
        return summaries

    def sample_as_json(self, payload: dict[str, Any], key: str) -> list[str]:
        return [json.dumps(value, sort_keys=True)[:500] for value in payload.get(key, [])[:3]]
