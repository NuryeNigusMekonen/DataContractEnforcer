from __future__ import annotations

from collections import Counter
from pathlib import Path
import difflib
import json
import re
from typing import Any

import yaml


TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
NUMERIC_TYPE_ORDER = {"integer": 0, "number": 1}


def normalize_version(version: str | None) -> str:
    text = str(version or "").strip()
    if not text:
        return "1.0.0"
    parts = text.split(".")
    while len(parts) < 3:
        parts.append("0")
    return ".".join(parts[:3])


def contract_version(contract: dict[str, Any]) -> str:
    explicit = str(contract.get("schema_version") or "")
    if explicit:
        return normalize_version(explicit)
    info = contract.get("info", {}) if isinstance(contract.get("info"), dict) else {}
    return normalize_version(str(info.get("version") or "1.0.0"))


def _tokenize(field_name: str) -> set[str]:
    return {token for token in TOKEN_PATTERN.findall(field_name.lower()) if len(token) >= 2}


def _leaf(field_name: str) -> str:
    return field_name.split(".")[-1]


def _enum_set(clause: dict[str, Any]) -> set[str]:
    return {json.dumps(value, sort_keys=True) for value in clause.get("enum", [])}


def _field_statistics(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    profiling = contract.get("profiling", {}) if isinstance(contract.get("profiling"), dict) else {}
    statistics = profiling.get("statistics", {}) if isinstance(profiling.get("statistics"), dict) else {}
    return statistics if isinstance(statistics, dict) else {}


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_numeric(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value.is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _range_text(minimum: Any, maximum: Any, stats: dict[str, Any] | None = None) -> str:
    stats = stats or {}
    observed_min = _float_or_none(stats.get("min"))
    observed_max = _float_or_none(stats.get("max"))
    if observed_min is not None and observed_max is not None:
        return f"{_format_numeric(observed_min)} to {_format_numeric(observed_max)}"
    low = _float_or_none(minimum)
    high = _float_or_none(maximum)
    if low is not None and high is not None:
        return f"{_format_numeric(low)} to {_format_numeric(high)}"
    if low is not None:
        return f">= {_format_numeric(low)}"
    if high is not None:
        return f"<= {_format_numeric(high)}"
    return "unknown"


def _numeric_type_rank(value: Any) -> int | None:
    return NUMERIC_TYPE_ORDER.get(str(value))


def _is_unit_interval(minimum: Any, maximum: Any, stats: dict[str, Any] | None = None) -> bool:
    low = _float_or_none(minimum)
    high = _float_or_none(maximum)
    stats = stats or {}
    observed_min = _float_or_none(stats.get("min"))
    observed_max = _float_or_none(stats.get("max"))
    lower_bound = observed_min if observed_min is not None else low
    upper_bound = observed_max if observed_max is not None else high
    return lower_bound is not None and upper_bound is not None and lower_bound >= 0.0 and upper_bound <= 1.0


def _looks_like_percentage_scale(minimum: Any, maximum: Any, stats: dict[str, Any] | None = None) -> bool:
    low = _float_or_none(minimum)
    high = _float_or_none(maximum)
    stats = stats or {}
    observed_max = _float_or_none(stats.get("max"))
    observed_mean = _float_or_none(stats.get("mean"))
    candidates = [value for value in [high, observed_max, observed_mean] if value is not None]
    return (low is None or low >= 0.0) and any(value > 1.0 for value in candidates)


def _rename_similarity(left: str, right: str) -> float:
    left_tokens = _tokenize(left)
    right_tokens = _tokenize(right)
    jaccard = 0.0
    if left_tokens or right_tokens:
        jaccard = len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)
    sequence = difflib.SequenceMatcher(None, left, right).ratio()
    leaf_bonus = 0.15 if _leaf(left) == _leaf(right) else 0.0
    return max(jaccard, sequence) + leaf_bonus


def _compatible_shape(left: dict[str, Any], right: dict[str, Any]) -> bool:
    comparable_keys = ["type", "format", "pattern"]
    if any(left.get(key) != right.get(key) for key in comparable_keys):
        return False
    left_enum = _enum_set(left)
    right_enum = _enum_set(right)
    if left_enum and right_enum and left_enum != right_enum:
        return False
    return True


def detect_renames(source_fields: dict[str, dict[str, Any]], target_fields: dict[str, dict[str, Any]]) -> dict[str, str]:
    removed = {field: clause for field, clause in source_fields.items() if field not in target_fields}
    added = {field: clause for field, clause in target_fields.items() if field not in source_fields}
    renames: dict[str, str] = {}
    used_targets: set[str] = set()
    for source_name, source_clause in removed.items():
        scored: list[tuple[float, str]] = []
        for target_name, target_clause in added.items():
            if target_name in used_targets or not _compatible_shape(source_clause, target_clause):
                continue
            score = _rename_similarity(source_name, target_name)
            if score >= 0.55:
                scored.append((score, target_name))
        if not scored:
            continue
        _, best_target = sorted(scored, key=lambda item: (-item[0], item[1]))[0]
        renames[source_name] = best_target
        used_targets.add(best_target)
    return renames


def classify_change(
    field_name: str,
    source_clause: dict[str, Any] | None,
    target_clause: dict[str, Any] | None,
    *,
    renamed_from: str | None = None,
    source_stats: dict[str, Any] | None = None,
    target_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if renamed_from:
        return {
            "field_name": field_name,
            "change_type": "FIELD_RENAME",
            "compatibility_class": "breaking_change",
            "severity": "HIGH",
            "backward_compatible": False,
            "forward_compatible": False,
            "rationale": f"{renamed_from} renamed to {field_name}",
        }
    if source_clause is None and target_clause is not None:
        required = bool(target_clause.get("required", False))
        return {
            "field_name": field_name,
            "change_type": "FIELD_ADDED_REQUIRED" if required else "FIELD_ADDED_OPTIONAL",
            "compatibility_class": "breaking_change" if required else "backward_compatible",
            "severity": "HIGH" if required else "LOW",
            "backward_compatible": not required,
            "forward_compatible": True,
            "rationale": f"{field_name}: added {'required' if required else 'optional'} field",
        }
    if target_clause is None and source_clause is not None:
        required = bool(source_clause.get("required", False))
        return {
            "field_name": field_name,
            "change_type": "FIELD_REMOVED_REQUIRED" if required else "FIELD_REMOVED_OPTIONAL",
            "compatibility_class": "breaking_change" if required else "forward_compatible",
            "severity": "HIGH" if required else "MEDIUM",
            "backward_compatible": True,
            "forward_compatible": not required,
            "rationale": f"{field_name}: removed {'required' if required else 'optional'} field",
        }
    if source_clause is None or target_clause is None:
        return {
            "field_name": field_name,
            "change_type": "NO_CHANGE",
            "compatibility_class": "backward_compatible",
            "severity": "LOW",
            "backward_compatible": True,
            "forward_compatible": True,
            "rationale": f"{field_name}: unchanged",
        }

    source_min = source_clause.get("minimum")
    source_max = source_clause.get("maximum")
    target_min = target_clause.get("minimum")
    target_max = target_clause.get("maximum")
    source_type = str(source_clause.get("type"))
    target_type = str(target_clause.get("type"))
    source_unit = _is_unit_interval(source_min, source_max, source_stats)
    target_unit = _is_unit_interval(target_min, target_max, target_stats)
    source_percentage = _looks_like_percentage_scale(source_min, source_max, source_stats)
    target_percentage = _looks_like_percentage_scale(target_min, target_max, target_stats)
    if source_type == target_type and ((source_percentage and target_unit) or (source_unit and target_percentage)):
        source_range_text = _range_text(source_min, source_max, source_stats)
        target_range_text = _range_text(target_min, target_max, target_stats)
        if source_min == target_min and source_max == target_max:
            declared_bounds = _range_text(source_min, source_max)
            rationale = (
                f"{field_name}: numeric scale shifted from observed range {source_range_text} into observed range {target_range_text} "
                f"while declared bounds remained {declared_bounds}"
            )
        else:
            rationale = f"{field_name}: numeric scale shifted from range {source_range_text} into range {target_range_text}"
        return {
            "field_name": field_name,
            "change_type": "NUMERIC_SCALE_SHIFT",
            "compatibility_class": "breaking_change",
            "severity": "CRITICAL",
            "backward_compatible": False,
            "forward_compatible": False,
            "rationale": rationale,
        }
    if source_type != target_type:
        source_rank = _numeric_type_rank(source_type)
        target_rank = _numeric_type_rank(target_type)
        if source_rank is not None and target_rank is not None and target_rank > source_rank:
            return {
                "field_name": field_name,
                "change_type": "TYPE_WIDENING",
                "compatibility_class": "backward_compatible",
                "severity": "LOW",
                "backward_compatible": True,
                "forward_compatible": True,
                "rationale": f"{field_name}: type widened {source_type} -> {target_type}",
            }
        if (source_unit and target_percentage) or (source_percentage and target_unit):
            source_range_text = _range_text(source_min, source_max, source_stats)
            target_range_text = _range_text(target_min, target_max, target_stats)
            return {
                "field_name": field_name,
                "change_type": "TYPE_NARROWING_SCALE_SHIFT",
                "compatibility_class": "breaking_change",
                "severity": "CRITICAL",
                "backward_compatible": False,
                "forward_compatible": False,
                "rationale": (
                    f"{field_name}: narrow type and scale shift detected "
                    f"{source_type} range {source_range_text} -> {target_type} range {target_range_text}"
                ),
            }
        return {
            "field_name": field_name,
            "change_type": "TYPE_NARROWING" if source_rank is not None and target_rank is not None else "TYPE_CHANGE",
            "compatibility_class": "breaking_change",
            "severity": "CRITICAL" if source_rank is not None and target_rank is not None else "HIGH",
            "backward_compatible": False,
            "forward_compatible": False,
            "rationale": f"{field_name}: type changed {source_type} -> {target_type}",
        }
    if any(value is not None for value in [source_min, source_max, target_min, target_max]) and (
        source_min != target_min or source_max != target_max
    ):
        widened = (
            (target_min is None or source_min is None or float(target_min) <= float(source_min))
            and (target_max is None or source_max is None or float(target_max) >= float(source_max))
        )
        return {
            "field_name": field_name,
            "change_type": "RANGE_WIDENING" if widened else "RANGE_NARROWING",
            "compatibility_class": "forward_compatible" if widened else "breaking_change",
            "severity": "LOW" if widened else "HIGH",
            "backward_compatible": False if widened else False,
            "forward_compatible": True if widened else False,
            "rationale": (
                f"{field_name}: range widened {source_min},{source_max} -> {target_min},{target_max}"
                if widened
                else f"{field_name}: range changed {source_min},{source_max} -> {target_min},{target_max}"
            ),
        }

    source_enum = _enum_set(source_clause)
    target_enum = _enum_set(target_clause)
    if source_enum != target_enum:
        removed = sorted(source_enum - target_enum)
        added = sorted(target_enum - source_enum)
        if removed:
            return {
                "field_name": field_name,
                "change_type": "ENUM_VALUE_REMOVED",
                "compatibility_class": "breaking_change",
                "severity": "HIGH",
                "backward_compatible": False,
                "forward_compatible": False,
                "rationale": f"{field_name}: enum values removed {removed}",
            }
        return {
            "field_name": field_name,
            "change_type": "ENUM_VALUE_ADDED",
            "compatibility_class": "forward_compatible",
            "severity": "LOW",
            "backward_compatible": False,
            "forward_compatible": True,
            "rationale": f"{field_name}: enum values added {added}",
        }

    return {
        "field_name": field_name,
        "change_type": "NO_CHANGE",
        "compatibility_class": "backward_compatible",
        "severity": "LOW",
        "backward_compatible": True,
        "forward_compatible": True,
        "rationale": f"{field_name}: no material change",
    }


def summarize_compatibility(changes: list[dict[str, Any]]) -> dict[str, Any]:
    material = [change for change in changes if change["change_type"] != "NO_CHANGE"]
    counts = Counter(change["compatibility_class"] for change in material)
    if any(change["compatibility_class"] == "breaking_change" for change in material):
        verdict = "breaking_change"
    elif material and all(change["backward_compatible"] for change in material):
        verdict = "backward_compatible"
    elif material and all(change["forward_compatible"] for change in material):
        verdict = "forward_compatible"
    else:
        verdict = "backward_compatible"
    primary_breaking = next((change for change in material if change["compatibility_class"] == "breaking_change"), None)
    return {
        "compatibility_verdict": verdict,
        "change_counts": dict(counts),
        "breaking_change_count": counts.get("breaking_change", 0),
        "primary_breaking_change": primary_breaking,
    }


def registry_notification(contract_id: str, changes: list[dict[str, Any]], registry_path: str | None) -> dict[str, Any]:
    breaking_fields = [change["field_name"] for change in changes if change["compatibility_class"] == "breaking_change"]
    notification = {
        "contract_id": contract_id,
        "change_type": "breaking_change" if breaking_fields else "no_breaking_change",
        "affected_subscribers": [],
        "breaking_fields": breaking_fields,
        "subscriber_details": [],
        "recommended_action": "apply adapter or update consumer logic" if breaking_fields else "no notification needed",
    }
    if not breaking_fields or not registry_path:
        return notification
    path = Path(registry_path)
    if not path.exists():
        return notification
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    subscriptions = payload.get("subscriptions", [])
    if not isinstance(subscriptions, list):
        return notification
    affected_subscribers: list[str] = []
    details: list[dict[str, Any]] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        if str(subscription.get("contract_id", "")) != contract_id:
            continue
        matched_fields: list[str] = []
        for item in subscription.get("breaking_fields", []):
            if isinstance(item, dict):
                field_name = str(item.get("field", ""))
            else:
                field_name = str(item)
            if field_name in breaking_fields:
                matched_fields.append(field_name)
        if not matched_fields:
            continue
        subscriber_id = str(subscription.get("subscriber_id", ""))
        affected_subscribers.append(subscriber_id)
        details.append(
            {
                "subscriber_id": subscriber_id,
                "contact": str(subscription.get("contact", "unknown")),
                "fields_consumed": list(subscription.get("fields_consumed", [])),
                "breaking_fields": matched_fields,
                "validation_mode": str(subscription.get("validation_mode", "AUDIT")),
                "registered_at": str(subscription.get("registered_at", "")),
                "failure_modes": [
                    {
                        "field": str(item.get("field", "")),
                        "reason": str(item.get("reason", "")),
                    }
                    for item in subscription.get("breaking_fields", [])
                    if isinstance(item, dict) and str(item.get("field", "")) in matched_fields
                ],
            }
        )
    notification["affected_subscribers"] = sorted(set(affected_subscribers))
    notification["subscriber_details"] = details
    return notification


def build_compatibility_report(
    source_contract: dict[str, Any],
    target_contract: dict[str, Any],
    registry_path: str | None = None,
) -> dict[str, Any]:
    source_fields = source_contract.get("fields", {}) or {}
    target_fields = target_contract.get("fields", {}) or {}
    source_stats = _field_statistics(source_contract)
    target_stats = _field_statistics(target_contract)
    renames = detect_renames(source_fields, target_fields)
    renamed_targets = set(renames.values())
    renamed_sources = set(renames.keys())

    changes: list[dict[str, Any]] = []
    for field_name in sorted(set(source_fields) | set(target_fields)):
        if field_name in renamed_sources:
            continue
        renamed_from = None
        if field_name in renamed_targets:
            renamed_from = next(source_name for source_name, target_name in renames.items() if target_name == field_name)
        change = classify_change(
            field_name,
            None if renamed_from else source_fields.get(field_name),
            target_fields.get(field_name),
            renamed_from=renamed_from,
            source_stats=None if renamed_from else source_stats.get(field_name),
            target_stats=target_stats.get(field_name),
        )
        changes.append(change)
    summary = summarize_compatibility(changes)
    contract_id = str(target_contract.get("contract_id") or target_contract.get("id") or "")
    notification = registry_notification(contract_id, changes, registry_path)
    return {
        "source_version": contract_version(source_contract),
        "target_version": contract_version(target_contract),
        "changes": changes,
        "renames": [{"from": source_name, "to": target_name} for source_name, target_name in sorted(renames.items())],
        "compatibility_verdict": summary["compatibility_verdict"],
        "change_counts": summary["change_counts"],
        "breaking_change_count": summary["breaking_change_count"],
        "primary_breaking_change": summary["primary_breaking_change"],
        "notification": notification,
    }
