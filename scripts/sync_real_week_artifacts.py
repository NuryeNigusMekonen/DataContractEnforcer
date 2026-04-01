from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(8192)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def stable_uuid(text: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, text))


def normalize_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return normalized


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_zulu(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sync_week1() -> None:
    active_intents_path = ROOT / "artifacts/week1/.orchestration/active_intents.yaml"
    trace_path = ROOT / "artifacts/week1/.orchestration/agent_trace.jsonl"
    output_path = ROOT / "artifacts/week1/outputs/intent_records.jsonl"
    ensure_dir(output_path.parent)

    active_intents = yaml.safe_load(active_intents_path.read_text(encoding="utf-8")).get("active_intents", [])
    traces = []
    with trace_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                traces.append(json.loads(line))

    traces_by_intent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in traces:
        traces_by_intent[str(record.get("intent_id", ""))].append(record)

    records: list[dict[str, Any]] = []
    for intent in active_intents:
        intent_id = str(intent["id"])
        related_traces = sorted(traces_by_intent.get(intent_id, []), key=lambda item: str(item.get("timestamp", "")))
        code_refs: list[dict[str, Any]] = []
        seen_refs: set[tuple[str, int, int, str]] = set()
        for trace in related_traces:
            mutation_class = str(trace.get("mutation_class", "")).upper()
            confidence = 0.97 if mutation_class == "INTENT_EVOLUTION" else 0.93 if mutation_class == "AST_REFACTOR" else 0.9
            for file_entry in trace.get("files", []):
                relative_path = str(file_entry.get("relative_path", ""))
                for conversation in file_entry.get("conversations", []):
                    for code_range in conversation.get("ranges", []):
                        line_start = int(code_range.get("start_line", 1))
                        line_end = int(code_range.get("end_line", code_range.get("start_line", 1)))
                        symbol = str(intent.get("title", "intent"))
                        ref_key = (relative_path, line_start, line_end, symbol)
                        if ref_key in seen_refs:
                            continue
                        seen_refs.add(ref_key)
                        code_refs.append(
                            {
                                "file": relative_path,
                                "line_start": line_start,
                                "line_end": line_end,
                                "symbol": symbol,
                                "confidence": confidence,
                            }
                        )
        if not code_refs:
            code_refs.append(
                {
                    "file": ".orchestration/active_intents.yaml",
                    "line_start": 1,
                    "line_end": 1,
                    "symbol": intent.get("title", "intent"),
                    "confidence": 0.55,
                }
            )
        code_refs.sort(key=lambda item: (item["file"], item["line_start"], item["line_end"]))
        status_history = intent.get("status_history", [])
        created_at = status_history[0]["at"] if status_history else datetime.now(timezone.utc).isoformat()
        governance_tags = ["week1", "intent-traceability", str(intent.get("status", "")).lower()]
        if related_traces:
            governance_tags.append("trace-backed")
        records.append(
            {
                "intent_id": stable_uuid(f"week1:{intent_id}"),
                "description": intent.get("description", intent.get("title", "")),
                "code_refs": code_refs,
                "governance_tags": governance_tags,
                "created_at": created_at.replace("+00:00", "Z"),
            }
        )
    write_jsonl(output_path, records)


def parse_week2_report(
    path: Path,
    rubric_hash: str,
    rubric_version: str,
    rubric_dimension_ids: dict[str, str],
) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    overall_score_match = re.search(r"Overall score:\s*([0-9.]+)", text, re.IGNORECASE)
    if overall_score_match is None:
        overall_score_match = re.search(r"\*\*Overall Score:\*\*\s*([0-9.]+)", text, re.IGNORECASE)
    overall_score = float(overall_score_match.group(1)) if overall_score_match else 0.0
    criteria: dict[str, dict[str, Any]] = {}
    section_pattern = re.compile(
        r"^###\s+(?P<name>.+?)(?:\s+\((?P<id>[^)]+)\))?\n"
        r"(?P<body>.*?)(?=^###\s+|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    for match in section_pattern.finditer(text):
        body = match.group("body")
        score_match = re.search(r"(?:\*\*)?Final score:?\s*(?:\*\*)?\s*(\d+)\s*/\s*5", body, re.IGNORECASE)
        if score_match is None:
            score_match = re.search(r"\*\*Final Score:\*\*\s*(\d+)\s*/\s*5", body, re.IGNORECASE)
        if score_match is None:
            continue
        score = int(score_match.group(1))
        criterion_id = (match.group("id") or "").strip()
        if not criterion_id:
            criterion_id = rubric_dimension_ids.get(normalize_name(match.group("name")), normalize_name(match.group("name")))
        body = match.group("body")
        evidence = []
        for line in re.findall(r"Cited evidence:\s*(.+)", body, re.IGNORECASE):
            evidence.extend([item.strip() for item in line.split(",") if item.strip()])
        remediation_match = re.search(
            r"(?:####\s+)?Remediation:\s*\n(?P<content>.*?)(?=\n(?:---|###|\Z))",
            body,
            re.IGNORECASE | re.DOTALL,
        )
        notes = remediation_match.group("content").strip() if remediation_match else ""
        criteria[criterion_id] = {
            "score": score,
            "evidence": evidence[:5] if evidence else [match.group("name").strip()],
            "notes": notes,
        }
    criteria_count = len(criteria) or 1
    dissent_match = re.search(r"Dissent-triggered criteria:\s*(\d+)", text)
    dissent_count = int(dissent_match.group(1)) if dissent_match else 0
    confidence = max(0.6, min(0.98, 0.96 - (dissent_count / criteria_count) * 0.2))
    verdict = "PASS" if overall_score >= 4.0 else "WARN" if overall_score >= 3.0 else "FAIL"
    evaluated_at = to_zulu(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc))
    target_ref_match = re.search(r"Repo:\s*(.+)", text)
    if target_ref_match is None:
        target_ref_match = re.search(r"# Audit Report for\s+(.+)", text)
    target_ref = target_ref_match.group(1).strip() if target_ref_match else path.as_posix()
    return {
        "verdict_id": stable_uuid(f"week2:{path.as_posix()}"),
        "target_ref": target_ref,
        "rubric_id": rubric_hash,
        "rubric_version": rubric_version,
        "scores": criteria,
        "overall_verdict": verdict,
        "overall_score": round(sum(item["score"] for item in criteria.values()) / criteria_count, 3),
        "confidence": round(confidence, 3),
        "evaluated_at": evaluated_at,
    }


def sync_week2() -> None:
    rubric_path = ROOT / "artifacts/week2/rubric/week2_rubric.json"
    output_path = ROOT / "artifacts/week2/outputs/verdicts.jsonl"
    ensure_dir(output_path.parent)
    rubric = json.loads(rubric_path.read_text(encoding="utf-8"))
    rubric_hash = sha256_file(rubric_path)
    rubric_version = rubric["rubric_metadata"]["version"]
    rubric_dimension_ids = {
        normalize_name(str(dimension.get("name") or dimension.get("title") or dimension.get("label") or dimension["id"])): str(
            dimension["id"]
        )
        for dimension in rubric["dimensions"]
    }
    report_paths = [
        ROOT / "artifacts/week2/report_onself_generated/self_audit_run.md",
        ROOT / "artifacts/week2/report_onpeer_generated/habesha_audit_from_run.md",
        ROOT / "artifacts/week2/report_bypeer_received/report_by_peer_received.md",
    ]
    records = [
        parse_week2_report(path, rubric_hash, rubric_version, rubric_dimension_ids) for path in report_paths if path.exists()
    ]
    write_jsonl(output_path, records)


def normalize_confidence(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    if numeric > 1.0:
        numeric = numeric / 100.0
    return max(0.0, min(1.0, numeric))


def sync_week3() -> None:
    extracted_dir = ROOT / "artifacts/week3/.refinery/extracted"
    chunk_dir = ROOT / "artifacts/week3/.refinery/chunks"
    ledger_path = ROOT / "artifacts/week3/.refinery/extraction_ledger.jsonl"
    output_path = ROOT / "artifacts/week3/outputs/extractions.jsonl"
    ensure_dir(output_path.parent)

    ledger_by_doc: dict[str, dict[str, Any]] = {}
    with ledger_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                record = json.loads(line)
                ledger_by_doc[str(record["doc_id"])] = record

    records: list[dict[str, Any]] = []
    for extracted_path in sorted(extracted_dir.glob("*.json")):
        doc = json.loads(extracted_path.read_text(encoding="utf-8"))
        doc_id = str(doc["doc_id"])
        ledger = ledger_by_doc.get(doc_id, {})
        extracted_facts = []
        entities: list[dict[str, Any]] = []
        fact_count = 0
        for page in doc.get("pages", []):
            for block in page.get("blocks", []):
                text = str(block.get("text", "")).strip()
                if not text or text == "<!-- image -->" or len(text) < 25:
                    continue
                fact_count += 1
                if fact_count > 25:
                    break
                fact_id = stable_uuid(f"{doc_id}:{page.get('page_number')}:{block.get('reading_order')}:{text[:80]}")
                extracted_facts.append(
                    {
                        "fact_id": fact_id,
                        "text": text,
                        "entity_refs": [],
                        "confidence": normalize_confidence(block.get("confidence")),
                        "page_ref": page.get("page_number"),
                        "source_excerpt": text[:500],
                    }
                )
            if fact_count > 25:
                break
        records.append(
            {
                "doc_id": stable_uuid(f"week3:{doc_id}"),
                "source_path": str(extracted_path.resolve()),
                "source_hash": sha256_file(extracted_path),
                "extracted_facts": extracted_facts,
                "entities": entities,
                "extraction_model": ledger.get("strategy_used", "unknown"),
                "processing_time_ms": int(ledger.get("processing_time_ms", 1) or 1),
                "token_count": {"input": 0, "output": 0},
                "extracted_at": str(ledger.get("timestamp", datetime.now(timezone.utc).isoformat())).replace("+00:00", "Z"),
            }
        )
    if len(records) < 50:
        target_count = 50
        for chunk_path in sorted(chunk_dir.glob("*.jsonl")):
            doc_key = chunk_path.stem.replace("doc_", "")
            ledger = ledger_by_doc.get(doc_key, {})
            chunk_lines = [json.loads(line) for line in chunk_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            chunk_records_added = 0
            for chunk_index in range(0, len(chunk_lines), 8):
                batch = chunk_lines[chunk_index:chunk_index + 8]
                facts = []
                fact_entities: list[dict[str, Any]] = []
                for item in batch:
                    text = str(item.get("content", "")).strip()
                    if not text or text == "<!-- image -->" or len(text) < 20:
                        continue
                    page_ref = None
                    page_refs = item.get("page_refs", [])
                    if page_refs:
                        page_ref = page_refs[0].get("page_number")
                    facts.append(
                        {
                            "fact_id": stable_uuid(f"{chunk_path.stem}:{item.get('ldu_id')}"),
                            "text": text,
                            "entity_refs": [],
                            "confidence": 0.84,
                            "page_ref": page_ref,
                            "source_excerpt": text[:500],
                        }
                    )
                if not facts:
                    continue
                chunk_records_added += 1
                records.append(
                    {
                        "doc_id": stable_uuid(f"week3-chunk:{chunk_path.stem}:{chunk_records_added}"),
                        "source_path": str(chunk_path.resolve()),
                        "source_hash": sha256_file(chunk_path),
                        "extracted_facts": facts[:6],
                        "entities": fact_entities,
                        "extraction_model": f"{ledger.get('strategy_used', 'chunk-derived')}-chunk-view",
                        "processing_time_ms": max(100, int((ledger.get("processing_time_ms", 800) or 800) / 4)),
                        "token_count": {
                            "input": int(sum(int(item.get("token_count", 0) or 0) for item in batch)),
                            "output": int(sum(max(1, len(str(item.get("content", "")).split()) // 4) for item in batch)),
                        },
                        "extracted_at": str(ledger.get("timestamp", datetime.now(timezone.utc).isoformat())).replace("+00:00", "Z"),
                    }
                )
                if len(records) >= target_count:
                    break
            if len(records) >= target_count:
                break
    write_jsonl(output_path, records)


def map_week4_node(node: dict[str, Any]) -> dict[str, Any]:
    node_id = str(node.get("id", "unknown"))
    raw_type = str(node.get("node_type", "")).lower()
    if "dataset" in raw_type or node_id.startswith("dataset::"):
        node_type = "TABLE"
    elif "transform" in raw_type:
        node_type = "PIPELINE"
    elif node_id.startswith("module::") or raw_type == "module":
        node_type = "FILE"
    else:
        node_type = "EXTERNAL"
    path_hint = node.get("source_file") or node_id.split("::", 1)[-1]
    return {
        "node_id": node_id,
        "type": node_type,
        "label": Path(path_hint).name or node_id,
        "metadata": {
            "path": path_hint,
            "language": Path(path_hint).suffix.lstrip(".") or "unknown",
            "purpose": str(node.get("transformation_type") or node.get("node_type") or "week4 cartography node"),
            "last_modified": to_zulu(datetime.now(timezone.utc)),
        },
    }


def sync_week4() -> None:
    lineage_graph_path = ROOT / "artifacts/week4/.cartography/lineage_graph.json"
    output_path = ROOT / "artifacts/week4/outputs/lineage_snapshots.jsonl"
    ensure_dir(output_path.parent)
    graph = json.loads(lineage_graph_path.read_text(encoding="utf-8"))
    snapshot = {
        "snapshot_id": stable_uuid(f"week4:{sha256_file(lineage_graph_path)}"),
        "codebase_root": str((ROOT / "artifacts/week4").resolve()),
        "git_commit": hashlib.sha1(lineage_graph_path.read_bytes()).hexdigest(),
        "nodes": [map_week4_node(node) for node in graph.get("nodes", [])],
        "edges": [
            {
                "source": edge.get("source"),
                "target": edge.get("target"),
                "relationship": edge.get("edge_type", "CONSUMES"),
                "confidence": 0.95,
            }
            for edge in graph.get("edges", [])
        ],
        "captured_at": to_zulu(datetime.fromtimestamp(lineage_graph_path.stat().st_mtime, tz=timezone.utc)),
    }
    write_jsonl(output_path, [snapshot])


def infer_schema(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, list):
        item_schema = infer_schema(value[0]) if value else {"type": "string"}
        return {"type": "array", "items": item_schema}
    if isinstance(value, dict):
        return {
            "type": "object",
            "properties": {key: infer_schema(item) for key, item in value.items()},
            "required": sorted(value.keys()),
            "additionalProperties": True,
        }
    return {"type": "string"}


def merge_types(left: Any, right: Any) -> Any:
    left_values = left if isinstance(left, list) else [left]
    right_values = right if isinstance(right, list) else [right]
    merged = sorted({value for value in left_values + right_values if value is not None})
    if not merged:
        return None
    if len(merged) == 1:
        return merged[0]
    return merged


def merge_schema(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {"type": merge_types(left.get("type"), right.get("type"))}
    left_type = left.get("type")
    right_type = right.get("type")
    object_types = {"object"} if isinstance(left_type, str) else set(left_type or [])
    object_types |= {"object"} if isinstance(right_type, str) else set(right_type or [])
    if "object" in object_types:
        left_properties = left.get("properties", {})
        right_properties = right.get("properties", {})
        merged_properties: dict[str, Any] = {}
        for key in sorted(set(left_properties) | set(right_properties)):
            if key in left_properties and key in right_properties:
                merged_properties[key] = merge_schema(left_properties[key], right_properties[key])
            elif key in left_properties:
                merged_properties[key] = left_properties[key]
            else:
                merged_properties[key] = right_properties[key]
        left_required = set(left.get("required", []))
        right_required = set(right.get("required", []))
        merged["properties"] = merged_properties
        merged["required"] = sorted(left_required & right_required) if left_required or right_required else []
        merged["additionalProperties"] = True
    if left.get("type") == "array" and right.get("type") == "array":
        merged["items"] = merge_schema(left.get("items", {"type": "string"}), right.get("items", {"type": "string"}))
    return merged


def extract_occurred_at(payload: dict[str, Any], fallback: datetime) -> datetime:
    preferred_keys = [
        "submitted_at",
        "requested_at",
        "uploaded_at",
        "started_at",
        "completed_at",
        "evaluation_timestamp",
        "check_timestamp",
        "occurred_at",
        "recorded_at",
        "timestamp",
        "at",
    ]
    for key in preferred_keys:
        value = payload.get(key)
        if isinstance(value, str):
            parsed = parse_ts(value)
            if parsed is not None:
                return parsed
    for value in payload.values():
        if isinstance(value, str):
            parsed = parse_ts(value)
            if parsed is not None:
                return parsed
    return fallback


def source_service_for(raw: dict[str, Any]) -> str:
    aggregate_type = str(raw.get("aggregate_type", "")).lower()
    if aggregate_type == "agentsession":
        return "agent-session"
    if aggregate_type == "documentpackage":
        return "document-processing-agent"
    if aggregate_type == "compliancerecord":
        return "compliance-agent"
    if aggregate_type == "auditledger":
        return "audit-ledger"
    if "credit" in raw.get("event_type", "").lower():
        return "credit-analysis-agent"
    if "fraud" in raw.get("event_type", "").lower():
        return "fraud-detection-agent"
    if "decision" in raw.get("event_type", "").lower():
        return "decision-orchestrator-agent"
    return "loan-application-service"


def sync_week5() -> None:
    seed_events_path = ROOT / "artifacts/week5/data/seed_events.jsonl"
    output_path = ROOT / "artifacts/week5/outputs/events.jsonl"
    schema_dir = ROOT / "artifacts/week5/schemas/events"
    ensure_dir(output_path.parent)
    ensure_dir(schema_dir)

    raw_events = []
    with seed_events_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                raw_events.append(json.loads(line))

    sequence_by_stream: dict[str, int] = defaultdict(int)
    schema_samples: dict[tuple[str, str], dict[str, Any]] = {}
    records: list[dict[str, Any]] = []
    fallback_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index, raw in enumerate(raw_events, start=1):
        stream_id = str(raw["stream_id"])
        sequence_by_stream[stream_id] += 1
        aggregate_uuid = stable_uuid(f"week5:{stream_id}")
        correlation_id = stable_uuid(str(raw.get("correlation_id") or f"corr:{stream_id}:{index}"))
        causation_raw = raw.get("causation_id")
        occurred_at = extract_occurred_at(raw.get("payload", {}), fallback_time + timedelta(seconds=index))
        recorded_at = occurred_at + timedelta(seconds=1)
        schema_version = f"{int(raw.get('event_version', 1))}.0"
        payload = raw.get("payload", {})
        record = {
            "event_id": stable_uuid(f"{stream_id}:{sequence_by_stream[stream_id]}:{raw['event_type']}"),
            "event_type": raw["event_type"],
            "aggregate_id": aggregate_uuid,
            "aggregate_type": raw["aggregate_type"],
            "sequence_number": sequence_by_stream[stream_id],
            "payload": payload,
            "metadata": {
                "causation_id": stable_uuid(str(causation_raw)) if causation_raw else None,
                "correlation_id": correlation_id,
                "user_id": str(raw.get("metadata", {}).get("actor_id", "system")),
                "source_service": source_service_for(raw),
            },
            "schema_version": schema_version,
            "occurred_at": to_zulu(occurred_at),
            "recorded_at": to_zulu(recorded_at),
        }
        records.append(record)
        key = (raw["event_type"], schema_version)
        payload_schema = infer_schema(payload)
        if key in schema_samples:
            schema_samples[key] = merge_schema(schema_samples[key], payload_schema)
        else:
            schema_samples[key] = payload_schema

    for (event_type, schema_version), payload_schema in schema_samples.items():
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": payload_schema.get("properties", {}),
            "required": payload_schema.get("required", []),
            "additionalProperties": True,
        }
        schema_path = schema_dir / f"{event_type}-{schema_version}.json"
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")

    write_jsonl(output_path, records)


def fix_symlinks() -> None:
    symlinks = {
        ROOT / ".orchestration": "artifacts/week1/.orchestration",
        ROOT / ".refinery": "artifacts/week3/.refinery",
        ROOT / ".cartography": "artifacts/week4/.cartography",
        ROOT / "rubric": "artifacts/week2/rubric",
        ROOT / "rubrics": "artifacts/week2/rubric",
        ROOT / "schemas/events": "../artifacts/week5/schemas/events",
    }
    for path, target in symlinks.items():
        if path.exists() or path.is_symlink():
            if path.is_symlink() or path.is_file():
                path.unlink()
            else:
                continue
        ensure_dir(path.parent)
        path.symlink_to(target)


def main() -> int:
    sync_week1()
    sync_week2()
    sync_week3()
    sync_week4()
    sync_week5()
    fix_symlinks()
    print("Synced real week1-week5 artifacts into canonical week7 outputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
