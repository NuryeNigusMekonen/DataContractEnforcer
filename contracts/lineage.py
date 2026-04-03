from __future__ import annotations

from collections import defaultdict, deque
from pathlib import Path
import re
from typing import Any

import yaml

from contracts.common import load_jsonl


TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def load_latest_lineage_snapshot(lineage_path: str | None) -> dict[str, Any]:
    if not lineage_path:
        return {}
    snapshots = load_jsonl(lineage_path)
    return snapshots[-1] if snapshots else {}


def _tokenize(*values: str) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        for token in TOKEN_PATTERN.findall(str(value).lower()):
            if len(token) >= 3:
                tokens.add(token)
    return tokens


def _prefix_from_identifier(value: str) -> str:
    return str(value).split("-", 1)[0]


def contract_ids_from_registry(registry_path: str | None) -> list[str]:
    if not registry_path:
        return []
    path = Path(registry_path)
    if not path.exists():
        return []
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    subscriptions = payload.get("subscriptions", [])
    if not isinstance(subscriptions, list):
        return []
    contract_ids: list[str] = []
    seen: set[str] = set()
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        contract_id = str(subscription.get("contract_id", ""))
        if contract_id and contract_id not in seen:
            seen.add(contract_id)
            contract_ids.append(contract_id)
    return contract_ids


def infer_published_contracts(subscriber_id: str, contract_ids: list[str]) -> list[str]:
    subscriber_prefix = _prefix_from_identifier(subscriber_id)
    if not subscriber_prefix:
        return []
    matches = [contract_id for contract_id in contract_ids if contract_id.startswith(f"{subscriber_prefix}-")]
    return sorted(matches)


def build_contract_dependency_graph(registry_path: str | None) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    forward: dict[str, list[dict[str, Any]]] = defaultdict(list)
    reverse: dict[str, list[dict[str, Any]]] = defaultdict(list)
    node_meta: dict[str, dict[str, Any]] = {}
    if not registry_path:
        return forward, reverse, node_meta
    path = Path(registry_path)
    if not path.exists():
        return forward, reverse, node_meta
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    subscriptions = payload.get("subscriptions", [])
    if not isinstance(subscriptions, list):
        return forward, reverse, node_meta

    contract_ids = contract_ids_from_registry(registry_path)
    for contract_id in contract_ids:
        node_meta[contract_id] = {"kind": "CONTRACT", "contract_id": contract_id}

    publisher_cache: dict[str, list[str]] = {}
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        contract_id = str(subscription.get("contract_id", ""))
        subscriber_id = str(subscription.get("subscriber_id", ""))
        if not contract_id or not subscriber_id:
            continue
        node_meta.setdefault(contract_id, {"kind": "CONTRACT", "contract_id": contract_id})
        node_meta[subscriber_id] = {
            "kind": "SUBSCRIBER",
            "subscriber_id": subscriber_id,
            "contact": str(subscription.get("contact", "unknown")),
            "validation_mode": str(subscription.get("validation_mode", "AUDIT")),
        }
        edge = {
            "source": contract_id,
            "target": subscriber_id,
            "relationship": "CONSUMED_BY",
            "fields_consumed": list(subscription.get("fields_consumed", [])),
            "breaking_fields": list(subscription.get("breaking_fields", [])),
            "contact": str(subscription.get("contact", "unknown")),
            "validation_mode": str(subscription.get("validation_mode", "AUDIT")),
            "inferred": False,
        }
        forward[contract_id].append(edge)
        reverse[subscriber_id].append(edge)

        if subscriber_id not in publisher_cache:
            publisher_cache[subscriber_id] = infer_published_contracts(subscriber_id, contract_ids)
        for published_contract in publisher_cache[subscriber_id]:
            if published_contract == contract_id:
                continue
            publish_edge = {
                "source": subscriber_id,
                "target": published_contract,
                "relationship": "PUBLISHES",
                "inferred": True,
            }
            forward[subscriber_id].append(publish_edge)
            reverse[published_contract].append(publish_edge)
            node_meta.setdefault(published_contract, {"kind": "CONTRACT", "contract_id": published_contract})

    return forward, reverse, node_meta


def _bfs(start_nodes: list[str], adjacency: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    visited: dict[str, dict[str, Any]] = {}
    queue: deque[tuple[str, list[str], list[str]]] = deque()
    seen: set[str] = set()
    for start_node in start_nodes:
        if not start_node:
            continue
        queue.append((start_node, [start_node], []))
        seen.add(start_node)
    while queue:
        current, node_path, edge_path = queue.popleft()
        for edge in adjacency.get(current, []):
            target = str(edge.get("target", ""))
            if not target:
                continue
            next_node_path = [*node_path, target]
            next_edge_path = [*edge_path, str(edge.get("relationship", ""))]
            if target not in visited:
                visited[target] = {
                    "path": next_node_path,
                    "edge_path": next_edge_path,
                    "hops": len(next_edge_path),
                    "terminal_edge": edge,
                }
            if target in seen:
                continue
            seen.add(target)
            queue.append((target, next_node_path, next_edge_path))
    return visited


def contract_graph_lineage(contract_id: str, registry_path: str | None) -> dict[str, list[dict[str, Any]]]:
    if not contract_id:
        return {"upstream": [], "downstream": []}
    forward, reverse, node_meta = build_contract_dependency_graph(registry_path)
    downstream_walk = _bfs([contract_id], forward)
    upstream_walk = _bfs([contract_id], reverse)

    def build_entries(walk: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for node_id, info in sorted(walk.items(), key=lambda item: (item[1]["hops"], item[0])):
            if node_id == contract_id:
                continue
            terminal_edge = info["terminal_edge"]
            metadata = node_meta.get(node_id, {})
            entry = {
                "id": node_id,
                "kind": metadata.get("kind", "UNKNOWN"),
                "relationship": str(terminal_edge.get("relationship", "")),
                "hops": info["hops"],
                "source": "contract_graph",
                "via": info["path"][1:-1],
                "relationship_path": info["edge_path"],
            }
            for key in ("fields_consumed", "breaking_fields", "contact", "validation_mode"):
                if key in terminal_edge:
                    entry[key] = terminal_edge[key]
            entries.append(entry)
        return entries

    return {
        "upstream": build_entries(upstream_walk),
        "downstream": build_entries(downstream_walk),
    }


def _lineage_graph_indexes(lineage_snapshot: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    node_index: dict[str, dict[str, Any]] = {}
    forward: dict[str, list[dict[str, Any]]] = defaultdict(list)
    reverse: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for node in lineage_snapshot.get("nodes", []):
        node_id = str(node.get("node_id", ""))
        if node_id:
            node_index[node_id] = node
    for edge in lineage_snapshot.get("edges", []):
        source = str(edge.get("source", ""))
        target = str(edge.get("target", ""))
        if not source or not target:
            continue
        forward[source].append(edge)
        reverse[target].append(
            {
                "source": target,
                "target": source,
                "relationship": f"REVERSE_{edge.get('relationship', '')}",
                "confidence": edge.get("confidence"),
                "original_relationship": edge.get("relationship"),
            }
        )
    return node_index, forward, reverse


def match_contract_seed_nodes(contract: dict[str, Any], lineage_snapshot: dict[str, Any]) -> list[str]:
    source_path = str(contract.get("source_path", ""))
    contract_id = str(contract.get("contract_id") or contract.get("id") or "")
    dataset = str(contract.get("dataset", ""))
    info = contract.get("info", {}) if isinstance(contract.get("info"), dict) else {}
    title = str(info.get("title", ""))
    base_name = Path(source_path).name
    stem = Path(source_path).stem
    strong_tokens = _tokenize(contract_id, dataset, title, base_name, stem)

    scored: list[tuple[int, str]] = []
    for node in lineage_snapshot.get("nodes", []):
        node_id = str(node.get("node_id", ""))
        node_path = str(node.get("metadata", {}).get("path", ""))
        label = str(node.get("label", ""))
        haystack = f"{node_id} {node_path} {label}".lower()
        score = 0
        if source_path and node_path == source_path:
            score += 10
        if base_name:
            if node_path.endswith(base_name):
                score += 8
            if label == base_name:
                score += 6
        if stem:
            stem_matches = sum(1 for token in _tokenize(stem) if token in haystack)
            score += stem_matches * 2
        score += sum(1 for token in strong_tokens if token in haystack)
        if score > 0:
            scored.append((score, node_id))
    if not scored:
        return []
    max_score = max(score for score, _ in scored)
    threshold = max(4, max_score - 1)
    return [node_id for score, node_id in sorted(scored, key=lambda item: (-item[0], item[1])) if score >= threshold]


def lineage_snapshot_lineage(contract: dict[str, Any], lineage_snapshot: dict[str, Any]) -> dict[str, Any]:
    if not lineage_snapshot:
        return {"upstream": [], "downstream": [], "graph_seeds": []}
    node_index, forward, reverse = _lineage_graph_indexes(lineage_snapshot)
    seeds = match_contract_seed_nodes(contract, lineage_snapshot)
    if not seeds:
        return {"upstream": [], "downstream": [], "graph_seeds": []}

    downstream_walk = _bfs(seeds, forward)
    upstream_walk = _bfs(seeds, reverse)

    def build_entries(walk: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for node_id, info in sorted(walk.items(), key=lambda item: (item[1]["hops"], item[0])):
            if node_id in seeds:
                continue
            node = node_index.get(node_id, {})
            entry = {
                "id": node_id,
                "kind": str(node.get("type", "UNKNOWN")),
                "label": str(node.get("label", "")),
                "path": str(node.get("metadata", {}).get("path", "")),
                "relationship": str(info["terminal_edge"].get("relationship", "")),
                "hops": info["hops"],
                "source": "lineage_graph",
                "via": info["path"][1:-1],
                "relationship_path": info["edge_path"],
            }
            confidence = info["terminal_edge"].get("confidence")
            if confidence is not None:
                entry["confidence"] = confidence
            entries.append(entry)
        return entries

    return {
        "upstream": build_entries(upstream_walk),
        "downstream": build_entries(downstream_walk),
        "graph_seeds": seeds,
    }


def _merge_lineage_entries(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    best: dict[tuple[str, str, str], int] = {}
    for group in groups:
        for entry in group:
            key = (str(entry.get("source", "")), str(entry.get("kind", "")), str(entry.get("id", "")))
            hops = int(entry.get("hops", 0))
            if key in best:
                current_index = best[key]
                if hops < int(merged[current_index].get("hops", 0)):
                    merged[current_index] = entry
                continue
            best[key] = len(merged)
            merged.append(entry)
    return sorted(merged, key=lambda entry: (int(entry.get("hops", 0)), str(entry.get("id", ""))))


def resolve_contract_lineage(contract: dict[str, Any], lineage_snapshot: dict[str, Any], registry_path: str | None) -> dict[str, Any]:
    contract_id = str(contract.get("contract_id") or contract.get("id") or "")
    graph_lineage = lineage_snapshot_lineage(contract, lineage_snapshot)
    contract_lineage = contract_graph_lineage(contract_id, registry_path)
    return {
        "upstream": _merge_lineage_entries(contract_lineage["upstream"], graph_lineage["upstream"]),
        "downstream": _merge_lineage_entries(contract_lineage["downstream"], graph_lineage["downstream"]),
        "graph_seeds": graph_lineage["graph_seeds"],
    }
