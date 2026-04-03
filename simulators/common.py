from __future__ import annotations

import copy
import hashlib
import json
import math
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "outputs"
SCENARIO_ROOT = ROOT / "test_data" / "scenarios"

SYSTEM_OUTPUT_PATHS: dict[str, Path] = {
    "week1": OUTPUT_ROOT / "week1" / "intent_records.jsonl",
    "week2": OUTPUT_ROOT / "week2" / "verdicts.jsonl",
    "week3": OUTPUT_ROOT / "week3" / "extractions.jsonl",
    "week4": OUTPUT_ROOT / "week4" / "lineage_snapshots.jsonl",
    "week5": OUTPUT_ROOT / "week5" / "events.jsonl",
    "traces": OUTPUT_ROOT / "traces" / "runs.jsonl",
}
DEFAULT_COUNTS: dict[str, int] = {
    "week1": 20,
    "week2": 20,
    "week3": 20,
    "week4": 5,
    "week5": 50,
    "traces": 30,
}
SYSTEM_ALIASES = {
    "week1": "week1",
    "week2": "week2",
    "week3": "week3",
    "week4": "week4",
    "week5": "week5",
    "trace": "traces",
    "traces": "traces",
    "langsmith": "traces",
    "langsmith_traces": "traces",
}
DEFAULT_MODES: dict[tuple[str, str], str] = {
    ("week3", "confidence_scale_break"): "all_records",
    ("traces", "total_tokens_mismatch"): "all_records",
}
EXTRA_OUTPUTS = [
    OUTPUT_ROOT / "week3" / "extractions_violated.jsonl",
    OUTPUT_ROOT / "traces" / "runs_violated.jsonl",
]


JsonDict = dict[str, Any]


@dataclass(frozen=True)
class ViolationSpec:
    system: str
    type: str
    field: str = ""
    mode: str = "first_record"
    record_index: int | None = None
    count: int | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "ViolationSpec":
        system = canonical_system_name(str(payload.get("system", "")))
        violation_type = str(payload.get("type", "")).strip()
        mode = str(payload.get("mode") or default_mode_for(system, violation_type))
        record_index = payload.get("record_index")
        count = payload.get("count")
        return cls(
            system=system,
            type=violation_type,
            field=str(payload.get("field", "")),
            mode=mode,
            record_index=int(record_index) if isinstance(record_index, int) else None,
            count=int(count) if isinstance(count, int) else None,
        )

    def to_summary(self) -> dict[str, Any]:
        return {
            "system": self.system,
            "type": self.type,
            "field": self.field,
            "mode": self.mode,
        }


@dataclass(frozen=True)
class ScenarioConfig:
    name: str
    seed: int
    counts: dict[str, int]
    enabled_simulators: tuple[str, ...]
    violations: tuple[ViolationSpec, ...]
    healthy: bool
    clear_existing: bool

    def violations_for(self, system: str) -> list[ViolationSpec]:
        canonical = canonical_system_name(system)
        return [violation for violation in self.violations if violation.system == canonical]


@dataclass(frozen=True)
class ApplicationContext:
    index: int
    application_id: str
    aggregate_id: str
    applicant_id: str
    company_name: str
    industry: str
    country: str
    requested_amount_usd: float
    package_id: str
    correlation_id: str
    agent_session_id: str
    underwriting_agent_id: str
    source_hash: str
    document_paths: dict[str, str]
    refinery_paths: dict[str, str]
    financials: dict[str, float]


COMPANY_PREFIXES = [
    "Abay",
    "Blue Nile",
    "Lalibela",
    "Tana",
    "Walia",
    "Selam",
    "Kedamawi",
    "Unity",
    "Aster",
    "Merkato",
    "Beacon",
    "Cobalt",
]
COMPANY_SUFFIXES = [
    "Industries",
    "Holdings",
    "Textiles",
    "Foods",
    "Logistics",
    "AgriWorks",
    "Capital",
    "Power",
    "Transport",
    "Manufacturing",
]
INDUSTRIES = [
    "manufacturing",
    "logistics",
    "agriculture",
    "energy",
    "retail",
    "insurance",
    "construction",
    "healthcare",
]
COUNTRIES = ["Ethiopia", "Kenya", "Uganda", "Rwanda", "Tanzania"]
DOCUMENT_TYPES = ["income_statement", "balance_sheet", "cash_flow_statement", "compliance_memo"]


def canonical_system_name(value: str) -> str:
    canonical = SYSTEM_ALIASES.get(value.strip().lower())
    if canonical is None:
        raise ValueError(f"unknown simulator system: {value}")
    return canonical


def default_mode_for(system: str, violation_type: str) -> str:
    return DEFAULT_MODES.get((canonical_system_name(system), violation_type), "first_record")


def stable_int(*parts: object, modulus: int = 2**31 - 1) -> int:
    rendered = "::".join(str(part) for part in parts)
    digest = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % modulus


def seeded_random(seed: int, *parts: object) -> random.Random:
    return random.Random(stable_int(seed, *parts))


def deterministic_uuid(*parts: object) -> str:
    rendered = "::".join(str(part) for part in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, rendered))


def fake_sha256(*parts: object) -> str:
    rendered = "::".join(str(part) for part in parts)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def fake_git_sha(*parts: object) -> str:
    rendered = "::".join(str(part) for part in parts)
    return hashlib.sha1(rendered.encode("utf-8")).hexdigest()


def isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def offset_timestamp(value: str, *, seconds: int = 0, minutes: int = 0) -> str:
    return isoformat_z(parse_timestamp(value) + timedelta(seconds=seconds, minutes=minutes))


def fake_model_name(rng: random.Random, family: str) -> str:
    catalog = {
        "extraction": [
            "layout-aware-extractor-v2",
            "chunk-derived-chunk-view",
            "ocr-fusion-refinery-v1",
        ],
        "trace": [
            "sim-graph-router-v2",
            "sim-decision-analyst-v3",
            "sim-embedding-indexer-v1",
        ],
        "decision": [
            "digital-courtroom-scorer-v2",
            "credit-orchestrator-v3",
            "compliance-sentinel-v1",
        ],
    }
    options = catalog.get(family, ["simulated-model-v1"])
    return rng.choice(options)


def sample_enum(rng: random.Random, values: list[str]) -> str:
    return rng.choice(values)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def mean_score(scores: dict[str, dict[str, Any]]) -> float:
    numeric_scores = [float(item["score"]) for item in scores.values() if isinstance(item, dict) and "score" in item]
    if not numeric_scores:
        return 0.0
    return round(sum(numeric_scores) / len(numeric_scores), 3)


def derive_overall_verdict(score: float) -> str:
    if score >= 4.0:
        return "PASS"
    if score >= 2.5:
        return "WARN"
    return "FAIL"


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, records: list[JsonDict]) -> None:
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def load_jsonl(path: Path) -> list[JsonDict]:
    if not path.exists():
        return []
    records: list[JsonDict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))
    return records


def clear_output_files() -> list[str]:
    removed: list[str] = []
    for path in [*SYSTEM_OUTPUT_PATHS.values(), *EXTRA_OUTPUTS]:
        if path.exists():
            path.unlink()
            removed.append(str(path.relative_to(ROOT)))
    return removed


def load_scenario(path: str | Path) -> ScenarioConfig:
    scenario_path = Path(path)
    if not scenario_path.is_absolute():
        scenario_path = ROOT / scenario_path
    if not scenario_path.exists():
        raise FileNotFoundError(f"scenario not found: {scenario_path}")
    payload = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
    enabled_payload = payload.get("enabled_simulators") or payload.get("enabled") or list(DEFAULT_COUNTS)
    enabled = tuple(canonical_system_name(str(item)) for item in enabled_payload)
    counts = {key: int(payload.get("counts", {}).get(key, default)) for key, default in DEFAULT_COUNTS.items()}
    violations = tuple(ViolationSpec.from_mapping(item) for item in payload.get("violations", []))
    return ScenarioConfig(
        name=str(payload.get("name") or scenario_path.stem),
        seed=int(payload.get("seed", 42)),
        counts=counts,
        enabled_simulators=enabled,
        violations=violations,
        healthy=bool(payload.get("healthy", not violations)),
        clear_existing=bool(payload.get("clear_existing", False)),
    )


def scenario_path(name: str) -> Path:
    candidate = SCENARIO_ROOT / name
    if candidate.exists():
        return candidate
    return candidate.with_suffix(".yaml")


def group_violations_by_system(violations: list[ViolationSpec] | tuple[ViolationSpec, ...]) -> dict[str, list[ViolationSpec]]:
    grouped: dict[str, list[ViolationSpec]] = {}
    for violation in violations:
        grouped.setdefault(violation.system, []).append(violation)
    return grouped


def deep_copy_records(records: list[JsonDict]) -> list[JsonDict]:
    return copy.deepcopy(records)


def selected_indices(records: list[JsonDict], violation: ViolationSpec, seed: int) -> list[int]:
    if not records:
        return []
    if violation.record_index is not None:
        index = min(max(violation.record_index, 0), len(records) - 1)
        return [index]
    if violation.mode == "all_records":
        return list(range(len(records)))
    if violation.mode == "last_record":
        return [len(records) - 1]
    if violation.mode == "random_record":
        return [seeded_random(seed, violation.system, violation.type).randrange(len(records))]
    count = max(1, min(len(records), violation.count or 1))
    return list(range(count))


def output_path_for_system(system: str) -> Path:
    return SYSTEM_OUTPUT_PATHS[canonical_system_name(system)]


def outputs_summary(records_by_system: dict[str, list[JsonDict]]) -> dict[str, int]:
    return {system: len(records) for system, records in records_by_system.items()}


def build_application_catalog(count: int, seed: int) -> list[ApplicationContext]:
    rng = seeded_random(seed, "application_catalog")
    applications: list[ApplicationContext] = []
    start = datetime(2026, 2, 10, 9, 0, tzinfo=UTC)
    for index in range(count):
        company_name = f"{COMPANY_PREFIXES[index % len(COMPANY_PREFIXES)]} {COMPANY_SUFFIXES[(index * 3) % len(COMPANY_SUFFIXES)]}"
        industry = INDUSTRIES[index % len(INDUSTRIES)]
        country = COUNTRIES[index % len(COUNTRIES)]
        applicant_id = f"ET-COMP-{index + 1:04d}"
        application_id = f"APP-2026-{index + 1:04d}"
        requested_amount_usd = round(350_000 + (index * 187_500) + rng.uniform(25_000, 450_000), 2)
        total_revenue = round(requested_amount_usd * rng.uniform(1.8, 3.2), 2)
        total_assets = round(total_revenue * rng.uniform(1.4, 2.1), 2)
        total_liabilities = round(total_assets * rng.uniform(0.32, 0.58), 2)
        net_income = round(total_revenue * rng.uniform(0.08, 0.19), 2)
        ebitda = round(net_income * rng.uniform(1.1, 1.7), 2)
        doc_root = ROOT / "artifacts" / "week5" / "simulated_documents" / applicant_id
        refinery_root = ROOT / "artifacts" / "week3" / ".refinery" / "extracted"
        document_paths = {
            "income_statement": str(doc_root / f"{application_id.lower()}_income_statement_2025.pdf"),
            "balance_sheet": str(doc_root / f"{application_id.lower()}_balance_sheet_2025.pdf"),
            "cash_flow_statement": str(doc_root / f"{application_id.lower()}_cash_flow_statement_2025.pdf"),
            "compliance_memo": str(doc_root / f"{application_id.lower()}_compliance_memo_2025.pdf"),
        }
        refinery_paths = {
            document_type: str(refinery_root / f"{application_id.lower()}_{document_type}.jsonl")
            for document_type in DOCUMENT_TYPES
        }
        applications.append(
            ApplicationContext(
                index=index,
                application_id=application_id,
                aggregate_id=deterministic_uuid("aggregate", application_id),
                applicant_id=applicant_id,
                company_name=company_name,
                industry=industry,
                country=country,
                requested_amount_usd=requested_amount_usd,
                package_id=f"docpkg-{index + 1:04d}",
                correlation_id=deterministic_uuid("correlation", application_id),
                agent_session_id=deterministic_uuid("agent-session", application_id),
                underwriting_agent_id=f"underwriter-{(index % 4) + 1:02d}",
                source_hash=fake_sha256(application_id, industry, start + timedelta(hours=index)),
                document_paths=document_paths,
                refinery_paths=refinery_paths,
                financials={
                    "total_revenue": total_revenue,
                    "total_assets": total_assets,
                    "total_liabilities": total_liabilities,
                    "net_income": net_income,
                    "ebitda": ebitda,
                },
            )
        )
    return applications


def required_application_count(counts: dict[str, int]) -> int:
    week3_needed = math.ceil(counts["week3"] / max(1, len(DOCUMENT_TYPES)))
    week5_needed = max(1, math.ceil(counts["week5"] / 10))
    trace_needed = max(1, math.ceil(counts["traces"] / 6))
    return max(week3_needed, week5_needed, trace_needed, 6)

