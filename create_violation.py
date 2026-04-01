from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import load_jsonl, write_jsonl
def main() -> int:
    records = load_jsonl("outputs/week3/extractions.jsonl")
    for record in records:
        for fact in record.get("extracted_facts", []):
            confidence = fact.get("confidence")
            if isinstance(confidence, (int, float)):
                fact["confidence"] = round(float(confidence) * 100, 1)
    write_jsonl("outputs/week3/extractions_violated.jsonl", records)
    traces = load_jsonl("outputs/traces/runs.jsonl")
    for index, record in enumerate(traces):
        if index % 3 == 0:
            record["total_tokens"] = int(record.get("total_tokens", 0)) + 17
    write_jsonl("outputs/traces/runs_violated.jsonl", traces)
    print("INJECTION: confidence scale changed from 0.0-1.0 to 0-100")
    print("INJECTION: trace total_tokens no longer equals prompt_tokens + completion_tokens")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
