from __future__ import annotations

import argparse
import fnmatch
import json
from pathlib import Path


DEFAULT_KEEP_PATTERNS = [
    "week1/intent_records.jsonl",
    "week1/intent_records_violated.jsonl",
    "week2/verdicts.jsonl",
    "week2/verdicts_violated.jsonl",
    "week3/extractions.jsonl",
    "week3/extractions_violated.jsonl",
    "week4/lineage_snapshots.jsonl",
    "week4/lineage_snapshots_violated.jsonl",
    "week5/events.jsonl",
    "week5/events_violated.jsonl",
    "week5/schemas/events/*.json",
    "traces/runs.jsonl",
    "traces/runs_violated.jsonl",
    "quarantine/quarantine.jsonl",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cleanup outputs/ by keeping only files used by the real+violated "
            "Week7 flow. Dry-run by default."
        )
    )
    parser.add_argument(
        "--outputs-dir",
        default="outputs",
        help="Root outputs directory to clean.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete files and empty directories. Without this flag, only prints planned removals.",
    )
    return parser.parse_args()


def should_keep(relative_path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(relative_path, pattern) for pattern in patterns)


def remove_empty_dirs(root: Path) -> list[str]:
    removed: list[str] = []
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_dir() and not any(path.iterdir()):
            path.rmdir()
            removed.append(str(path.relative_to(root)))
    return removed


def main() -> int:
    args = parse_args()
    outputs_root = Path(args.outputs_dir)
    if not outputs_root.exists():
        raise FileNotFoundError(f"outputs directory not found: {outputs_root}")

    keep_patterns = list(DEFAULT_KEEP_PATTERNS)
    files = sorted(path for path in outputs_root.rglob("*") if path.is_file())
    to_remove: list[Path] = []
    kept: list[str] = []
    for path in files:
        relative = str(path.relative_to(outputs_root))
        if should_keep(relative, keep_patterns):
            kept.append(relative)
            continue
        to_remove.append(path)

    removed_files: list[str] = []
    removed_dirs: list[str] = []
    if args.apply:
        for path in to_remove:
            path.unlink()
            removed_files.append(str(path.relative_to(outputs_root)))
        removed_dirs = remove_empty_dirs(outputs_root)

    summary = {
        "mode": "apply" if args.apply else "dry_run",
        "outputs_dir": str(outputs_root),
        "kept_count": len(kept),
        "remove_count": len(to_remove),
        "files_to_remove": [str(path.relative_to(outputs_root)) for path in to_remove],
        "removed_files": removed_files,
        "removed_empty_dirs": removed_dirs,
        "keep_patterns": keep_patterns,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
