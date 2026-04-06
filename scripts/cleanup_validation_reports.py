from __future__ import annotations

import argparse
import fnmatch
import json
import shutil
from pathlib import Path


DEFAULT_KEEP_PATTERNS = [
    "week*.json",
    "traces.json",
    "schema_evolution.json",
    "ai_extensions.json",
    "ai_metrics.json",
    "what_if_*.json",
    "run_summary.json",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cleanup validation_reports by keeping only selected filename patterns. "
            "Runs in dry-run mode unless --apply is provided."
        )
    )
    parser.add_argument(
        "--reports-dir",
        default="validation_reports",
        help="Directory that contains validation report JSON files.",
    )
    parser.add_argument(
        "--keep",
        action="append",
        default=[],
        help=(
            "Extra filename pattern to keep (can be provided multiple times). "
            "Defaults keep: week*.json, traces.json, schema_evolution.json, ai_extensions.json, "
            "ai_metrics.json, what_if_*.json, run_summary.json"
        ),
    )
    parser.add_argument(
        "--archive-dir",
        default="",
        help="Optional archive directory. If set, removed files are moved here instead of deleted.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply cleanup changes. Without this flag, only prints what would change.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicit dry-run flag (same behavior as omitting --apply).",
    )
    return parser.parse_args()


def matches_any(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def main() -> int:
    args = parse_args()
    reports_dir = Path(args.reports_dir)
    if not reports_dir.exists() or not reports_dir.is_dir():
        raise SystemExit(f"reports dir not found: {reports_dir}")

    keep_patterns = [*DEFAULT_KEEP_PATTERNS, *args.keep]
    archive_dir = Path(args.archive_dir) if args.archive_dir else None
    if archive_dir is not None and args.apply:
        archive_dir.mkdir(parents=True, exist_ok=True)

    kept: list[str] = []
    removed: list[str] = []
    for path in sorted(reports_dir.iterdir()):
        if not path.is_file():
            continue
        if matches_any(path.name, keep_patterns):
            kept.append(path.name)
            continue
        removed.append(path.name)
        if not args.apply:
            continue
        if archive_dir is None:
            path.unlink(missing_ok=True)
        else:
            target = archive_dir / path.name
            if target.exists():
                target = archive_dir / f"{path.stem}__dup{path.suffix}"
            shutil.move(path.as_posix(), target.as_posix())

    mode = "apply" if args.apply else "dry_run"
    summary = {
        "mode": mode,
        "reports_dir": str(reports_dir),
        "keep_patterns": keep_patterns,
        "archive_dir": str(archive_dir) if archive_dir else None,
        "kept_count": len(kept),
        "removed_count": len(removed),
        "kept": kept,
        "removed": removed,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
