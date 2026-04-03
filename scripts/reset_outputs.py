from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_all_outputs import generate_all_outputs_from_scenario_path
from simulators.common import clear_output_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete simulated JSONL outputs and optionally regenerate the healthy baseline.")
    parser.add_argument("--regenerate-healthy", action="store_true", help="Rebuild the healthy scenario after clearing outputs.")
    parser.add_argument(
        "--scenario",
        default="test_data/scenarios/healthy.yaml",
        help="Scenario used when --regenerate-healthy is passed.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    removed = clear_output_files()
    summary: dict[str, object] = {"removed": removed}
    if args.regenerate_healthy:
        summary["regenerated"] = generate_all_outputs_from_scenario_path(args.scenario, clear_existing=False)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
