from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simulators import generate_scenario_outputs, scenario_summary, write_generated_outputs
from simulators.common import clear_output_files, load_scenario


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate simulated week1-week5 and trace outputs from a scenario.")
    parser.add_argument("--scenario", required=True, help="Path to a scenario YAML file.")
    parser.add_argument("--clear-existing", action="store_true", help="Delete existing simulator outputs before writing new ones.")
    return parser.parse_args()


def generate_all_outputs_from_scenario_path(scenario_path: str | Path, *, clear_existing: bool = False) -> dict[str, object]:
    scenario = load_scenario(scenario_path)
    removed = clear_output_files() if clear_existing or scenario.clear_existing else []
    records_by_system = generate_scenario_outputs(scenario)
    written = write_generated_outputs(records_by_system)
    summary = scenario_summary(scenario, records_by_system)
    summary["cleared"] = removed
    summary["written_files"] = written
    return summary


def main() -> int:
    args = parse_args()
    summary = generate_all_outputs_from_scenario_path(args.scenario, clear_existing=args.clear_existing)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

