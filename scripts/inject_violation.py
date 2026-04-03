from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from simulators import apply_violations_to_current_outputs
from simulators.common import ViolationSpec, canonical_system_name, default_mode_for, group_violations_by_system, load_scenario


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inject one or more contract violations into the current simulator outputs.")
    parser.add_argument("--system", help="System to mutate, for example week3 or traces.")
    parser.add_argument("--type", dest="violation_type", help="Violation type to inject.")
    parser.add_argument("--mode", help="Violation targeting mode: first_record, last_record, random_record, all_records.")
    parser.add_argument("--scenario", help="Optional scenario file containing one or more violations.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic seed used by random targeting modes.")
    return parser.parse_args()


def build_single_violation(args: argparse.Namespace) -> ViolationSpec:
    if not args.system or not args.violation_type:
        raise ValueError("--system and --type are required when --scenario is not provided")
    system = canonical_system_name(args.system)
    mode = args.mode or default_mode_for(system, args.violation_type)
    return ViolationSpec(system=system, type=args.violation_type, mode=mode)


def main() -> int:
    args = parse_args()
    if args.scenario:
        scenario = load_scenario(args.scenario)
        if not scenario.violations:
            raise ValueError("scenario does not declare any violations")
        violations = list(scenario.violations)
        seed = scenario.seed
    else:
        violations = [build_single_violation(args)]
        seed = args.seed
    grouped = group_violations_by_system(violations)
    summary = {
        "mutated_systems": apply_violations_to_current_outputs(grouped, seed),
        "violations": [violation.to_summary() for violation in violations],
        "seed": seed,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

