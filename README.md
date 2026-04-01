# Data Contract Enforcer

This repository contains a runnable week 7 implementation of the TRP1 Data Contract Enforcer. The runtime `outputs/` tree is rebuilt from the canonical artifacts under `artifacts/week1` through `artifacts/week7`, and the trace export under `outputs/traces` is a realistic local sample with clean and violated runs for the AI-contract flow.

## Prerequisites

1. Install `uv` if needed:

```bash
uv --version
```

2. Confirm Python 3.11+:

```bash
python3 --version
```

3. Sync the project environment from `pyproject.toml` and `uv.lock`:

```bash
uv sync
```

4. Rebuild the canonical runtime views from the source artifacts:

```bash
python3 scripts/sync_real_week_artifacts.py
```

Legacy fallback:

```bash
python3 -m pip install -r requirements.txt
```

## How To Run

1. Generate the clean contracts:

```bash
python3 contracts/generator.py --source outputs/week1/intent_records.jsonl --output generated_contracts
python3 contracts/generator.py --source outputs/week2/verdicts.jsonl --output generated_contracts
python3 contracts/generator.py --source outputs/week3/extractions.jsonl --output generated_contracts
python3 contracts/generator.py --source outputs/week4/lineage_snapshots.jsonl --output generated_contracts
python3 contracts/generator.py --source outputs/week5/events.jsonl --output generated_contracts
python3 contracts/generator.py --source outputs/traces/runs.jsonl --output generated_contracts
```

Expected output:
`generated_contracts/week3-document-refinery-extractions.yaml`
`generated_contracts/week3_extractions.yaml`
`generated_contracts/week3-document-refinery-extractions_dbt.yml`
`generated_contracts/week5-event-records.yaml`

2. Run validation on clean data:

```bash
python3 contracts/runner.py --contract generated_contracts/week1-intent-records.yaml --data outputs/week1/intent_records.jsonl --output validation_reports/clean_week1.json
python3 contracts/runner.py --contract generated_contracts/week2-verdict-records.yaml --data outputs/week2/verdicts.jsonl --output validation_reports/clean_week2.json
python3 contracts/runner.py --contract generated_contracts/week3-document-refinery-extractions.yaml --data outputs/week3/extractions.jsonl --output validation_reports/clean_week3.json
python3 contracts/runner.py --contract generated_contracts/week4-lineage-snapshots.yaml --data outputs/week4/lineage_snapshots.jsonl --output validation_reports/clean_week4.json
python3 contracts/runner.py --contract generated_contracts/week5-event-records.yaml --data outputs/week5/events.jsonl --output validation_reports/clean_week5.json
python3 contracts/runner.py --contract generated_contracts/langsmith-trace-records.yaml --data outputs/traces/runs.jsonl --output validation_reports/clean_traces.json
```

Expected output:
Each JSON report is written to `validation_reports/`.
The clean week 1 through week 5 data and the clean trace export should pass all contract checks.

3. Inject the known week 3 and trace violations:

```bash
python3 create_violation.py
python3 contracts/runner.py --contract generated_contracts/week3-document-refinery-extractions.yaml --data outputs/week3/extractions_violated.jsonl --output validation_reports/violated_week3.json
python3 contracts/runner.py --contract generated_contracts/langsmith-trace-records.yaml --data outputs/traces/runs_violated.jsonl --output validation_reports/violated_traces.json
```

Expected output:
`outputs/week3/extractions_violated.jsonl`
`outputs/traces/runs_violated.jsonl`
`validation_reports/violated_week3.json`
`validation_reports/violated_traces.json`

The violated run should include:
`extracted_facts.confidence.range` as `FAIL`
`extracted_facts.confidence.drift` as `FAIL`

4. Attribute the violations:

```bash
python3 contracts/attributor.py --violation validation_reports/violated_week3.json --lineage outputs/week4/lineage_snapshots.jsonl --contract generated_contracts/week3-document-refinery-extractions.yaml --output violation_log/violations.jsonl
python3 contracts/attributor.py --violation validation_reports/violated_traces.json --lineage outputs/week4/lineage_snapshots.jsonl --contract generated_contracts/langsmith-trace-records.yaml --output violation_log/violations.jsonl
```

Expected output:
`violation_log/violations.jsonl` with ranked blame-chain entries and a documented injected-violation comment at the top of the file

5. Generate a second schema snapshot from the violated data, then diff schema evolution:

```bash
python3 contracts/generator.py --source outputs/week3/extractions_violated.jsonl --contract-id week3-document-refinery-extractions --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts
python3 contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --output validation_reports/schema_evolution.json
```

Expected output:
`validation_reports/schema_evolution.json`

The diff should classify the confidence maximum change as `BREAKING`.

6. Run the AI-specific checks:

```bash
python3 contracts/ai_extensions.py --mode all --extractions outputs/week3/extractions.jsonl --verdicts outputs/week2/verdicts.jsonl --output validation_reports/ai_extensions.json
```

Expected output:
`validation_reports/ai_extensions.json`
`validation_reports/ai_metrics.json`

Current behavior with the real week 3 extraction set:
`embedding_drift.status` should be `PASS`
`prompt_input_validation.status` should be `WARN` because some refinery documents do not yield usable text previews and are intentionally quarantined

7. Generate the Enforcer Report:

```bash
python3 contracts/report_generator.py
```

Expected output:
`enforcer_report/report_data.json`
`enforcer_report/report_YYYYMMDD.pdf`

After running all steps, open `enforcer_report/report_data.json` and verify:
`data_health_score` is between 0 and 100
`violation_count` is at least 3 after the injected and AI-warning flows

## Repo Notes

- `artifacts/week1` through `artifacts/week7` are the canonical human-facing organization of the repository. Each week's source artifacts live under its own folder.
- `scripts/sync_real_week_artifacts.py` rebuilds the canonical `outputs/week*` JSONL files from the real week 1 through week 5 artifacts.
- `contracts/` contains the generator, runner, attributor, schema analyzer, AI extensions, and report generator.
- `outputs/`, `.orchestration/`, `.refinery/`, `.cartography/`, `rubric/`, `rubrics/`, `schemas/events/`, `generated_contracts/`, `validation_reports/`, `schema_snapshots/`, `enforcer_report/`, and `violation_log/` are preserved as runtime paths via symlinks so the week 7 tooling and commands keep working.
- This directory is now a Git repository with commit history available, so the attributor can attach real commit hashes for tracked source files.
