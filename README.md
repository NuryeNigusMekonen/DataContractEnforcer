# Data Contract Enforcer

This repository contains a runnable implementation of the Data Contract Enforcer. The runtime `outputs/` tree is rebuilt from the canonical artifacts under `artifacts/week1` through `artifacts/week7`, and the trace export under `outputs/traces` is a realistic local sample with clean and violated runs for the AI-contract flow.

## Simulation Layer

Week 7 now includes a deterministic simulation layer under `simulators/` so you can regenerate contract-facing producer outputs for Weeks 1 to 5 plus LangSmith-style traces without rerunning the original upstream projects.

The simulator writes these canonical inputs:

- `outputs/week1/intent_records.jsonl`
- `outputs/week2/verdicts.jsonl`
- `outputs/week3/extractions.jsonl`
- `outputs/week4/lineage_snapshots.jsonl`
- `outputs/week5/events.jsonl`
- `outputs/traces/runs.jsonl`

Scenario files live in `test_data/scenarios/` and control counts, enabled systems, deterministic seed, and injected violations.

## Prerequisites

1. Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

2. Confirm Python 3.11+:

```bash
python3 --version
```

3. Rebuild the canonical runtime views from the source artifacts:

```bash
python3 scripts/sync_real_week_artifacts.py
```

If you want to work entirely from simulated producer outputs instead, use the simulator commands below instead of syncing the original artifacts.

## Generate Simulated Outputs

Generate the healthy baseline:

```bash
python scripts/generate_all_outputs.py --scenario test_data/scenarios/healthy.yaml --clear-existing
```

Inject one violation into the current outputs:

```bash
python scripts/inject_violation.py --system week3 --type confidence_scale_break
python scripts/inject_violation.py --system week5 --type timestamp_break
```

Apply a multi-system broken scenario in place:

```bash
python scripts/inject_violation.py --scenario test_data/scenarios/mixed_breaks.yaml
```

Reset the simulator outputs:

```bash
python scripts/reset_outputs.py
python scripts/reset_outputs.py --regenerate-healthy
```

Included scenarios:

- `healthy.yaml`
- `week2_enum_break.yaml`
- `week3_confidence_scale_break.yaml`
- `week4_missing_node_ref.yaml`
- `week5_timestamp_break.yaml`
- `mixed_breaks.yaml`

## How To Run

1. Bootstrap ContractRegistry (required by updated manual):

```bash
cat contract_registry/subscriptions.yaml | rg subscriber_id
```

Expected output:
At least 4 subscriber entries (this repo has 6).

2. Generate contracts (minimum Week 3 and Week 5):

```bash
python3 contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/

python3 contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/
```

Expected output:
`generated_contracts/week3_extractions.yaml`
`generated_contracts/week3_extractions_dbt.yml`
`generated_contracts/week5_events.yaml`
`generated_contracts/week5_events_dbt.yml`
`schema_snapshots/week3-document-refinery-extractions/<timestamp>.yaml`

3. Validate clean baseline in AUDIT mode:

```bash
python3 contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode AUDIT \
  --output validation_reports/clean.json
```

Expected output:
`validation_reports/clean.json` with all checks passing on clean data.

4. Inject canonical violation and validate in ENFORCE mode:

```bash
python3 create_violation.py
python3 contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions_violated.jsonl \
  --mode ENFORCE \
  --output validation_reports/violated.json || true
```

Expected output:
`outputs/week3/extractions_violated.jsonl`
`validation_reports/violated.json`
The ENFORCE command should block (`exit code 2`) because violations were detected.

5. Attribute violation (Tier 1 transitive blast radius):

```bash
python3 contracts/attributor.py \
  --violation validation_reports/violated.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/violations.jsonl
```

Expected output:
`violation_log/violations.jsonl` with ranked blame-chain entries and full transitive downstream impact across local contracts and subscribers.

6. Create second snapshot from violated data and run schema evolution:

```bash
python3 contracts/generator.py \
  --source outputs/week3/extractions_violated.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/

python3 contracts/schema_analyzer.py --contract-id week3-document-refinery-extractions --output validation_reports/schema_evolution.json
```

Expected output:
`validation_reports/schema_evolution.json`
with at least one breaking change from the confidence scale shift.

7. Run a read-only what-if simulation before accepting a producer change:

```bash
python3 contracts/what_if.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --change-spec test_data/changes/week3_confidence_scale_change.json \
  --adapter-config contract_registry/adapters.yaml \
  --output validation_reports/what_if_week3_confidence.json
```

Expected output:
`validation_reports/what_if_week3_confidence.json`

The what-if report does not overwrite `outputs/`, `generated_contracts/`, baselines, or schema snapshots. It simulates the proposal in memory, validates the raw changed payload, reruns validation with adapter/upcasting rules when available, and enriches the direct blast radius from the ContractRegistry with transitive lineage context from Week 4.

Additional committed change specs:
`test_data/changes/week2_verdict_enum_addition.json`
`test_data/changes/week5_required_regulatory_basis.json`

8. Run AI-specific checks:

```bash
python3 contracts/ai_extensions.py --mode all --extractions outputs/week3/extractions.jsonl --verdicts outputs/week2/verdicts.jsonl --output validation_reports/ai_extensions.json
```

Expected output:
`validation_reports/ai_extensions.json`
`validation_reports/ai_metrics.json`

Current behavior with the real week 3 extraction set:
`embedding_drift.status` should be `PASS`
`prompt_input_validation.status` should be `WARN` because some refinery documents do not yield usable text previews and are intentionally quarantined

9. Generate Enforcer report artifact:

```bash
python3 contracts/report_generator.py
```

Expected output:
`enforcer_report/report_data.json`
`enforcer_report/report_YYYYMMDD.pdf`

If `validation_reports/what_if_*.json` files are present, the report now includes a short what-if simulation summary alongside weekly violations and schema changes.

After running all steps, open `enforcer_report/report_data.json` and verify:
`data_health_score` is between 0 and 100
`violation_count` is at least 3 after the injected and AI-warning flows

## Repo Notes

- `artifacts/week1` through `artifacts/week7` are the canonical human-facing organization of the repository. Each week's source artifacts live under its own folder.
- `scripts/sync_real_week_artifacts.py` rebuilds the canonical `outputs/week*` JSONL files from the real week 1 through week 5 artifacts.
- `scripts/generate_all_outputs.py`, `scripts/inject_violation.py`, and `scripts/reset_outputs.py` manage the deterministic simulation layer for repeatable Week 7 demos.
- `contracts/` contains the generator, runner, attributor, schema analyzer, AI extensions, and report generator.
- `outputs/`, `.orchestration/`, `.refinery/`, `.cartography/`, `rubric/`, `rubrics/`, `schemas/events/`, `generated_contracts/`, `validation_reports/`, `schema_snapshots/`, `enforcer_report/`, and `violation_log/` are preserved as runtime paths via symlinks so the week 7 tooling and commands keep working.
- This directory is now a Git repository with commit history available, so the attributor can attach real commit hashes for tracked source files.

## Simulation Demo Flow

Healthy run:

```bash
python scripts/generate_all_outputs.py --scenario test_data/scenarios/healthy.yaml --clear-existing

python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/

python contracts/generator.py \
  --source outputs/week5/events.jsonl \
  --contract-id week5-event-records \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output generated_contracts/

python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode ENFORCE \
  --output validation_reports/simulated_week3_clean.json
```

Expected result:
`validation_reports/simulated_week3_clean.json` should report all checks passing on the healthy simulated baseline.

Violation run:

```bash
python scripts/inject_violation.py --system week3 --type confidence_scale_break

python contracts/runner.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --mode ENFORCE \
  --output validation_reports/simulated_week3_broken.json || true

python contracts/attributor.py \
  --violation validation_reports/simulated_week3_broken.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --registry contract_registry/subscriptions.yaml \
  --output violation_log/simulated_week3_violations.jsonl

python contracts/what_if.py \
  --contract generated_contracts/week3-document-refinery-extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --change-spec test_data/changes/week3_confidence_scale_change.json \
  --adapter-config contract_registry/adapters.yaml \
  --output validation_reports/what_if_week3_confidence.json
```

Expected result:
The raw confidence-scale change is classified as breaking, the attributor can trace the blast radius through Week 4 lineage and ContractRegistry subscribers, and the what-if report shows whether the configured adapter can recover the payload in memory.

## Live Dashboard

The repository now includes a file-backed full-stack dashboard:

- `backend/` serves a Flask API on port `5000`
- `backend/watcher.py` polls `outputs/week1` through `outputs/week5` plus `outputs/traces` and reruns validation when files change
- `frontend/` serves a React dashboard on port `3000`
- the UI reads only from the existing Week 7 JSON and JSONL artifacts under `validation_reports/`, `violation_log/`, `schema_snapshots/`, `enforcer_report/`, `outputs/`, and `runs.jsonl`

### Run the backend

Install Python dependencies if needed:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Start the Flask API:

```bash
python3 backend/app.py
```

When Flask starts, the watcher performs an initial live validation sync and then polls the output folders every 3 seconds. Any file change under the watched output directories triggers a validation refresh and a live rewrite of `validation_reports/live_*.json` plus `violation_log/live_violations.jsonl`.

Available endpoints:

- `GET /api/summary`
- `GET /api/weeks`
- `GET /api/violations`
- `GET /api/blame`
- `GET /api/blast-radius`
- `GET /api/schema-evolution`
- `GET /api/what-if`
- `GET /api/timeline`
- `POST /api/what-if/run`
- `POST /api/regenerate`

### Run the frontend

Install frontend dependencies:

```bash
cd frontend
npm install
```

Start the React app:

```bash
npm run dev
```

The Vite dev server runs on `http://localhost:3000` and proxies `/api/*` requests to Flask on `http://localhost:5000`.

### Open the dashboard

After both servers are running, open:

```text
http://localhost:3000
```

The dashboard auto-refreshes every 5 seconds and is designed to make the demo flow obvious:

1. Healthy artifacts keep the week cards green and the top summary stable.
2. Inject a failure with the existing Week 7 scripts or contract commands.
3. The UI updates automatically so the affected week turns red or yellow, the new violation is highlighted, the blast radius changes, blame rows appear, and the what-if panel shows whether adapter recovery is possible.

### Live controls

The dashboard includes two direct controls:

- `Run Simulation` posts a selected change spec to `/api/what-if/run`, saves the latest result to `validation_reports/what_if_latest.json`, and refreshes the What-If panel immediately.
- `Regenerate Outputs` posts a selected scenario to `/api/regenerate`, runs the simulator, and then forces a live validation refresh so the dashboard reflects the new output state without a manual backend restart.
