from __future__ import annotations

import sys
from pathlib import Path

from flask import Flask, jsonify, request
from flask_cors import CORS

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from backend.watcher import get_watcher
    from backend.services.schema_service import get_schema_evolution
    from backend.services.summary_service import get_kpi_summary, get_summary, get_weeks_status
    from backend.services.task_service import get_job_manager, submit_regenerate_job, submit_what_if_job
    from backend.services.timeline_service import get_timeline, get_timeline_panel
    from backend.services.validation_service import available_scenarios, get_latest_validations, regenerate_outputs
    from backend.services.violation_service import (
        get_blast_radius,
        get_blast_radius_summary,
        get_blame_chains,
        get_blame_top,
        get_top_incident,
        get_violations,
    )
    from backend.services.whatif_service import get_what_if, run_what_if
except ModuleNotFoundError:
    from watcher import get_watcher
    from services.schema_service import get_schema_evolution
    from services.summary_service import get_kpi_summary, get_summary, get_weeks_status
    from services.task_service import get_job_manager, submit_regenerate_job, submit_what_if_job
    from services.timeline_service import get_timeline, get_timeline_panel
    from services.validation_service import available_scenarios, get_latest_validations, regenerate_outputs
    from services.violation_service import (
        get_blast_radius,
        get_blast_radius_summary,
        get_blame_chains,
        get_blame_top,
        get_top_incident,
        get_violations,
    )
    from services.whatif_service import get_what_if, run_what_if


app = Flask(__name__)
CORS(app)
_watcher_started = False


def ensure_watcher_started() -> None:
    global _watcher_started
    if _watcher_started:
        return
    get_watcher().start()
    _watcher_started = True


@app.before_request
def start_runtime_services() -> None:
    ensure_watcher_started()


@app.get("/api/summary")
def summary() -> tuple:
    return jsonify(get_summary()), 200


@app.get("/api/kpi")
def kpi() -> tuple:
    return jsonify(get_kpi_summary()), 200


@app.get("/api/weeks")
def weeks() -> tuple:
    return jsonify(get_weeks_status()), 200


@app.get("/api/validations")
def validations() -> tuple:
    return jsonify(get_latest_validations()), 200


@app.get("/api/violations")
def violations() -> tuple:
    limit = request.args.get("limit", type=int)
    severity = request.args.get("severity", default=None, type=str)
    search = request.args.get("search", default=None, type=str)
    return jsonify(get_violations(limit=limit, severity=severity, search=search)), 200


@app.get("/api/incidents")
def incidents() -> tuple:
    return jsonify(get_top_incident()), 200


@app.get("/api/blame")
def blame() -> tuple:
    return jsonify(get_blame_chains()), 200


@app.get("/api/blame/top")
def blame_top() -> tuple:
    limit = request.args.get("limit", default=3, type=int)
    return jsonify(get_blame_top(limit=limit)), 200


@app.get("/api/blast-radius")
def blast_radius() -> tuple:
    return jsonify(get_blast_radius()), 200


@app.get("/api/blast-radius/summary")
def blast_radius_summary() -> tuple:
    limit = request.args.get("limit", default=5, type=int)
    return jsonify(get_blast_radius_summary(limit=limit)), 200


@app.get("/api/schema-evolution")
def schema_evolution() -> tuple:
    return jsonify(get_schema_evolution()), 200


@app.get("/api/what-if")
def what_if() -> tuple:
    return jsonify(get_what_if()), 200


@app.get("/api/what-if/latest")
def what_if_latest() -> tuple:
    return jsonify(get_what_if()), 200


@app.get("/api/timeline")
def timeline() -> tuple:
    limit = request.args.get("limit", default=8, type=int)
    return jsonify(get_timeline_panel(limit=limit)), 200


@app.get("/api/health")
def health() -> tuple:
    return jsonify({"status": "ok", "watcher": get_watcher().snapshot_state()}), 200


@app.get("/api/jobs/<job_id>")
def job_status(job_id: str) -> tuple:
    job = get_job_manager().get_job(job_id)
    if job is None:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job), 200


@app.post("/api/what-if/run")
def run_what_if_endpoint() -> tuple:
    payload = request.get_json(silent=True) or {}
    reference = str(payload.get("change_spec_path") or payload.get("spec_id") or "")
    run_async = bool(payload.get("async", True))
    if not reference:
        return jsonify({"error": "change_spec_path or spec_id is required"}), 400
    if run_async:
        job = submit_what_if_job(run_what_if, reference)
        return jsonify(job), 202
    result = run_what_if(reference)
    return jsonify(result), 200


@app.post("/api/regenerate")
def regenerate() -> tuple:
    payload = request.get_json(silent=True) or {}
    scenario = str(payload.get("scenario") or "test_data/scenarios/healthy.yaml")
    clear_existing = bool(payload.get("clear_existing", True))
    run_async = bool(payload.get("async", True))
    if run_async:
        job = submit_regenerate_job(
            lambda selected_scenario: regenerate_outputs(selected_scenario, clear_existing=clear_existing),
            scenario=scenario,
        )
        job["available_scenarios"] = available_scenarios()
        return jsonify(job), 202
    generation = regenerate_outputs(scenario, clear_existing=clear_existing)
    watcher = get_watcher()
    watcher.sync_snapshot()
    validation = watcher.force_validate_all(reason=f"regenerate:{scenario}")
    return jsonify({"generation": generation, "validation": validation, "available_scenarios": available_scenarios()}), 200


if __name__ == "__main__":
    ensure_watcher_started()
    app.run(host="0.0.0.0", port=5000, debug=True)
