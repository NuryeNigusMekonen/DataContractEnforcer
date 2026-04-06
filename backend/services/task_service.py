from __future__ import annotations

import threading
import uuid
from typing import Any, Callable

try:
    from backend.watcher import get_watcher
except ModuleNotFoundError:
    from watcher import get_watcher

from contracts.common import utc_now

JobCallable = Callable[[], dict[str, Any]]


def _publish_week7_dashboard(*, mode: str, reason: str, skip_sync: bool) -> dict[str, Any]:
    try:
        from backend.services.validation_service import get_all_validation_targets
    except ModuleNotFoundError:
        from services.validation_service import get_all_validation_targets

    from scripts.run_week7_e2e import run_mode

    summary = run_mode(mode, skip_sync=skip_sync)
    watcher = get_watcher()
    watcher.sync_snapshot()

    updated_week_keys = [target["key"] for target in get_all_validation_targets()]
    validation = summary.get(mode, {}) if isinstance(summary.get(mode), dict) else {}
    watcher.record_validation_result(
        reason=reason,
        updated_week_keys=updated_week_keys,
        completed_at=validation.get("completed_at") or summary.get("run_at"),
    )
    return {
        "summary": summary,
        "validation": validation,
    }


def execute_publish_pipeline(*, mode: str) -> dict[str, Any]:
    published = _publish_week7_dashboard(mode=mode, reason=f"publish:{mode}", skip_sync=False)
    payload = {
        "mode": mode,
        "validation": published["validation"],
        "summary": published["summary"],
    }
    if mode == "violated":
        payload["injected"] = published["summary"].get("injected", [])
    return payload


def execute_regenerate_pipeline(
    regenerate_outputs_fn: Callable[[str], dict[str, Any]],
    *,
    scenario: str,
) -> dict[str, Any]:
    generation = regenerate_outputs_fn(scenario)
    published = _publish_week7_dashboard(mode="real", reason=f"regenerate:{scenario}", skip_sync=True)
    return {
        "generation": generation,
        "validation": published["validation"],
        "summary": published["summary"],
    }


def execute_inject_pipeline() -> dict[str, Any]:
    published = _publish_week7_dashboard(mode="violated", reason="inject:violations", skip_sync=False)
    return {
        "injected": published["summary"].get("injected", []),
        "validation": published["validation"],
        "summary": published["summary"],
    }


class JobManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._jobs: dict[str, dict[str, Any]] = {}

    def create_job(self, *, kind: str, description: str, target: JobCallable) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        job = {
            "job_id": job_id,
            "kind": kind,
            "description": description,
            "status": "queued",
            "created_at": utc_now(),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "result": None,
        }
        with self._lock:
            self._jobs[job_id] = job

        worker = threading.Thread(
            target=self._run_job,
            args=(job_id, target),
            name=f"dashboard-job-{kind}",
            daemon=True,
        )
        worker.start()
        return dict(job)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return dict(job)

    def _run_job(self, job_id: str, target: JobCallable) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job["status"] = "running"
            job["started_at"] = utc_now()

        try:
            result = target()
        except Exception as exc:
            with self._lock:
                job = self._jobs[job_id]
                job["status"] = "failed"
                job["completed_at"] = utc_now()
                job["error"] = str(exc)
            return

        with self._lock:
            job = self._jobs[job_id]
            job["status"] = "completed"
            job["completed_at"] = utc_now()
            job["result"] = result


_JOB_MANAGER: JobManager | None = None
_JOB_MANAGER_LOCK = threading.Lock()


def get_job_manager() -> JobManager:
    global _JOB_MANAGER
    with _JOB_MANAGER_LOCK:
        if _JOB_MANAGER is None:
            _JOB_MANAGER = JobManager()
        return _JOB_MANAGER


def submit_what_if_job(run_what_if_fn: Callable[[str], dict[str, Any]], reference: str) -> dict[str, Any]:
    return get_job_manager().create_job(
        kind="what_if",
        description=f"What-if simulation for {reference}",
        target=lambda: run_what_if_fn(reference),
    )


def submit_regenerate_job(
    regenerate_outputs_fn: Callable[[str], dict[str, Any]],
    *,
    scenario: str,
) -> dict[str, Any]:
    return get_job_manager().create_job(
        kind="regenerate",
        description=f"Regenerate outputs for {scenario}",
        target=lambda: execute_regenerate_pipeline(regenerate_outputs_fn, scenario=scenario),
    )


def submit_inject_job() -> dict[str, Any]:
    return get_job_manager().create_job(
        kind="inject_violations",
        description="Inject violations from current outputs",
        target=execute_inject_pipeline,
    )


def submit_publish_job(*, mode: str) -> dict[str, Any]:
    return get_job_manager().create_job(
        kind=f"publish_{mode}",
        description=f"Publish dashboard with Week 7 CLI {mode} flow",
        target=lambda: execute_publish_pipeline(mode=mode),
    )
