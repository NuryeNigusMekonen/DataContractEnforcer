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
    def execute() -> dict[str, Any]:
        generation = regenerate_outputs_fn(scenario)
        watcher = get_watcher()
        watcher.sync_snapshot()
        validation = watcher.force_validate_all(reason=f"regenerate:{scenario}")
        return {
            "generation": generation,
            "validation": validation,
        }

    return get_job_manager().create_job(
        kind="regenerate",
        description=f"Regenerate outputs for {scenario}",
        target=execute,
    )
