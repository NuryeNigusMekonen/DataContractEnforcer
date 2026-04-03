from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.common import utc_now

try:
    from backend.services.common import OUTPUTS_DIR
    from backend.services.validation_service import get_all_validation_targets, run_validation_batch
except ModuleNotFoundError:
    from services.common import OUTPUTS_DIR
    from services.validation_service import get_all_validation_targets, run_validation_batch


WATCH_DIRECTORIES = {
    "week1": OUTPUTS_DIR / "week1",
    "week2": OUTPUTS_DIR / "week2",
    "week3": OUTPUTS_DIR / "week3",
    "week4": OUTPUTS_DIR / "week4",
    "week5": OUTPUTS_DIR / "week5",
    "traces": OUTPUTS_DIR / "traces",
}


class LiveValidationWatcher:
    def __init__(self, poll_interval_seconds: float = 3.0) -> None:
        self.poll_interval_seconds = poll_interval_seconds
        self._state_lock = threading.RLock()
        self._worker_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._signatures: dict[str, str] = {}
        self._state: dict[str, Any] = {
            "status": "idle",
            "busy": False,
            "poll_interval_seconds": poll_interval_seconds,
            "last_scan_at": None,
            "last_event_at": None,
            "last_completed_at": None,
            "last_reason": "not_started",
            "updated_week_keys": [],
            "validation_count": 0,
            "last_error": None,
        }

    def start(self) -> None:
        with self._state_lock:
            if self._started:
                return
            self._signatures = self._snapshot_signatures()
            self._started = True
            self._thread = threading.Thread(target=self._run_loop, name="live-validation-watcher", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def snapshot_state(self) -> dict[str, Any]:
        with self._state_lock:
            return dict(self._state)

    def force_validate(self, week_keys: list[str], *, reason: str) -> dict[str, Any]:
        return self._run_validation(week_keys, reason=reason)

    def force_validate_all(self, *, reason: str) -> dict[str, Any]:
        return self._run_validation([target["key"] for target in get_all_validation_targets()], reason=reason)

    def sync_snapshot(self) -> None:
        with self._state_lock:
            self._signatures = self._snapshot_signatures()

    def _run_loop(self) -> None:
        self._run_validation([target["key"] for target in get_all_validation_targets()], reason="startup_sync")
        while not self._stop_event.wait(self.poll_interval_seconds):
            with self._state_lock:
                self._state["last_scan_at"] = utc_now()
            current_signatures = self._snapshot_signatures()
            changed_week_keys = [
                key for key, signature in current_signatures.items()
                if self._signatures.get(key) != signature
            ]
            if not changed_week_keys:
                continue
            self._signatures = current_signatures
            if changed_week_keys:
                self._run_validation(changed_week_keys, reason="outputs_changed")

    def _run_validation(self, week_keys: list[str], *, reason: str) -> dict[str, Any]:
        unique_week_keys = list(dict.fromkeys(week_keys))
        with self._worker_lock:
            with self._state_lock:
                self._state.update(
                    {
                        "status": "validating",
                        "busy": True,
                        "last_reason": reason,
                        "updated_week_keys": unique_week_keys,
                        "last_event_at": utc_now(),
                        "last_error": None,
                    }
                )
            try:
                result = run_validation_batch(unique_week_keys)
            except Exception as exc:
                with self._state_lock:
                    self._state.update(
                        {
                            "status": "error",
                            "busy": False,
                            "last_error": str(exc),
                            "last_completed_at": utc_now(),
                        }
                    )
                raise

            with self._state_lock:
                self._state.update(
                    {
                        "status": "watching",
                        "busy": False,
                        "validation_count": int(self._state.get("validation_count", 0)) + len(unique_week_keys),
                        "last_completed_at": result.get("completed_at"),
                    }
                )
            return result

    def _snapshot_signatures(self) -> dict[str, str]:
        signatures: dict[str, str] = {}
        for key, directory in WATCH_DIRECTORIES.items():
            entries: list[str] = []
            if directory.exists():
                for path in sorted(directory.rglob("*")):
                    if not path.is_file():
                        continue
                    stat = path.stat()
                    entries.append(f"{path.relative_to(OUTPUTS_DIR)}:{stat.st_mtime_ns}:{stat.st_size}")
            signatures[key] = "|".join(entries)
        return signatures


_WATCHER: LiveValidationWatcher | None = None
_WATCHER_LOCK = threading.Lock()


def get_watcher() -> LiveValidationWatcher:
    global _WATCHER
    with _WATCHER_LOCK:
        if _WATCHER is None:
            _WATCHER = LiveValidationWatcher()
        return _WATCHER
