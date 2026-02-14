from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Any, Optional

from .config import load_config
from .pipeline import run_once
from .workers.business_leads import ensure_daily_target_generated


logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AutomationSettings:
    interval_seconds: int = 900
    area: Optional[str] = None
    categories: str = "all"
    areas_file: str = "config/areas.json"
    categories_file: str = "config/categories.json"
    sync_limit: Optional[int] = 100
    rdap_limit: Optional[int] = 5
    rdap_statuses: list[str] = field(default_factory=lambda: ["new", "skipped", "rdap_error", "dns_error"])
    email_limit: Optional[int] = 0
    score_limit: Optional[int] = 0
    platform: str = "csv"
    min_score: Optional[float] = None
    business_score_limit: Optional[int] = 500
    business_platform: str = "csv_business"
    business_min_score: Optional[float] = 40.0
    business_require_unhosted_domain: bool = False
    business_require_contact: bool = True
    business_require_domain_qualification: bool = False
    daily_target_enabled: bool = True
    daily_target_count: int = 100
    daily_target_min_score: float = 40.0
    daily_target_platform_prefix: str = "daily"
    daily_target_require_contact: bool = True
    daily_target_require_domain_qualification: bool = False
    daily_target_require_unhosted_domain: bool = False
    daily_target_allow_recycle: bool = True


class AutomationController:
    def __init__(self) -> None:
        config = load_config()
        self._settings = AutomationSettings(
            interval_seconds=max(getattr(config, "auto_runner_interval_seconds", 900), 30),
            daily_target_enabled=config.auto_daily_target_enabled,
            daily_target_count=max(config.daily_target_count, 1),
            daily_target_min_score=float(config.daily_target_min_score),
            daily_target_platform_prefix=config.daily_target_platform_prefix,
            daily_target_require_contact=config.daily_target_require_contact,
            daily_target_require_domain_qualification=config.daily_target_require_domain_qualification,
            daily_target_require_unhosted_domain=config.daily_target_require_unhosted_domain,
            daily_target_allow_recycle=config.daily_target_allow_recycle,
        )
        self._auto_start = getattr(config, "auto_runner_enabled", False)

        self._thread: Optional[Thread] = None
        self._stop_event = Event()
        self._settings_lock = Lock()
        self._run_lock = Lock()
        self._state_lock = Lock()

        self._last_run_started_at: Optional[str] = None
        self._last_run_finished_at: Optional[str] = None
        self._last_error: Optional[str] = None
        self._last_result: Optional[dict[str, Any]] = None
        self._run_count: int = 0

    @property
    def auto_start_enabled(self) -> bool:
        return self._auto_start

    def _normalized_updates(self, updates: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in updates.items():
            if value is None:
                continue
            if key == "interval_seconds":
                normalized[key] = max(int(value), 30)
            elif key in {"daily_target_count"}:
                normalized[key] = max(int(value), 1)
            elif key in {"daily_target_min_score", "business_min_score", "min_score"}:
                normalized[key] = float(value)
            elif key == "rdap_statuses":
                if isinstance(value, list):
                    normalized[key] = [str(item).strip() for item in value if str(item).strip()]
            else:
                normalized[key] = value
        return normalized

    def update_settings(self, updates: dict[str, Any]) -> None:
        normalized = self._normalized_updates(updates)
        if not normalized:
            return
        with self._settings_lock:
            for key, value in normalized.items():
                if hasattr(self._settings, key):
                    setattr(self._settings, key, value)

    def _snapshot_settings(self) -> AutomationSettings:
        with self._settings_lock:
            return AutomationSettings(**asdict(self._settings))

    def _run_cycle(self, trigger: str) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            return {"trigger": trigger, "busy": True}

        settings = self._snapshot_settings()
        try:
            with self._state_lock:
                self._last_run_started_at = _utc_now()
                self._last_error = None

            pipeline_result = run_once(
                area=settings.area,
                categories=settings.categories,
                areas_file=settings.areas_file,
                categories_file=settings.categories_file,
                sync_limit=settings.sync_limit,
                rdap_limit=settings.rdap_limit,
                rdap_statuses=settings.rdap_statuses,
                email_limit=settings.email_limit,
                score_limit=settings.score_limit,
                platform=settings.platform,
                min_score=settings.min_score,
                business_score_limit=settings.business_score_limit,
                business_platform=settings.business_platform,
                business_min_score=settings.business_min_score,
                business_require_unhosted_domain=settings.business_require_unhosted_domain,
                business_require_contact=settings.business_require_contact,
                business_require_domain_qualification=settings.business_require_domain_qualification,
            )

            daily_result = None
            if settings.daily_target_enabled:
                daily_result = ensure_daily_target_generated(
                    target_count=settings.daily_target_count,
                    min_score=settings.daily_target_min_score,
                    platform_prefix=settings.daily_target_platform_prefix,
                    require_contact=settings.daily_target_require_contact,
                    require_unhosted_domain=settings.daily_target_require_unhosted_domain,
                    require_domain_qualification=settings.daily_target_require_domain_qualification,
                    allow_recycle=settings.daily_target_allow_recycle,
                )

            result = {
                "trigger": trigger,
                "busy": False,
                "pipeline": pipeline_result,
                "daily_target": daily_result,
            }
            with self._state_lock:
                self._last_result = result
                self._last_run_finished_at = _utc_now()
                self._run_count += 1
            return result
        except Exception as exc:
            with self._state_lock:
                self._last_error = str(exc)
                self._last_run_finished_at = _utc_now()
            raise
        finally:
            self._run_lock.release()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_cycle(trigger="scheduled")
            except Exception as e:
                # Error is stored in state; keep loop alive for next interval.
                logger.exception("Error during scheduled run cycle: %s", e)

            wait_seconds = self._snapshot_settings().interval_seconds
            if self._stop_event.wait(wait_seconds):
                break

    def start(self, updates: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        if updates:
            self.update_settings(updates)

        if self.running:
            return self.status()

        self._stop_event = Event()
        self._thread = Thread(target=self._loop, daemon=True, name="domain-lead-auto-runner")
        self._thread.start()
        return self.status()

    def stop(self) -> dict[str, Any]:
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(timeout=5)
        return self.status()

    def run_now(self) -> dict[str, Any]:
        return self._run_cycle(trigger="manual")

    def run_daily_target_now(self) -> dict[str, Any]:
        if not self._run_lock.acquire(blocking=False):
            return {"trigger": "manual_daily_target", "busy": True}

        with self._state_lock:
            self._last_run_started_at = _utc_now()
            self._last_error = None

        settings = self._snapshot_settings()
        try:
            result = ensure_daily_target_generated(
                target_count=settings.daily_target_count,
                min_score=settings.daily_target_min_score,
                platform_prefix=settings.daily_target_platform_prefix,
                require_contact=settings.daily_target_require_contact,
                require_unhosted_domain=settings.daily_target_require_unhosted_domain,
                require_domain_qualification=settings.daily_target_require_domain_qualification,
                allow_recycle=settings.daily_target_allow_recycle,
            )
            with self._state_lock:
                self._last_result = {
                    "trigger": "manual_daily_target",
                    "busy": False,
                    "pipeline": None,
                    "daily_target": result,
                }
                self._last_run_finished_at = _utc_now()
                self._run_count += 1
            return result
        except Exception as exc:
            with self._state_lock:
                self._last_error = str(exc)
                self._last_run_finished_at = _utc_now()
            raise
        finally:
            self._run_lock.release()

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def status(self) -> dict[str, Any]:
        settings = self._snapshot_settings()
        with self._state_lock:
            return {
                "running": self.running,
                "busy": self._run_lock.locked(),
                "settings": asdict(settings),
                "last_run_started_at": self._last_run_started_at,
                "last_run_finished_at": self._last_run_finished_at,
                "last_error": self._last_error,
                "last_result": self._last_result,
                "run_count": self._run_count,
            }
