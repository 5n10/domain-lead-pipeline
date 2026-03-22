from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Any, Optional

from .config import load_config
from .pipeline import run_once
from .queue_runner import maintain_queue, run_queued_batch
from .retry import get_retryable_businesses, reset_for_retry
from .workers.business_leads import ensure_daily_target_generated, score_businesses


logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Verification settings (separate from pipeline settings)
# ---------------------------------------------------------------------------

@dataclass
class VerificationSettings:
    """Settings for the continuous verification loop."""
    domain_guess_batch: int = 200         # ~200 businesses → ~8-25 min per batch
    domain_guess_min_score: float = 0.0
    searxng_batch: int = 200              # SearXNG meta-search, 5x parallel, ~100/min
    searxng_min_score: float = 0.0
    ddg_batch: int = 10                   # DDG HTML scraping — slow, keep small
    ddg_min_score: float = 30.0
    llm_batch: int = 10                   # Groq API — free tier rate-limited, keep small
    llm_min_score: float = 30.0
    google_search_batch: int = 5          # Google scraping — slowest, keep minimal
    google_search_min_score: float = 30.0
    rescore_after_batch: bool = True
    pause_between_batches: int = 3        # seconds between batches when there IS work
    pause_when_idle: int = 60             # seconds to sleep when all layers processed 0


# ---------------------------------------------------------------------------
# Pipeline automation settings (existing)
# ---------------------------------------------------------------------------

@dataclass
class AutomationSettings:
    interval_seconds: int = 900
    area: Optional[str] = None
    categories: str = "all"
    areas_file: str = "config/areas.json"
    categories_file: str = "config/categories.json"
    sync_limit: Optional[int] = 1000  # Process 1000 businesses per cycle (prevent bottlenecks)
    rdap_limit: Optional[int] = 200
    rdap_statuses: list[str] = field(default_factory=lambda: ["new", "skipped", "rdap_error", "dns_error"])
    email_limit: Optional[int] = 1000
    score_limit: Optional[int] = 1000
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

        # --- Pipeline thread state ---
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

        # --- Continuous verification thread state ---
        self._verify_settings = VerificationSettings()
        self._verify_thread: Optional[Thread] = None
        self._verify_stop_event = Event()
        self._verify_state_lock = Lock()

        self._verify_last_started_at: Optional[str] = None
        self._verify_last_finished_at: Optional[str] = None
        self._verify_last_error: Optional[str] = None
        self._verify_batch_count: int = 0
        self._verify_totals: dict[str, int] = {
            "domain_guess_processed": 0,
            "domain_guess_websites": 0,
            "searxng_processed": 0,
            "searxng_websites": 0,
            "ddg_processed": 0,
            "ddg_websites": 0,
            "llm_processed": 0,
            "llm_websites": 0,
            "google_search_processed": 0,
            "google_search_websites": 0,
            "rescored": 0,
        }

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def auto_start_enabled(self) -> bool:
        return self._auto_start

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    @property
    def verification_running(self) -> bool:
        return bool(self._verify_thread and self._verify_thread.is_alive())

    # ------------------------------------------------------------------
    # Pipeline settings helpers
    # ------------------------------------------------------------------

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

    def _snapshot_verify_settings(self) -> VerificationSettings:
        with self._settings_lock:
            return VerificationSettings(**asdict(self._verify_settings))

    def update_verify_settings(self, updates: dict[str, Any]) -> None:
        with self._settings_lock:
            for key, value in updates.items():
                if value is not None and hasattr(self._verify_settings, key):
                    setattr(self._verify_settings, key, value)

    # ------------------------------------------------------------------
    # Pipeline cycle (existing run_once based)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Continuous verification loop (NEW — runs in its own thread)
    # ------------------------------------------------------------------

    def _verification_loop(self) -> None:
        """Continuously run verification layers in tight batches.

        This is separate from the full pipeline loop. It cycles through:
          1. Domain Guess (fast, FREE, ~500/min)
          2. LLM Verify (fast via Groq API, ~50/batch)
          3. DDG Search (moderate, FREE, ~40/min)
          4. Google Search (slow, FREE, ~15/min)
          5. Rescore affected businesses

        If no work is found, sleeps for ``pause_when_idle`` seconds (default 120).
        If work was done, sleeps for ``pause_between_batches`` seconds (default 5).
        """
        logger.info("Continuous verification loop started")

        while not self._verify_stop_event.is_set():
            settings = self._snapshot_verify_settings()
            total_processed = 0

            with self._verify_state_lock:
                self._verify_last_started_at = _utc_now()
                self._verify_last_error = None

            try:
                # --- Retry: Re-queue businesses with backoff-eligible errors ---
                if not self._verify_stop_event.is_set():
                    try:
                        retryable = get_retryable_businesses(limit=50)
                        if retryable:
                            ids = [r["id"] for r in retryable]
                            reset_count = reset_for_retry(ids)
                            logger.info(
                                "Verification: Re-queued %d businesses for retry (of %d eligible)",
                                reset_count, len(retryable),
                            )
                    except Exception as e:
                        logger.exception("Verification: Retry re-queue error: %s", e)

                # --- Run verification layers via queue_runner ---
                layers = [
                    ("domain_guess", settings.domain_guess_batch, settings.domain_guess_min_score),
                    ("searxng", settings.searxng_batch, settings.searxng_min_score),
                    ("llm", settings.llm_batch, settings.llm_min_score),
                    ("ddg", settings.ddg_batch, settings.ddg_min_score),
                    ("google_search", settings.google_search_batch, settings.google_search_min_score),
                ]

                for layer_name, batch_size, min_score in layers:
                    if self._verify_stop_event.is_set():
                        break
                    try:
                        qr = run_queued_batch(layer_name, limit=batch_size, min_score=min_score)
                        worker_result = qr.get("worker_result", {}) or {}
                        layer_processed = worker_result.get("processed", 0) if isinstance(worker_result, dict) else 0
                        layer_websites = worker_result.get("websites_found", 0) if isinstance(worker_result, dict) else 0
                        total_processed += layer_processed
                        with self._verify_state_lock:
                            self._verify_totals[f"{layer_name}_processed"] = (
                                self._verify_totals.get(f"{layer_name}_processed", 0) + layer_processed
                            )
                            self._verify_totals[f"{layer_name}_websites"] = (
                                self._verify_totals.get(f"{layer_name}_websites", 0) + layer_websites
                            )
                        if layer_processed > 0:
                            logger.info(
                                "Verification: %s batch done — %d processed, %d websites found (claimed %d from queue)",
                                layer_name, layer_processed, layer_websites, qr.get("claimed", 0),
                            )
                    except Exception as e:
                        logger.exception("Verification: %s error: %s", layer_name, e)

                # --- Queue maintenance (hourly) ---
                if not self._verify_stop_event.is_set():
                    try:
                        now = datetime.now(timezone.utc)
                        last_maint = getattr(self, "_last_queue_maintenance", None)
                        if last_maint is None or (now - last_maint).total_seconds() >= 3600:
                            maint = maintain_queue()
                            self._last_queue_maintenance = now
                            logger.info(
                                "Queue maintenance: requeued %d failed, enqueued %d stale, depth=%s",
                                maint.get("requeued_failed", 0),
                                maint.get("stale_enqueued", 0),
                                maint.get("queue_depth", {}),
                            )
                    except Exception as e:
                        logger.exception("Queue maintenance error: %s", e)

                # --- Rescore after verification ---
                if self._verify_stop_event.is_set():
                    break
                if settings.rescore_after_batch and total_processed > 0:
                    try:
                        rescored = score_businesses(limit=None, force_rescore=False)
                        with self._verify_state_lock:
                            self._verify_totals["rescored"] += rescored
                        logger.info(
                            "Verification: Rescored %d businesses after batch (%d total processed)",
                            rescored, total_processed,
                        )
                    except Exception as e:
                        logger.exception("Verification: Rescore error: %s", e)

                with self._verify_state_lock:
                    self._verify_batch_count += 1
                    self._verify_last_finished_at = _utc_now()

            except Exception as exc:
                with self._verify_state_lock:
                    self._verify_last_error = str(exc)
                    self._verify_last_finished_at = _utc_now()
                logger.exception("Verification loop error: %s", exc)

            # --- Wait ---
            if total_processed == 0:
                wait = settings.pause_when_idle
                logger.info("Verification: No work found, sleeping %ds", wait)
            else:
                wait = settings.pause_between_batches
                logger.info(
                    "Verification: Batch complete (%d processed), sleeping %ds",
                    total_processed, wait,
                )

            if self._verify_stop_event.wait(wait):
                break

        logger.info("Continuous verification loop stopped")

    # ------------------------------------------------------------------
    # Pipeline start / stop
    # ------------------------------------------------------------------

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
            self._thread.join(timeout=30)
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

    # ------------------------------------------------------------------
    # Continuous verification start / stop
    # ------------------------------------------------------------------

    def start_verification(self, updates: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Start the continuous verification loop in a background thread."""
        if updates:
            self.update_verify_settings(updates)

        if self.verification_running:
            return self.status()

        self._verify_stop_event = Event()
        self._verify_thread = Thread(
            target=self._verification_loop,
            daemon=True,
            name="domain-lead-verify-loop",
        )
        self._verify_thread.start()
        return self.status()

    def stop_verification(self) -> dict[str, Any]:
        """Stop the continuous verification loop."""
        if self._verify_thread and self._verify_thread.is_alive():
            self._verify_stop_event.set()
            self._verify_thread.join(timeout=60)
        return self.status()

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        settings = self._snapshot_settings()
        verify_settings = self._snapshot_verify_settings()
        with self._state_lock:
            pipeline_status = {
                "running": self.running,
                "busy": self._run_lock.locked(),
                "settings": asdict(settings),
                "last_run_started_at": self._last_run_started_at,
                "last_run_finished_at": self._last_run_finished_at,
                "last_error": self._last_error,
                "last_result": self._last_result,
                "run_count": self._run_count,
            }
        with self._verify_state_lock:
            verification_status = {
                "running": self.verification_running,
                "settings": asdict(verify_settings),
                "last_started_at": self._verify_last_started_at,
                "last_finished_at": self._verify_last_finished_at,
                "last_error": self._verify_last_error,
                "batch_count": self._verify_batch_count,
                "totals": dict(self._verify_totals),
            }

        return {
            **pipeline_status,
            "verification": verification_status,
        }
