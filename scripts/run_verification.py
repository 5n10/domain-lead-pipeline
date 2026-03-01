#!/usr/bin/env python3
"""Standalone verification loop — runs independently from the API server.

This script runs the same multi-layer verification pipeline
(domain guess, SearXNG, LLM, DDG, Google Search, rescore)
in its own process. It can be started alongside the API so that
crashing the API doesn't stop verification, and vice-versa.

Usage:
    PYTHONPATH=src .venv/bin/python scripts/run_verification.py

Environment variables (all optional, sensible defaults):
    DOMAIN_GUESS_BATCH      (default: 200)
    SEARXNG_BATCH           (default: 200)
    DDG_BATCH               (default: 10)
    LLM_BATCH               (default: 10)
    GOOGLE_SEARCH_BATCH     (default: 5)
    PAUSE_BETWEEN_BATCHES   (default: 3)
    PAUSE_WHEN_IDLE         (default: 60)
    RESCORE_AFTER_BATCH     (default: true)
    LOG_LEVEL               (default: INFO)
    LOG_FORMAT              (default: json)
"""
from __future__ import annotations

import logging
import os
import signal
import sys
from threading import Event

# Ensure project source is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from domain_pipeline.logging_config import setup_logging, set_correlation_id
from domain_pipeline.retry import get_retryable_businesses, reset_for_retry
from domain_pipeline.workers.business_leads import score_businesses
from domain_pipeline.workers.domain_guess import run_batch as run_domain_guess
from domain_pipeline.workers.web_search_verify import run_batch as run_ddg_verify
from domain_pipeline.workers.llm_verify import run_batch as run_llm_verify
from domain_pipeline.workers.searxng_verify import run_batch as run_searxng_verify

logger = logging.getLogger(__name__)

# Graceful shutdown
_stop = Event()


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, "").strip()
    return int(raw) if raw else default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip()
    return float(raw) if raw else default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def verification_loop() -> None:
    """Main verification loop — mirrors AutomationController._verification_loop()."""
    domain_guess_batch = _env_int("DOMAIN_GUESS_BATCH", 200)
    domain_guess_min_score = _env_float("DOMAIN_GUESS_MIN_SCORE", 0.0)
    searxng_batch = _env_int("SEARXNG_BATCH", 200)
    searxng_min_score = _env_float("SEARXNG_MIN_SCORE", 0.0)
    ddg_batch = _env_int("DDG_BATCH", 10)
    ddg_min_score = _env_float("DDG_MIN_SCORE", 30.0)
    llm_batch = _env_int("LLM_BATCH", 10)
    llm_min_score = _env_float("LLM_MIN_SCORE", 30.0)
    google_search_batch = _env_int("GOOGLE_SEARCH_BATCH", 5)
    google_search_min_score = _env_float("GOOGLE_SEARCH_MIN_SCORE", 30.0)
    rescore_after_batch = _env_bool("RESCORE_AFTER_BATCH", True)
    pause_between = _env_int("PAUSE_BETWEEN_BATCHES", 3)
    pause_idle = _env_int("PAUSE_WHEN_IDLE", 60)

    logger.info("Standalone verification loop started", extra={
        "worker": "verification",
        "domain_guess_batch": domain_guess_batch,
        "searxng_batch": searxng_batch,
        "ddg_batch": ddg_batch,
        "llm_batch": llm_batch,
        "google_search_batch": google_search_batch,
    })

    while not _stop.is_set():
        set_correlation_id()  # Fresh correlation ID per batch cycle
        total_processed = 0

        try:
            # --- Retry: Re-queue businesses with backoff-eligible errors ---
            if not _stop.is_set():
                try:
                    retryable = get_retryable_businesses(limit=50)
                    if retryable:
                        ids = [r["id"] for r in retryable]
                        reset_count = reset_for_retry(ids)
                        logger.info(
                            "Re-queued %d businesses for retry (of %d eligible)",
                            reset_count, len(retryable),
                            extra={"worker": "verification"},
                        )
                except Exception as e:
                    logger.exception("Retry re-queue error: %s", e)

            # --- Layer 1: Domain Guess ---
            if not _stop.is_set():
                try:
                    result = run_domain_guess(limit=domain_guess_batch, min_score=domain_guess_min_score)
                    processed = result.get("processed", 0)
                    total_processed += processed
                    if processed > 0:
                        logger.info(
                            "Domain Guess: %d processed, %d websites",
                            processed, result.get("websites_found", 0),
                            extra={"worker": "domain_guess", "batch_size": processed},
                        )
                except Exception as e:
                    logger.exception("Domain Guess error: %s", e)

            # --- Layer 2: SearXNG ---
            if not _stop.is_set():
                try:
                    result = run_searxng_verify(limit=searxng_batch, min_score=searxng_min_score)
                    processed = result.get("processed", 0)
                    total_processed += processed
                    if processed > 0:
                        logger.info(
                            "SearXNG: %d processed, %d websites",
                            processed, result.get("websites_found", 0),
                            extra={"worker": "searxng", "batch_size": processed},
                        )
                except Exception as e:
                    logger.exception("SearXNG error: %s", e)

            # --- Layer 3: LLM Verify ---
            if not _stop.is_set():
                try:
                    result = run_llm_verify(limit=llm_batch, min_score=llm_min_score)
                    processed = result.get("processed", 0)
                    total_processed += processed
                    if processed > 0:
                        logger.info(
                            "LLM Verify: %d processed, %d websites",
                            processed, result.get("websites_found", 0),
                            extra={"worker": "llm", "batch_size": processed},
                        )
                except Exception as e:
                    logger.exception("LLM Verify error: %s", e)

            # --- Layer 4: DDG Search ---
            if not _stop.is_set():
                try:
                    result = run_ddg_verify(limit=ddg_batch, min_score=ddg_min_score)
                    processed = result.get("processed", 0)
                    total_processed += processed
                    if processed > 0:
                        logger.info(
                            "DDG Search: %d processed, %d websites",
                            processed, result.get("websites_found", 0),
                            extra={"worker": "ddg", "batch_size": processed},
                        )
                except Exception as e:
                    logger.exception("DDG Search error: %s", e)

            # --- Layer 5: Google Search ---
            if not _stop.is_set():
                try:
                    from domain_pipeline.workers.google_search_verify import run_batch as run_google_search_verify
                    result = run_google_search_verify(limit=google_search_batch, min_score=google_search_min_score)
                    processed = result.get("processed", 0)
                    total_processed += processed
                    if processed > 0:
                        logger.info(
                            "Google Search: %d processed, %d websites",
                            processed, result.get("websites_found", 0),
                            extra={"worker": "google_search", "batch_size": processed},
                        )
                except Exception as e:
                    logger.exception("Google Search error: %s", e)

            # --- Rescore ---
            if not _stop.is_set() and rescore_after_batch and total_processed > 0:
                try:
                    rescored = score_businesses(limit=None, force_rescore=False)
                    logger.info(
                        "Rescored %d businesses after batch (%d total processed)",
                        rescored, total_processed,
                        extra={"worker": "verification"},
                    )
                except Exception as e:
                    logger.exception("Rescore error: %s", e)

        except Exception as exc:
            logger.exception("Verification loop error: %s", exc)

        # --- Wait ---
        if total_processed == 0:
            logger.info("No work found, sleeping %ds", pause_idle, extra={"worker": "verification"})
            wait = pause_idle
        else:
            logger.info("Batch complete (%d processed), sleeping %ds", total_processed, pause_between,
                        extra={"worker": "verification"})
            wait = pause_between

        if _stop.wait(wait):
            break

    logger.info("Standalone verification loop stopped", extra={"worker": "verification"})


def main() -> None:
    setup_logging(
        level=os.getenv("LOG_LEVEL", "INFO"),
        json_format=os.getenv("LOG_FORMAT", "json").lower() == "json",
    )

    def _signal_handler(signum: int, frame) -> None:
        logger.info("Received signal %d, stopping...", signum)
        _stop.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    verification_loop()


if __name__ == "__main__":
    main()
