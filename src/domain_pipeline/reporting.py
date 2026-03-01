"""Daily automated metric reports.

Collects key metrics, computes deltas from the previous report,
and sends a summary via all configured notification channels.

Can be run as a scheduled job (cron) or called from the API.

Usage:
    from domain_pipeline.reporting import generate_daily_report
    report = generate_daily_report()
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .metrics import collect_metrics
from .notifications import send_notification

logger = logging.getLogger(__name__)

# Store last report snapshot for delta computation
REPORT_STATE_FILE = Path("./data/last_report.json")


def _load_previous_report() -> Optional[dict]:
    """Load the previous report state, if any."""
    try:
        if REPORT_STATE_FILE.exists():
            return json.loads(REPORT_STATE_FILE.read_text())
    except Exception as e:
        logger.warning("Failed to load previous report: %s", e)
    return None


def _save_report_state(metrics: dict) -> None:
    """Save current metrics as the baseline for next report."""
    try:
        REPORT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "businesses_total": metrics["businesses"]["total"],
            "no_website": metrics["businesses"]["no_website"],
            "scored": metrics["businesses"]["scored"],
            "exports_total": metrics["business_exports"]["total"],
            "high_confidence": metrics["confidence_distribution"]["high"],
            "medium_confidence": metrics["confidence_distribution"]["medium"],
            "any_verified": metrics["verification"].get("any_source", 0),
        }
        REPORT_STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.warning("Failed to save report state: %s", e)


def _delta(current: int, previous: Optional[int]) -> str:
    """Format a delta like '+42' or '-5' or 'N/A'."""
    if previous is None:
        return "N/A"
    diff = current - previous
    if diff > 0:
        return f"+{diff}"
    return str(diff)


def generate_daily_report() -> dict:
    """Generate and send a daily metric report.

    Returns the report data dict.
    """
    metrics = collect_metrics()
    previous = _load_previous_report()

    biz = metrics["businesses"]
    exports = metrics["business_exports"]
    conf = metrics["confidence_distribution"]
    verified = metrics["verification"].get("any_source", 0)

    # Compute deltas
    deltas = {}
    if previous:
        deltas["businesses"] = _delta(biz["total"], previous.get("businesses_total"))
        deltas["no_website"] = _delta(biz["no_website"], previous.get("no_website"))
        deltas["scored"] = _delta(biz["scored"], previous.get("scored"))
        deltas["exports"] = _delta(exports["total"], previous.get("exports_total"))
        deltas["high_confidence"] = _delta(conf["high"], previous.get("high_confidence"))
        deltas["verified"] = _delta(verified, previous.get("any_verified"))
    else:
        deltas = {k: "N/A" for k in ["businesses", "no_website", "scored", "exports", "high_confidence", "verified"]}

    # Build report message
    lines = [
        f"Businesses: {biz['total']:,} ({deltas['businesses']})",
        f"No Website: {biz['no_website']:,} ({deltas['no_website']})",
        f"Scored: {biz['scored']:,} ({deltas['scored']})",
        f"Verified: {verified:,} ({deltas['verified']})",
        f"High Confidence: {conf['high']:,} ({deltas['high_confidence']})",
        f"Medium Confidence: {conf['medium']:,}",
        f"Exports: {exports['total']:,} ({deltas['exports']})",
    ]
    message = "\n".join(lines)

    # Send notification
    sent = send_notification(
        title="Daily Pipeline Report",
        message=message,
        tags=["chart_with_upwards_trend", "clipboard"],
    )

    # Save state for next delta
    _save_report_state(metrics)

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics_snapshot": {
            "businesses_total": biz["total"],
            "no_website": biz["no_website"],
            "scored": biz["scored"],
            "verified": verified,
            "high_confidence": conf["high"],
            "medium_confidence": conf["medium"],
            "exports_total": exports["total"],
        },
        "deltas": deltas,
        "notification_sent": sent,
    }

    logger.info("Daily report generated: %s", json.dumps(report, default=str))
    return report
