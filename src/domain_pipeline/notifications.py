"""Push notifications via ntfy.sh.

Completely FREE, no registration required. Just set NTFY_TOPIC in .env
and install the ntfy app on your phone: https://ntfy.sh

Sends notifications for:
- Pipeline completion with summary stats
- New leads found
- Pipeline errors
"""
from __future__ import annotations

import logging
from typing import Optional

import requests

from .config import load_config

logger = logging.getLogger(__name__)


def send_notification(
    title: str,
    message: str,
    priority: str = "default",
    tags: Optional[list[str]] = None,
) -> bool:
    """Send a push notification via ntfy.sh.

    Returns True if sent, False if not configured or failed.
    """
    config = load_config()
    if not config.ntfy_topic:
        return False

    url = f"{config.ntfy_server.rstrip('/')}/{config.ntfy_topic}"
    headers: dict[str, str] = {
        "Title": title,
        "Priority": priority,
    }
    if tags:
        headers["Tags"] = ",".join(tags)

    try:
        resp = requests.post(
            url,
            data=message.encode("utf-8"),
            headers=headers,
            timeout=10,
        )
        if resp.ok:
            logger.debug("Notification sent: %s", title)
            return True
        logger.warning("ntfy returned %d: %s", resp.status_code, resp.text[:200])
        return False
    except requests.RequestException as exc:
        logger.warning("ntfy notification failed: %s", exc)
        return False


def notify_pipeline_complete(result: dict) -> bool:
    """Send notification when pipeline run completes."""
    imported = result.get("imported", 0)
    scored = result.get("business_scored", 0)
    websites_google = result.get("websites_found", 0)
    websites_ddg = result.get("ddg_websites_found", 0)
    export_path = result.get("business_export_path", "none")

    message = (
        f"Imported: {imported}, Scored: {scored}\n"
        f"Websites found: {websites_google} (Google) + {websites_ddg} (DDG)\n"
        f"Export: {export_path}"
    )
    return send_notification(
        title="Pipeline Complete",
        message=message,
        tags=["white_check_mark", "chart_with_upwards_trend"],
    )


def notify_new_leads(count: int, export_path: Optional[str] = None) -> bool:
    """Send notification when new leads are exported."""
    message = f"{count} leads exported"
    if export_path:
        message += f"\nFile: {export_path}"
    return send_notification(
        title=f"{count} Leads Exported",
        message=message,
        priority="high" if count >= 10 else "default",
        tags=["tada", "moneybag"],
    )


def notify_error(job_name: str, error: str) -> bool:
    """Send notification when a pipeline error occurs."""
    return send_notification(
        title=f"Pipeline Error: {job_name}",
        message=error[:500],
        priority="high",
        tags=["warning", "x"],
    )
