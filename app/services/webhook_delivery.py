"""Webhook delivery service with HMAC signatures and retries."""

import asyncio
import hashlib
import hmac
import json
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

RETRY_DELAYS = [2, 4, 8]  # seconds
TIMEOUT = 10.0


async def deliver_webhook(url: str, payload: dict, secret: str | None = None) -> bool:
    """POST JSON payload to webhook URL with HMAC signature and 3 retries."""
    body = json.dumps(payload, default=str)
    headers = {"Content-Type": "application/json"}

    if secret:
        signature = hmac.new(
            secret.encode(), body.encode(), hashlib.sha256
        ).hexdigest()
        headers["X-Webhook-Signature"] = signature

    for attempt, delay in enumerate(RETRY_DELAYS):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                resp = await client.post(url, content=body, headers=headers)
                if resp.status_code < 400:
                    logger.info("Webhook delivered to %s (status %s)", url, resp.status_code)
                    return True
                logger.warning("Webhook %s returned %s on attempt %d", url, resp.status_code, attempt + 1)
        except Exception as e:
            logger.warning("Webhook %s failed attempt %d: %s", url, attempt + 1, e)

        if attempt < len(RETRY_DELAYS) - 1:
            await asyncio.sleep(delay)

    logger.error("Webhook delivery failed after %d attempts: %s", len(RETRY_DELAYS), url)
    return False


async def deliver_to_webhook_model(webhook, event_type: str, payload: dict, db=None) -> bool:
    """Deliver a webhook event using the Webhook ORM model, updating tracking fields."""
    payload["event_type"] = event_type
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()

    success = await deliver_webhook(webhook.url, payload, secret=webhook.secret)

    if db:
        if success:
            webhook.last_triggered = datetime.now(timezone.utc)
            webhook.failure_count = 0
        else:
            webhook.failure_count = (webhook.failure_count or 0) + 1
            # Auto-disable after 10 consecutive failures
            if webhook.failure_count >= 10:
                webhook.is_active = False
                logger.warning("Webhook %s disabled after %d consecutive failures", webhook.id, webhook.failure_count)
        await db.commit()

    return success
