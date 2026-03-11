"""Webhook delivery service with retries."""

import logging
import httpx

logger = logging.getLogger(__name__)

RETRY_DELAYS = [2, 4, 8]  # seconds
TIMEOUT = 10.0


async def deliver_webhook(url: str, payload: dict) -> bool:
    """POST JSON payload to webhook URL with 3 retries and exponential backoff."""
    import asyncio

    for attempt, delay in enumerate(RETRY_DELAYS):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                resp = await client.post(url, json=payload)
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
