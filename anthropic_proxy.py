"""Lightweight Anthropic API proxy for Railway.

Railway's Tailscale userspace networking breaks outbound HTTPS to api.anthropic.com.
This proxy runs on R730-2 (direct internet) and Railway calls it via Tailscale TCP.
"""

import json
import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY not set")

client = Anthropic(api_key=ANTHROPIC_API_KEY, timeout=15.0)

app = FastAPI(title="Anthropic Proxy")


class MessageRequest(BaseModel):
    model: str
    max_tokens: int
    messages: list[dict]


@app.post("/v1/messages")
async def proxy_messages(body: MessageRequest):
    """Proxy Anthropic messages.create() call."""
    try:
        resp = client.messages.create(
            model=body.model,
            max_tokens=body.max_tokens,
            messages=body.messages,
        )
        return {
            "content": [{"text": resp.content[0].text}],
            "model": resp.model,
            "usage": {"input_tokens": resp.usage.input_tokens, "output_tokens": resp.usage.output_tokens},
        }
    except Exception as e:
        logger.error("Anthropic API error: %s", e)
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok"}
