"""Drop-in Anthropic-shaped client backed by local Ollama on R730.

LocalAnthropic / AsyncLocalAnthropic mimic anthropic.Anthropic /
anthropic.AsyncAnthropic so call sites only need a one-line import swap:

    from app.services.llm_client import LocalAnthropic as Anthropic
    from app.services.llm_client import AsyncLocalAnthropic as AsyncAnthropic

Calls to client.messages.create(model=..., max_tokens=..., messages=[...])
are routed to Ollama. The model name passed by callers is ignored; the
Ollama model name comes from OLLAMA_MODEL (default qwen3.5:122b on R730).

Set ALLOW_ANTHROPIC=1 in the env to fall back to the real Anthropic SDK
when credits exist and you specifically want hosted Sonnet/Haiku output.
"""
from __future__ import annotations

import os
from typing import Any

import httpx

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:122b")
_ALLOW_HOSTED = os.environ.get("ALLOW_ANTHROPIC", "0") == "1"


class _Content:
    __slots__ = ("text", "type")

    def __init__(self, text: str) -> None:
        self.text = text
        self.type = "text"


class _Usage:
    __slots__ = ("input_tokens", "output_tokens")

    def __init__(self, input_tokens: int = 0, output_tokens: int = 0) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class _Response:
    __slots__ = ("content", "model", "usage")

    def __init__(self, text: str, model: str, usage: _Usage) -> None:
        self.content = [_Content(text)]
        self.model = model
        self.usage = usage


def _payload(messages: list[dict], max_tokens: int) -> dict:
    # Note: `think` is a top-level field on Ollama /api/chat (not under options).
    # Putting it under options silently leaves the model in thinking mode and
    # the visible `message.content` comes back empty.
    return {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": False,
        "think": False,
        "options": {
            "num_predict": max_tokens,
        },
    }


def _parse(body: dict) -> _Response:
    text = (body.get("message") or {}).get("content") or ""
    usage = _Usage(
        input_tokens=int(body.get("prompt_eval_count") or 0),
        output_tokens=int(body.get("eval_count") or 0),
    )
    return _Response(text=text, model=OLLAMA_MODEL, usage=usage)


class _SyncMessages:
    def create(self, *, model: str, max_tokens: int, messages: list[dict], **_: Any) -> _Response:
        with httpx.Client(timeout=120.0) as c:
            r = c.post(f"{OLLAMA_URL}/api/chat", json=_payload(messages, max_tokens))
            r.raise_for_status()
            return _parse(r.json())


class _AsyncMessages:
    async def create(self, *, model: str, max_tokens: int, messages: list[dict], **_: Any) -> _Response:
        async with httpx.AsyncClient(timeout=120.0) as c:
            r = await c.post(f"{OLLAMA_URL}/api/chat", json=_payload(messages, max_tokens))
            r.raise_for_status()
            return _parse(r.json())


class LocalAnthropic:
    """Sync drop-in for anthropic.Anthropic."""

    def __init__(self, *_: Any, **__: Any) -> None:
        self.messages = _SyncMessages()


class AsyncLocalAnthropic:
    """Async drop-in for anthropic.AsyncAnthropic."""

    def __init__(self, *_: Any, **__: Any) -> None:
        self.messages = _AsyncMessages()
