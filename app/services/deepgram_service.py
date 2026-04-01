"""Deepgram real-time transcription service.

Streams mu-law 8kHz audio from Twilio Media Streams to Deepgram Nova 3.
Returns interim and final transcript results via callback.
"""

import asyncio
import json
import logging
from typing import Callable, Awaitable

import websockets

from app.config import settings

logger = logging.getLogger(__name__)

DEEPGRAM_WS_URL = (
    "wss://api.deepgram.com/v1/listen"
    "?model=nova-3"
    "&encoding=mulaw"
    "&sample_rate=8000"
    "&channels=1"
    "&smart_format=true"
    "&punctuate=true"
    "&interim_results=true"
)


class DeepgramStreamer:
    """Streams audio to Deepgram and calls back with transcripts."""

    def __init__(self, on_transcript: Callable[[str, bool], Awaitable[None]]):
        self._on_transcript = on_transcript
        self._ws = None
        self._receive_task = None
        self._running = False

    async def start(self):
        """Connect to Deepgram WebSocket."""
        if not settings.DEEPGRAM_API_KEY:
            logger.warning("DEEPGRAM_API_KEY not set — transcription disabled")
            return

        try:
            self._ws = await websockets.connect(
                DEEPGRAM_WS_URL,
                additional_headers={"Authorization": f"Token {settings.DEEPGRAM_API_KEY}"},
            )
            self._running = True
            self._receive_task = asyncio.create_task(self._receive_loop())
            logger.info("Deepgram streamer connected")
        except Exception as e:
            logger.error("Deepgram connection failed: %s", e)
            self._ws = None

    def feed_audio(self, audio_bytes: bytes):
        """Send audio chunk to Deepgram (non-blocking)."""
        if self._ws and self._running:
            asyncio.create_task(self._send_audio(audio_bytes))

    async def _send_audio(self, audio_bytes: bytes):
        try:
            await self._ws.send(audio_bytes)
        except Exception:
            pass

    async def _receive_loop(self):
        """Receive transcript results from Deepgram."""
        try:
            async for message in self._ws:
                try:
                    data = json.loads(message)
                    if data.get("type") == "Results":
                        channel = data.get("channel", {})
                        alternatives = channel.get("alternatives", [])
                        if alternatives:
                            text = alternatives[0].get("transcript", "")
                            is_final = data.get("is_final", False)
                            if text.strip():
                                await self._on_transcript(text, is_final)
                except (json.JSONDecodeError, KeyError):
                    pass
        except websockets.ConnectionClosed:
            logger.info("Deepgram connection closed")
        except Exception as e:
            logger.error("Deepgram receive error: %s", e)
        finally:
            self._running = False

    async def stop(self):
        """Close the Deepgram connection."""
        self._running = False
        if self._ws:
            try:
                await self._ws.send(b'')
                await self._ws.close()
            except Exception:
                pass
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        logger.info("Deepgram streamer stopped")
