"""WebSocket connection manager for real-time call transcription.

Routes transcript data from Deepgram to connected browser clients.
Accumulates final lines so the full transcript is available at call end.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class TranscriptWSManager:
    """Manages WebSocket connections keyed by room_key (phone digits)."""

    def __init__(self):
        self._connections: dict[str, set[WebSocket]] = {}
        self._transcript_buffer: dict[str, list[str]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, room_key: str, ws: WebSocket):
        async with self._lock:
            if room_key not in self._connections:
                self._connections[room_key] = set()
            self._connections[room_key].add(ws)
        logger.info("Transcript WS connected for %s (total: %d)", room_key, len(self._connections.get(room_key, set())))

    async def disconnect(self, room_key: str, ws: WebSocket):
        async with self._lock:
            conns = self._connections.get(room_key)
            if conns:
                conns.discard(ws)
                if not conns:
                    del self._connections[room_key]

    async def broadcast_transcript(self, room_key: str, text: str, is_final: bool, speaker: str = "customer"):
        if is_final and text.strip():
            if room_key not in self._transcript_buffer:
                self._transcript_buffer[room_key] = []
            self._transcript_buffer[room_key].append(text.strip())

        message = json.dumps({
            "text": text,
            "is_final": is_final,
            "room_key": room_key,
            "speaker": speaker,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        async with self._lock:
            conns = self._connections.get(room_key)
            if not conns:
                return
            conns_snapshot = list(conns)

        dead = []
        for ws in conns_snapshot:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                conns = self._connections.get(room_key)
                if conns:
                    for ws in dead:
                        conns.discard(ws)
                    if not conns:
                        del self._connections[room_key]

    def get_transcript(self, room_key: str) -> str:
        return " ".join(self._transcript_buffer.get(room_key, []))

    def clear_transcript(self, room_key: str):
        self._transcript_buffer.pop(room_key, None)


transcript_manager = TranscriptWSManager()
