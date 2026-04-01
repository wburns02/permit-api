"""WebSocket endpoints for real-time call transcription.

Two paths:
1. /ws/twilio-media/{room_key}  — Twilio sends mu-law audio here
2. /ws/call-transcript/{room_key} — Browser connects to receive transcripts
"""

import base64
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.transcript_manager import transcript_manager
from app.services.deepgram_service import DeepgramStreamer
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/call-transcript/{room_key}")
async def ws_call_transcript(websocket: WebSocket, room_key: str):
    """Browser connects here to receive live transcript entries."""
    await websocket.accept()
    await transcript_manager.connect(room_key, websocket)

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except (json.JSONDecodeError, TypeError):
                pass
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error("Transcript WS error for %s: %s", room_key, e)
    finally:
        await transcript_manager.disconnect(room_key, websocket)


@router.websocket("/ws/twilio-media/{room_key}")
async def ws_twilio_media(websocket: WebSocket, room_key: str):
    """Twilio Media Streams sends mu-law 8kHz audio here."""
    await websocket.accept()
    logger.info("Twilio media stream connected for %s", room_key)

    streamer = None

    try:
        if settings.DEEPGRAM_API_KEY:
            async def on_transcript(text: str, is_final: bool):
                await transcript_manager.broadcast_transcript(
                    room_key=room_key,
                    text=text,
                    is_final=is_final,
                    speaker="customer",
                )

            streamer = DeepgramStreamer(on_transcript=on_transcript)
            await streamer.start()
            logger.info("Deepgram started for %s", room_key)
        else:
            logger.warning("Deepgram not configured — no live transcription for %s", room_key)

        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            event = msg.get("event")

            if event == "connected":
                logger.info("Twilio media connected: streamSid=%s", msg.get("streamSid", "?"))

            elif event == "start":
                logger.info("Twilio media start for %s", room_key)

            elif event == "media":
                payload = msg.get("media", {}).get("payload", "")
                if payload and streamer:
                    audio_bytes = base64.b64decode(payload)
                    streamer.feed_audio(audio_bytes)

            elif event == "stop":
                logger.info("Twilio media stop for %s", room_key)
                break

    except WebSocketDisconnect:
        logger.info("Twilio media disconnected for %s", room_key)
    except Exception as e:
        logger.error("Twilio media error for %s: %s", room_key, e)
    finally:
        if streamer:
            await streamer.stop()
        logger.info("Twilio media cleanup done for %s", room_key)
