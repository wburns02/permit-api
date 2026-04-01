# Browser Soft Phone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a floating browser-based soft phone to PermitLookup that makes outbound calls via Twilio WebRTC with real-time Deepgram transcription and Claude AI post-call summaries.

**Architecture:** The Twilio Voice SDK in the browser initiates WebRTC calls. Twilio Media Streams sends customer audio to the backend via WebSocket, which pipes it to Deepgram Nova 3 for real-time STT. Transcripts are broadcast to the browser via a second WebSocket. Post-call, Claude Haiku generates a summary. All code lives in the existing permit-api project on R730.

**Tech Stack:** Twilio Voice SDK (browser), Twilio Python SDK (backend token/TwiML), Deepgram SDK (streaming STT), FastAPI WebSockets, Claude Haiku (AI summaries), vanilla JS frontend

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `app/config.py` | Modify | Add Twilio + Deepgram settings |
| `app/models/dialer.py` | Modify | Add twilio_call_sid, recording_url, recording_duration, transcript columns |
| `app/services/twilio_voice.py` | Create | Token generation, TwiML building, E.164 formatting |
| `app/services/deepgram_service.py` | Create | Deepgram streaming WebSocket client |
| `app/services/transcript_manager.py` | Create | WebSocket connection manager + transcript buffer |
| `app/api/v1/dialer.py` | Modify | Add token, TwiML, recording-callback, status-callback endpoints |
| `app/api/v1/dialer_ws.py` | Create | WebSocket endpoints for Twilio Media + browser transcript |
| `app/main.py` | Modify | Mount WebSocket router, add DB migration |
| `app/static/index.html` | Modify | Floating softphone widget + Twilio Device + transcript display |
| `requirements.txt` | Modify | Add twilio, deepgram-sdk |

---

### Task 1: Config + Dependencies + Database Migration

**Files:**
- Modify: `app/config.py`
- Modify: `app/models/dialer.py`
- Modify: `app/main.py` (startup migration)
- Modify: `requirements.txt`

- [ ] **Step 1: Add Twilio + Deepgram settings to config.py**

After the `ANTHROPIC_API_KEY` line (~line 86), add:

```python
    # Twilio Voice (browser soft phone)
    TWILIO_ACCOUNT_SID: str | None = None
    TWILIO_AUTH_TOKEN: str | None = None
    TWILIO_PHONE_NUMBER: str | None = None
    TWILIO_API_KEY: str | None = None
    TWILIO_API_SECRET: str | None = None
    TWILIO_TWIML_APP_SID: str | None = None

    # Deepgram (real-time transcription)
    DEEPGRAM_API_KEY: str | None = None
```

- [ ] **Step 2: Add columns to CallLog model**

In `app/models/dialer.py`, add after the `callback_date` column (line 25):

```python
    twilio_call_sid = Column(String(64))  # Twilio's call identifier
    recording_url = Column(Text)  # URL to dual-channel recording
    recording_duration = Column(Integer)  # Recording length in seconds
    transcript = Column(Text)  # Full transcript text
```

- [ ] **Step 3: Add dependencies to requirements.txt**

Append to `requirements.txt`:

```
twilio>=9.0.0
deepgram-sdk>=3.0.0
```

- [ ] **Step 4: Add database migration in main.py**

In `app/main.py`, in the `lifespan()` function, after the existing `webhook_url` migration (~line 84), add:

```python
    # Auto-migrate: add softphone columns to call_logs
    try:
        async with primary_engine.begin() as conn:
            for col, typ in [
                ("twilio_call_sid", "VARCHAR(64)"),
                ("recording_url", "TEXT"),
                ("recording_duration", "INTEGER"),
                ("transcript", "TEXT"),
            ]:
                await conn.execute(_text(
                    f"ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS {col} {typ}"
                ))
    except Exception as e:
        logger.warning("Could not apply softphone migration: %s", e)
```

- [ ] **Step 5: Commit**

```bash
cd /home/will/permit-api
git add app/config.py app/models/dialer.py app/main.py requirements.txt
git commit -m "feat: add Twilio + Deepgram config, CallLog columns, dependencies"
```

---

### Task 2: Twilio Voice Service

**Files:**
- Create: `app/services/twilio_voice.py`

- [ ] **Step 1: Create the Twilio voice service**

```python
"""Twilio Voice service — browser calling via WebRTC.

Handles access token generation, TwiML building, and E.164 formatting.
Ported from Crown Hardware implementation, adapted for PermitLookup.
"""

import logging
from app.config import settings

logger = logging.getLogger(__name__)


def is_configured() -> bool:
    """Check if Twilio voice calling is fully configured."""
    return all([
        settings.TWILIO_ACCOUNT_SID,
        settings.TWILIO_AUTH_TOKEN,
        settings.TWILIO_PHONE_NUMBER,
        settings.TWILIO_API_KEY,
        settings.TWILIO_API_SECRET,
        settings.TWILIO_TWIML_APP_SID,
    ])


def generate_access_token(identity: str) -> str | None:
    """Create a Twilio Access Token with VoiceGrant for browser calling."""
    if not is_configured():
        return None

    from twilio.jwt.access_token import AccessToken
    from twilio.jwt.access_token.grants import VoiceGrant

    token = AccessToken(
        settings.TWILIO_ACCOUNT_SID,
        settings.TWILIO_API_KEY,
        settings.TWILIO_API_SECRET,
        identity=identity,
        ttl=3600,
    )
    voice_grant = VoiceGrant(
        outgoing_application_sid=settings.TWILIO_TWIML_APP_SID,
        incoming_allow=False,  # Outbound only for now
    )
    token.add_grant(voice_grant)
    return token.to_jwt()


def format_e164(phone: str) -> str:
    """Clean and format a phone number to E.164."""
    clean = phone.strip().replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('.', '')
    if not clean.startswith('+'):
        if len(clean) == 10:
            clean = f'+1{clean}'
        elif len(clean) == 11 and clean.startswith('1'):
            clean = f'+{clean}'
        else:
            clean = f'+{clean}'
    return clean


def build_outbound_twiml(to_number: str, host: str) -> str:
    """Build TwiML XML for outbound call with recording + media stream."""
    from twilio.twiml.voice_response import VoiceResponse

    clean = format_e164(to_number)
    room_key = clean.replace('+', '')  # e.g., "15125551234"

    response = VoiceResponse()

    # Media Stream for real-time transcription (customer audio)
    if settings.DEEPGRAM_API_KEY:
        stream_url = f"wss://{host}/ws/twilio-media/{room_key}"
        start = response.start()
        start.stream(url=stream_url, track="inbound_track")
        logger.info("TwiML: injecting <Stream> -> %s", stream_url)

    # Dial with dual-channel recording
    dial = response.dial(
        caller_id=settings.TWILIO_PHONE_NUMBER,
        record="record-from-answer-dual",
        recording_status_callback=f"https://{host}/v1/dialer/recording-callback",
        recording_status_callback_method="POST",
    )
    dial.number(clean)

    return str(response)
```

- [ ] **Step 2: Commit**

```bash
git add app/services/twilio_voice.py
git commit -m "feat: add Twilio voice service — token, TwiML, E.164 formatting"
```

---

### Task 3: Deepgram Streaming Service

**Files:**
- Create: `app/services/deepgram_service.py`

- [ ] **Step 1: Create the Deepgram streaming service**

```python
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
            pass  # Connection may have closed

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
                # Send empty byte to signal end of audio
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
```

- [ ] **Step 2: Commit**

```bash
git add app/services/deepgram_service.py
git commit -m "feat: add Deepgram streaming service — Nova 3 real-time STT"
```

---

### Task 4: Transcript WebSocket Manager

**Files:**
- Create: `app/services/transcript_manager.py`

- [ ] **Step 1: Create the transcript manager**

This is ported directly from Crown Hardware's `call_transcript_manager.py`, unchanged — it's provider-agnostic and works with any STT backend.

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add app/services/transcript_manager.py
git commit -m "feat: add transcript WebSocket manager — buffers + broadcasts"
```

---

### Task 5: WebSocket Endpoints (Twilio Media + Browser Transcript)

**Files:**
- Create: `app/api/v1/dialer_ws.py`
- Modify: `app/main.py` (mount WS router)

- [ ] **Step 1: Create the WebSocket router**

```python
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
```

- [ ] **Step 2: Mount the WebSocket router in main.py**

In `app/main.py`, after the existing router imports (~line 61), add:

```python
from app.api.v1.dialer_ws import router as dialer_ws_router
```

Then after the existing `app.include_router(campaigns_router, prefix="/v1")` line (~line 162), add:

```python
app.include_router(dialer_ws_router)  # WebSocket routes mounted at root (no /v1 prefix)
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v1/dialer_ws.py app/main.py
git commit -m "feat: add WebSocket endpoints — Twilio media + browser transcript"
```

---

### Task 6: Twilio REST Endpoints (Token, TwiML, Callbacks)

**Files:**
- Modify: `app/api/v1/dialer.py` (append new endpoints)

- [ ] **Step 1: Add the Twilio endpoints**

Append at the end of `app/api/v1/dialer.py`:

```python
# ---------------------------------------------------------------------------
# Twilio Voice — browser soft phone endpoints
# ---------------------------------------------------------------------------

from fastapi import Form
from fastapi.responses import PlainTextResponse


@router.get("/token")
async def get_twilio_token(
    user: ApiUser = Depends(get_current_user),
):
    """Generate a Twilio Access Token for browser calling."""
    _require_paid(user)

    from app.services.twilio_voice import generate_access_token, is_configured

    if not is_configured():
        raise HTTPException(status_code=503, detail="Voice calling not configured")

    identity = f"user_{str(user.id)[:8]}"
    token = generate_access_token(identity)
    if not token:
        raise HTTPException(status_code=503, detail="Failed to generate voice token")

    return {"token": token, "identity": identity}


@router.post("/twiml/outbound", response_class=PlainTextResponse)
async def twiml_outbound(request: Request, To: str = Form("")):
    """TwiML webhook — Twilio calls this when browser initiates outbound call."""
    from app.services.twilio_voice import build_outbound_twiml

    host = request.headers.get("host", "permits.ecbtx.com")
    to_number = To

    if not to_number:
        form = await request.form()
        to_number = form.get("To", "")

    if not to_number:
        return PlainTextResponse(
            "<Response><Say>No phone number provided.</Say></Response>",
            media_type="text/xml",
        )

    logger.info("TwiML outbound: To=%s host=%s", to_number, host)
    twiml = build_outbound_twiml(to_number, host)
    return PlainTextResponse(twiml, media_type="text/xml")


@router.post("/recording-callback")
async def recording_callback(request: Request, db: AsyncSession = Depends(get_db)):
    """Twilio calls this when a recording is ready."""
    form = await request.form()
    call_sid = form.get("CallSid", "")
    recording_sid = form.get("RecordingSid", "")
    recording_url = form.get("RecordingUrl", "")
    recording_duration = form.get("RecordingDuration", "0")

    logger.info("Recording callback: call_sid=%s recording_sid=%s duration=%s", call_sid, recording_sid, recording_duration)

    if call_sid:
        from sqlalchemy import update
        await db.execute(
            update(CallLog)
            .where(CallLog.twilio_call_sid == call_sid)
            .values(
                recording_url=f"{recording_url}.mp3" if recording_url else None,
                recording_duration=int(recording_duration) if recording_duration else None,
            )
        )
        await db.commit()

    return {"status": "ok"}


@router.post("/status-callback")
async def status_callback(request: Request, db: AsyncSession = Depends(get_db)):
    """Twilio calls this on call status changes."""
    form = await request.form()
    call_sid = form.get("CallSid", "")
    call_status = form.get("CallStatus", "")
    call_duration = form.get("CallDuration", "0")

    logger.info("Status callback: call_sid=%s status=%s duration=%s", call_sid, call_status, call_duration)

    if call_sid and call_status == "completed":
        from sqlalchemy import update
        await db.execute(
            update(CallLog)
            .where(CallLog.twilio_call_sid == call_sid)
            .values(duration_seconds=int(call_duration) if call_duration else None)
        )
        await db.commit()

    return {"status": "ok"}
```

Also add the missing import at the top of the file (after the existing imports, ~line 10):

```python
from app.database import get_db
```

Note: `get_db` may already be imported. If so, skip this step.

- [ ] **Step 2: Add a logger at the top of dialer.py if not present**

Check if `logger` is defined. If not, add after the imports:

```python
import logging
logger = logging.getLogger(__name__)
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v1/dialer.py
git commit -m "feat: add Twilio endpoints — token, TwiML, recording + status callbacks"
```

---

### Task 7: Post-Call Wrap-Up Endpoint

**Files:**
- Modify: `app/api/v1/dialer.py` (add wrap-up endpoint)

- [ ] **Step 1: Add the wrap-up endpoint**

Append to `app/api/v1/dialer.py`:

```python
class WrapUpRequest(BaseModel):
    transcript: str = ""
    lead_context: dict = {}


@router.post("/{call_id}/wrap-up")
async def wrap_up_call(
    call_id: str,
    body: WrapUpRequest,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate AI summary from call transcript."""
    _require_paid(user)

    from app.services.call_intelligence import analyze_transcription
    from app.services.transcript_manager import transcript_manager

    # Use provided transcript or fetch from buffer
    transcript_text = body.transcript.strip()
    if not transcript_text:
        # Try to get from transcript manager buffer
        transcript_text = transcript_manager.get_transcript(call_id)

    if not transcript_text:
        raise HTTPException(status_code=400, detail="No transcript available")

    # Run AI analysis
    result = await analyze_transcription(transcript_text, body.lead_context or None)

    # Update the call log
    call_uuid = uuid.UUID(call_id) if len(call_id) > 12 else None
    if call_uuid:
        from sqlalchemy import update
        await db.execute(
            update(CallLog)
            .where(CallLog.id == call_uuid)
            .values(
                transcript=transcript_text,
                ai_summary=result.get("summary", ""),
                action_items=result.get("action_items", []),
            )
        )
        await db.commit()

    # Clean up transcript buffer
    transcript_manager.clear_transcript(call_id)

    return result
```

- [ ] **Step 2: Commit**

```bash
git add app/api/v1/dialer.py
git commit -m "feat: add post-call wrap-up endpoint — Claude AI analysis"
```

---

### Task 8: Frontend — Floating Softphone Widget HTML + CSS

**Files:**
- Modify: `app/static/index.html`

- [ ] **Step 1: Add softphone CSS**

Find the last `</style>` tag before `</head>` (in the main global styles section, around line 160). Insert the softphone CSS just before it:

```css
/* Floating Soft Phone */
#softphone-fab{position:fixed;bottom:24px;right:24px;z-index:2000;width:56px;height:56px;border-radius:50%;background:linear-gradient(135deg,#22c55e,#16a34a);color:#fff;border:none;cursor:pointer;box-shadow:0 4px 16px rgba(34,197,94,.35);font-size:24px;display:flex;align-items:center;justify-content:center;transition:all .2s}
#softphone-fab:hover{transform:scale(1.1);box-shadow:0 6px 24px rgba(34,197,94,.45)}
#softphone-fab.active{background:linear-gradient(135deg,#ef4444,#dc2626);box-shadow:0 4px 16px rgba(239,68,68,.35)}
#softphone-widget{position:fixed;bottom:90px;right:24px;z-index:2001;width:360px;background:var(--surface);border:1px solid var(--border);border-radius:16px;box-shadow:0 8px 32px rgba(0,0,0,.4);display:none;flex-direction:column;overflow:hidden;max-height:80vh}
#softphone-widget.visible{display:flex}
.sp-header{padding:12px 16px;background:var(--surface2);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.sp-header .sp-title{font-size:13px;font-weight:600;color:var(--text)}
.sp-header button{background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px}
.sp-body{padding:16px;flex:1;overflow-y:auto}
.sp-number-row{display:flex;gap:8px;margin-bottom:12px}
.sp-number-row input{flex:1;padding:10px 12px;border:1px solid var(--border);border-radius:8px;background:var(--surface2);color:var(--text);font-size:15px;font-family:var(--font)}
.sp-call-btn{width:100%;padding:12px;border:none;border-radius:10px;font-size:15px;font-weight:700;cursor:pointer;transition:all .15s;font-family:var(--font)}
.sp-call-btn.dial{background:linear-gradient(135deg,#22c55e,#16a34a);color:#fff;box-shadow:0 4px 12px rgba(34,197,94,.25)}
.sp-call-btn.dial:hover{filter:brightness(1.1)}
.sp-call-btn.hangup{background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff}
.sp-call-btn.mute{background:var(--surface2);color:var(--text2);border:1px solid var(--border);flex:1}
.sp-call-btn.mute.active{background:rgba(239,68,68,.1);color:var(--red);border-color:var(--red)}
.sp-status{text-align:center;padding:8px 0;font-size:13px;color:var(--text2)}
.sp-timer{font-size:24px;font-weight:700;color:var(--text);text-align:center;padding:8px 0;font-variant-numeric:tabular-nums}
.sp-transcript{background:var(--surface2);border-radius:8px;padding:12px;max-height:200px;overflow-y:auto;margin:12px 0;font-size:12px;line-height:1.6}
.sp-transcript .interim{color:var(--text3);font-style:italic}
.sp-transcript .final{color:var(--text)}
.sp-actions{display:flex;gap:8px;margin-top:8px}
.sp-lead-info{font-size:12px;color:var(--text2);padding:8px 0;border-bottom:1px solid var(--border);margin-bottom:8px}
.sp-lead-info strong{color:var(--text)}
.sp-wrapup{padding:12px 0}
.sp-wrapup .summary{font-size:13px;color:var(--text);line-height:1.5;margin-bottom:12px;padding:10px;background:var(--surface2);border-radius:8px}
.sp-dispositions{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px}
.sp-dispositions button{padding:6px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface2);color:var(--text2);font-size:11px;cursor:pointer;font-family:var(--font);transition:all .15s}
.sp-dispositions button:hover,.sp-dispositions button.selected{border-color:var(--accent);color:var(--accent2);background:rgba(99,102,241,.08)}
```

- [ ] **Step 2: Add softphone HTML**

Just before the closing `</body>` tag, insert:

```html
<!-- Floating Soft Phone -->
<button id="softphone-fab" onclick="toggleSoftphone()" title="Soft Phone" style="display:none">&#x1f4de;</button>
<div id="softphone-widget">
  <div class="sp-header">
    <span class="sp-title">&#x1f4de; PermitLookup Dialer</span>
    <button onclick="toggleSoftphone()">&#x2715;</button>
  </div>
  <div class="sp-body">
    <div id="sp-lead-info" class="sp-lead-info" style="display:none"></div>
    <div id="sp-idle">
      <div class="sp-number-row">
        <input type="tel" id="sp-number" placeholder="+1 (512) 555-1234">
      </div>
      <button class="sp-call-btn dial" id="sp-dial-btn" onclick="softphoneDial()">&#x1f4de; Call</button>
    </div>
    <div id="sp-calling" style="display:none">
      <div class="sp-status" id="sp-status">Connecting...</div>
      <div class="sp-timer" id="sp-timer">00:00</div>
      <div class="sp-transcript" id="sp-transcript"></div>
      <div class="sp-actions">
        <button class="sp-call-btn mute" id="sp-mute-btn" onclick="softphoneToggleMute()">&#x1f507; Mute</button>
        <button class="sp-call-btn hangup" onclick="softphoneHangup()">&#x1f4f5; Hang Up</button>
      </div>
    </div>
    <div id="sp-wrapup" class="sp-wrapup" style="display:none">
      <div class="sp-status">Call ended</div>
      <div id="sp-summary" class="summary"></div>
      <div class="sp-dispositions" id="sp-dispositions">
        <button onclick="spSetDisposition(this,'connected')">Connected</button>
        <button onclick="spSetDisposition(this,'voicemail')">Voicemail</button>
        <button onclick="spSetDisposition(this,'no_answer')">No Answer</button>
        <button onclick="spSetDisposition(this,'callback')">Callback</button>
        <button onclick="spSetDisposition(this,'sold')">Sold</button>
      </div>
      <textarea id="sp-notes" placeholder="Call notes..." style="width:100%;padding:8px;border:1px solid var(--border);border-radius:6px;background:var(--surface2);color:var(--text);font-size:12px;font-family:var(--font);resize:vertical;min-height:60px"></textarea>
      <button class="sp-call-btn dial" onclick="softphoneSave()" style="margin-top:8px">&#x1f4be; Save & Close</button>
    </div>
  </div>
</div>
```

- [ ] **Step 3: Commit**

```bash
git add app/static/index.html
git commit -m "feat: add floating softphone widget HTML + CSS"
```

---

### Task 9: Frontend — Twilio Device + Call Flow + Transcript + Wrap-Up JS

**Files:**
- Modify: `app/static/index.html` (add JS before closing `</body>`)

- [ ] **Step 1: Add the softphone JavaScript**

Insert this script block right before the final `</body>` tag (after the softphone HTML from Task 8):

```html
<script src="https://sdk.twilio.com/js/client/releases/1.14.3/twilio.js"></script>
<script>
// ─── SOFT PHONE ─────────────────────────────────────────────────────────

var spDevice = null;
var spCall = null;
var spTimer = null;
var spTimerStart = null;
var spTranscriptWs = null;
var spRoomKey = '';
var spLeadContext = null;
var spDisposition = '';
var spCallSid = '';
var spTranscriptLines = [];

// Show FAB when user has API key
if (currentKey) document.getElementById('softphone-fab').style.display = 'flex';

function toggleSoftphone() {
  var w = document.getElementById('softphone-widget');
  w.classList.toggle('visible');
}

function openSoftphone(phone, leadCtx) {
  document.getElementById('softphone-fab').style.display = 'flex';
  document.getElementById('softphone-widget').classList.add('visible');
  document.getElementById('sp-number').value = phone || '';
  spLeadContext = leadCtx || null;
  if (leadCtx) {
    var info = document.getElementById('sp-lead-info');
    info.style.display = '';
    info.innerHTML = '<strong>' + escapeHtml(leadCtx.name || leadCtx.address || '') + '</strong><br>' + escapeHtml(leadCtx.address || '') + (leadCtx.permit_number ? '<br>Permit: ' + escapeHtml(leadCtx.permit_number) : '');
  }
  // Show idle state
  document.getElementById('sp-idle').style.display = '';
  document.getElementById('sp-calling').style.display = 'none';
  document.getElementById('sp-wrapup').style.display = 'none';
}

async function initTwilioDevice() {
  if (spDevice) return;
  if (!currentKey) { showToast('Sign in first', 'error'); return; }
  try {
    var resp = await fetch(API + '/v1/dialer/token', { headers: { 'X-API-Key': currentKey } });
    if (!resp.ok) throw new Error('Token failed: ' + resp.status);
    var data = await resp.json();
    spDevice = new Twilio.Device(data.token, { logLevel: 'warn', codecPreferences: ['opus', 'pcmu'] });
    spDevice.on('registered', function() { console.log('Twilio device registered'); });
    spDevice.on('error', function(err) { console.error('Twilio error:', err); showToast('Phone error: ' + err.message, 'error'); });
    await spDevice.register();
    console.log('Twilio device ready');
  } catch (e) {
    console.error('Twilio init failed:', e);
    showToast('Could not initialize phone: ' + e.message, 'error');
    spDevice = null;
  }
}

async function softphoneDial() {
  var number = document.getElementById('sp-number').value.trim();
  if (!number) { showToast('Enter a phone number', 'error'); return; }

  await initTwilioDevice();
  if (!spDevice) return;

  // Clean number for room key
  spRoomKey = number.replace(/[^0-9]/g, '');
  if (spRoomKey.length === 10) spRoomKey = '1' + spRoomKey;

  // Switch to calling UI
  document.getElementById('sp-idle').style.display = 'none';
  document.getElementById('sp-calling').style.display = '';
  document.getElementById('sp-wrapup').style.display = 'none';
  document.getElementById('sp-status').textContent = 'Calling ' + number + '...';
  document.getElementById('sp-timer').textContent = '00:00';
  document.getElementById('sp-transcript').innerHTML = '';
  spTranscriptLines = [];
  spDisposition = '';
  document.getElementById('softphone-fab').classList.add('active');

  try {
    spCall = await spDevice.connect({ params: { To: number } });
    spCall.on('accept', function() {
      document.getElementById('sp-status').textContent = 'Connected';
      spTimerStart = Date.now();
      spTimer = setInterval(updateSpTimer, 1000);
      spCallSid = spCall.parameters ? spCall.parameters.CallSid || '' : '';
      connectTranscriptWs();
    });
    spCall.on('disconnect', function() { handleSpCallEnd(); });
    spCall.on('cancel', function() { handleSpCallEnd(); });
    spCall.on('reject', function() { handleSpCallEnd(); });
    spCall.on('error', function(err) {
      showToast('Call error: ' + err.message, 'error');
      handleSpCallEnd();
    });
  } catch (e) {
    showToast('Call failed: ' + e.message, 'error');
    handleSpCallEnd();
  }
}

function softphoneHangup() {
  if (spCall) { spCall.disconnect(); spCall = null; }
}

function softphoneToggleMute() {
  if (!spCall) return;
  var muted = !spCall.isMuted();
  spCall.mute(muted);
  var btn = document.getElementById('sp-mute-btn');
  btn.classList.toggle('active', muted);
  btn.innerHTML = muted ? '&#x1f50a; Unmute' : '&#x1f507; Mute';
}

function updateSpTimer() {
  if (!spTimerStart) return;
  var elapsed = Math.floor((Date.now() - spTimerStart) / 1000);
  var m = String(Math.floor(elapsed / 60)).padStart(2, '0');
  var s = String(elapsed % 60).padStart(2, '0');
  document.getElementById('sp-timer').textContent = m + ':' + s;
}

function connectTranscriptWs() {
  if (!spRoomKey) return;
  var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  var url = proto + '//' + location.host + '/ws/call-transcript/' + spRoomKey;
  try {
    spTranscriptWs = new WebSocket(url);
    spTranscriptWs.onmessage = function(e) {
      try {
        var msg = JSON.parse(e.data);
        if (msg.type === 'pong') return;
        var div = document.getElementById('sp-transcript');
        if (msg.is_final) {
          spTranscriptLines.push(msg.text);
          div.innerHTML += '<div class="final">' + escapeHtml(msg.text) + '</div>';
        } else {
          var interim = div.querySelector('.interim-live');
          if (!interim) { interim = document.createElement('div'); interim.className = 'interim interim-live'; div.appendChild(interim); }
          interim.textContent = msg.text;
        }
        div.scrollTop = div.scrollHeight;
      } catch (err) {}
    };
    spTranscriptWs.onclose = function() { console.log('Transcript WS closed'); };
  } catch (e) { console.warn('Transcript WS failed:', e); }
}

async function handleSpCallEnd() {
  if (spTimer) { clearInterval(spTimer); spTimer = null; }
  if (spTranscriptWs) { spTranscriptWs.close(); spTranscriptWs = null; }
  document.getElementById('softphone-fab').classList.remove('active');
  spCall = null;

  // Switch to wrap-up
  document.getElementById('sp-calling').style.display = 'none';
  document.getElementById('sp-wrapup').style.display = '';

  var elapsed = spTimerStart ? Math.floor((Date.now() - spTimerStart) / 1000) : 0;
  var transcript = spTranscriptLines.join(' ');

  // Save call log
  if (currentKey) {
    try {
      var logBody = {
        phone_number: document.getElementById('sp-number').value,
        duration_seconds: elapsed,
        disposition: 'connected',
        notes: '',
        twilio_call_sid: spCallSid,
      };
      if (spLeadContext && spLeadContext.lead_id) logBody.lead_id = spLeadContext.lead_id;
      var resp = await fetch(API + '/v1/dialer/log', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
        body: JSON.stringify(logBody),
      });
      if (resp.ok) {
        var logData = await resp.json();
        // Try AI wrap-up if we have a transcript
        if (transcript && logData.call_id) {
          document.getElementById('sp-summary').innerHTML = '<em>Analyzing call...</em>';
          var wrapResp = await fetch(API + '/v1/dialer/' + logData.call_id + '/wrap-up', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
            body: JSON.stringify({ transcript: transcript, lead_context: spLeadContext || {} }),
          });
          if (wrapResp.ok) {
            var wrapData = await wrapResp.json();
            document.getElementById('sp-summary').innerHTML = escapeHtml(wrapData.summary || 'Call completed.');
            if (wrapData.action_items && wrapData.action_items.length) {
              document.getElementById('sp-summary').innerHTML += '<br><strong>Action items:</strong><br>' + wrapData.action_items.map(function(a) { return '• ' + escapeHtml(a.task || a); }).join('<br>');
            }
          } else {
            document.getElementById('sp-summary').textContent = 'Call completed. ' + elapsed + 's duration.';
          }
        } else {
          document.getElementById('sp-summary').textContent = 'Call completed. ' + elapsed + 's duration.';
        }
      }
    } catch (e) {
      document.getElementById('sp-summary').textContent = 'Call completed. Could not save log.';
    }
  }
}

function spSetDisposition(btn, disp) {
  spDisposition = disp;
  document.querySelectorAll('.sp-dispositions button').forEach(function(b) { b.classList.remove('selected'); });
  btn.classList.add('selected');
}

async function softphoneSave() {
  var notes = document.getElementById('sp-notes').value.trim();
  // Update disposition if changed
  if (spDisposition && currentKey) {
    try {
      await fetch(API + '/v1/dialer/disposition', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
        body: JSON.stringify({ phone_number: document.getElementById('sp-number').value, status: spDisposition }),
      });
    } catch (e) {}
  }
  showToast('Call saved', 'success');
  // Reset
  document.getElementById('sp-idle').style.display = '';
  document.getElementById('sp-calling').style.display = 'none';
  document.getElementById('sp-wrapup').style.display = 'none';
  document.getElementById('sp-number').value = '';
  document.getElementById('sp-notes').value = '';
  document.getElementById('sp-lead-info').style.display = 'none';
  document.getElementById('sp-summary').innerHTML = '';
  spLeadContext = null;
  toggleSoftphone();
}
</script>
```

- [ ] **Step 2: Commit**

```bash
git add app/static/index.html
git commit -m "feat: add softphone JS — Twilio Device, call flow, transcript, wrap-up"
```

---

### Task 10: Integration — Wire "Call Now" to Softphone

**Files:**
- Modify: `app/static/index.html` (update analyst panel + dialer page)

- [ ] **Step 1: Update analyst panel "Call Now" button**

Find the `analystOpenPanel` function. In the section where it renders the "Call Now" button (the `<a href="tel:..."` line), replace the tel: link with a softphone call:

Replace:
```javascript
+ '<a href="tel:' + esc(phone) + '" class="analyst-call-btn">\u{1f4de} Call Now</a>'
```

With:
```javascript
+ '<button onclick="openSoftphone(\'' + esc(phone).replace(/'/g, "\\'") + '\', ' + JSON.stringify(JSON.stringify({name: name, address: fullAddr, permit_number: permitNum})) + ')" class="analyst-call-btn">\u{1f4de} Call Now</button>'
```

- [ ] **Step 2: Update dialer page "Call" button**

Find the `dialerCall` function (search for `function dialerCall`). Replace the `window.open('tel:...')` call with:

```javascript
function dialerCall() {
  if (!currentDialerLead) return;
  var phone = currentDialerLead.contractor_phone || currentDialerLead.applicant_phone || currentDialerLead.phone || '';
  if (!phone) { showToast('No phone number for this lead', 'error'); return; }
  openSoftphone(phone, {
    name: currentDialerLead.contractor_name || currentDialerLead.applicant_name || '',
    address: (currentDialerLead.address || '') + ', ' + (currentDialerLead.city || '') + ' ' + (currentDialerLead.state || ''),
    permit_number: currentDialerLead.permit_number || '',
    lead_id: currentDialerLead.id || '',
  });
  dialerTimerStart = Date.now();
  dialerTimerInterval = setInterval(dialerUpdateTimer, 1000);
}
```

- [ ] **Step 3: Add env vars to R730**

SSH to R730 and append Twilio + Deepgram credentials to `/etc/permit-api.env`:

```bash
# Append Twilio + Deepgram credentials from infrastructure memory
ssh will@192.168.7.71 "sudo tee -a /etc/permit-api.env > /dev/null << 'ENV'
TWILIO_ACCOUNT_SID=<from Twilio console>
TWILIO_AUTH_TOKEN=<from Twilio console>
TWILIO_PHONE_NUMBER=+15125804061
TWILIO_API_KEY=<from Twilio console>
TWILIO_API_SECRET=<from Twilio console>
TWILIO_TWIML_APP_SID=<from TwiML apps>
DEEPGRAM_API_KEY=<from Deepgram console>
ENV"
```

Then restart the service:

```bash
ssh will@192.168.7.71 "cd /home/will/permit-api-live && git pull origin main && sudo pip install twilio deepgram-sdk websockets && sudo systemctl restart permit-api"
```

- [ ] **Step 4: Final commit and push**

```bash
git add app/static/index.html
git commit -m "feat: wire Call Now to softphone — analyst panel + dialer page"
git push
```

---

## Summary

| Task | What It Does | Files |
|------|-------------|-------|
| 1 | Config, deps, DB migration | config.py, dialer.py (model), main.py, requirements.txt |
| 2 | Twilio voice service | twilio_voice.py (new) |
| 3 | Deepgram streaming service | deepgram_service.py (new) |
| 4 | Transcript WebSocket manager | transcript_manager.py (new) |
| 5 | WebSocket endpoints | dialer_ws.py (new), main.py |
| 6 | Twilio REST endpoints | dialer.py (modify) |
| 7 | Post-call wrap-up endpoint | dialer.py (modify) |
| 8 | Floating widget HTML + CSS | index.html |
| 9 | Softphone JS (Twilio + transcript + wrapup) | index.html |
| 10 | Wire Call Now + env vars + deploy | index.html, R730 config |
