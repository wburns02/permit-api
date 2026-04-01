# Browser Soft Phone with Live Transcription

**Date:** 2026-04-01
**Status:** Approved
**Project:** PermitLookup (permits.ecbtx.com)

## Summary

Floating browser-based soft phone that lets users call permit holders directly from any page. Twilio WebRTC handles the call, Deepgram Nova 3 transcribes the customer side in real-time via Twilio Media Streams, Claude AI generates a post-call summary with disposition and action items.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Call provider | Twilio (existing account) | Already have credentials, proven WebRTC SDK, dual-channel recording |
| Phone number | +1 512 580 4061 (Austin local) | Higher answer rates than toll-free, $1.15/mo |
| TwiML App SID | AP657f272baa24b8e8e656b8d1bd7b5bed | Voice URL: permits.ecbtx.com/v1/dialer/twiml/outbound |
| Transcription | Deepgram Nova 3 (real-time streaming) | Cheaper than Google STT, single provider for real-time + post-call, $200 credit |
| Post-call fallback | Deepgram pre-recorded API on dual-channel recording | Full speaker-labeled transcript if live stream missed anything |
| AI summary | Claude Haiku | Already in the codebase, fast, cheap |
| UI placement | Floating widget (bottom-right) | Never leaves the current page, accessible from analyst + dialer + anywhere |
| Scope | Outbound only | Inbound calls, SMS, multi-call queue are future features |

## Credentials

All credentials stored in `/etc/permit-api.env` on R730. See infrastructure memory for values.

| Credential | Source |
|------------|--------|
| TWILIO_ACCOUNT_SID | Shared Mac Septic CRM Twilio account |
| TWILIO_AUTH_TOKEN | Same Twilio account |
| TWILIO_PHONE_NUMBER | +15125804061 (Austin local, purchased 2026-04-01) |
| TWILIO_API_KEY | Shared API key from Crown Hardware setup |
| TWILIO_API_SECRET | Same API key pair |
| TWILIO_TWIML_APP_SID | "PermitLookup" TwiML app, created 2026-04-01 |
| DEEPGRAM_API_KEY | Deepgram account (willwalterburns@gmail.com), Nova 3 |

## Architecture

```
Browser (Twilio Voice SDK + AudioContext)
  |-- Outbound call via WebRTC --> Twilio --> Phone network
  |-- Customer audio --> Twilio Media Stream --> R730 backend
  |                                              |--> Deepgram Nova 3 (real-time STT)
  |                                              |--> WebSocket --> Browser (live transcript)
  |-- Call ends --> dual-channel recording --> Deepgram pre-recorded API (fallback)
                --> Claude Haiku summary + disposition + action items
```

## Backend Changes

### New Endpoints (app/api/v1/dialer.py)

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| /v1/dialer/token | GET | API key | Mint Twilio AccessToken with VoiceGrant |
| /v1/dialer/twiml/outbound | POST | None (Twilio webhook) | Return TwiML for outbound call with Media Stream |
| /v1/dialer/recording-callback | POST | None (Twilio webhook) | Receive recording URL after call ends |
| /v1/dialer/status-callback | POST | None (Twilio webhook) | Receive call status changes |

### New WebSocket Endpoints (app/api/v1/dialer_ws.py)

| Endpoint | Purpose |
|----------|---------|
| /ws/twilio-media/{room_key} | Receives mu-law 8kHz audio from Twilio Media Stream, forwards to Deepgram |
| /ws/call-transcript/{room_key} | Sends live transcript JSON to the browser |

### New Service (app/services/deepgram_service.py)

Deepgram streaming client that:
1. Opens a WebSocket to `wss://api.deepgram.com/v1/listen`
2. Receives mu-law 8kHz audio chunks from Twilio Media Stream
3. Sends audio to Deepgram
4. Receives transcript results (interim + final)
5. Broadcasts to connected browser clients via the transcript WebSocket

Config: `model=nova-3`, `encoding=mulaw`, `sample_rate=8000`, `channels=1`, `smart_format=true`, `punctuate=true`, `interim_results=true`

### New Service (app/services/call_wrapup_service.py)

Post-call AI wrap-up using Claude Haiku:
- Input: full transcript text + lead context (permit data, address, etc.)
- Output: JSON with `call_summary`, `suggested_disposition`, `action_items[]`
- Called automatically when call ends and transcript is available

### Database Changes (app/models/dialer.py)

Add columns to existing `CallLog` model:
- `twilio_call_sid` VARCHAR(64) — Twilio's call identifier
- `recording_url` TEXT — URL to the dual-channel recording
- `recording_duration` INTEGER — recording length in seconds
- `transcript` TEXT — full transcript text

### New Dependencies

- `twilio` Python package (for AccessToken generation + TwiML building)
- `deepgram-sdk` Python package (for streaming WebSocket client)

### Environment Variables (add to /etc/permit-api.env on R730)

```
TWILIO_ACCOUNT_SID=<from Twilio console>
TWILIO_AUTH_TOKEN=<from Twilio console>
TWILIO_PHONE_NUMBER=+15125804061
TWILIO_API_KEY=<from Twilio console>
TWILIO_API_SECRET=<from Twilio console>
TWILIO_TWIML_APP_SID=<from TwiML apps page>
DEEPGRAM_API_KEY=<from Deepgram console>
```

## Frontend Changes (app/static/index.html)

### Floating Soft Phone Widget

Fixed-position element, bottom-right corner, z-index 2000 (above everything).

States:
1. **Collapsed**: Small green phone icon circle (48px), pulsing gently
2. **Expanded (idle)**: Number input, dial pad (optional), Call button, recent calls
3. **Expanded (calling)**: "Calling +1..." with cancel button, ringing animation
4. **Expanded (connected)**: Timer, live transcript scroll area, Mute/Hangup buttons
5. **Expanded (wrap-up)**: AI summary, disposition buttons, notes field, Save button

### Twilio Voice SDK

Load from CDN: `https://sdk.twilio.com/js/client/v1.14/twilio.min.js` (or latest)

Initialization flow:
1. On first "Call Now" click, fetch token from `GET /v1/dialer/token`
2. Create `Twilio.Device` with token
3. Register event handlers: `registered`, `error`, `incoming`
4. `device.register()` to connect to Twilio signaling
5. On call: `device.connect({ params: { To: "+1XXXXXXXXXX" } })`

### Integration Points

- **Analyst panel "Call Now" button**: Instead of `<a href="tel:...">`, calls `openSoftphone(phoneNumber, leadContext)`
- **Analyst panel "Send to Dialer" button**: Same — opens softphone with number
- **Dialer page "Call" button**: Instead of `window.open('tel:...')`, calls `openSoftphone(phoneNumber, leadContext)`
- **Batch "Send to Dialer" action**: Queues leads, first one auto-opens in softphone

### Live Transcript Display

During a connected call, the widget shows a scrolling transcript area:
- Connects to `wss://permits.ecbtx.com/ws/call-transcript/{room_key}`
- Receives JSON: `{ "text": "...", "is_final": true/false, "speaker": "customer", "timestamp": "..." }`
- Interim results shown in gray/italic, final results in white/normal
- Auto-scrolls to bottom

### Post-Call Wrap-Up

When call disconnects:
1. Stop timer
2. Show "Processing..." spinner
3. POST to `/v1/dialer/log` with call data
4. POST to `/v1/dialer/{id}/wrap-up` with transcript
5. Display: AI summary, suggested disposition (pre-selected), action items
6. User confirms disposition, adds notes, clicks Save
7. Widget collapses back to icon

## TwiML Response (from /v1/dialer/twiml/outbound)

```xml
<Response>
  <Start>
    <Stream url="wss://permits.ecbtx.com/ws/twilio-media/{phone_digits}" track="inbound_track"/>
  </Start>
  <Dial callerId="+15125804061"
        record="record-from-answer-dual"
        recordingStatusCallback="https://permits.ecbtx.com/v1/dialer/recording-callback"
        recordingStatusCallbackMethod="POST"
        action="https://permits.ecbtx.com/v1/dialer/status-callback">
    <Number>{destination_number}</Number>
  </Dial>
</Response>
```

## Implementation Order

1. Backend: Twilio token endpoint + TwiML webhook + recording/status callbacks
2. Backend: Deepgram streaming service + WebSocket endpoints
3. Backend: Call wrap-up service (Claude AI)
4. Backend: Database migration (new CallLog columns)
5. Frontend: Floating widget HTML/CSS
6. Frontend: Twilio Device initialization + call flow
7. Frontend: Live transcript WebSocket display
8. Frontend: Post-call wrap-up UI
9. Frontend: Integration with analyst panel + dialer page
10. Environment variables on R730

## Out of Scope

- Inbound calls (outbound only)
- SMS/text messaging
- Multiple simultaneous calls
- Agent-side real-time STT (customer side only for now)
- Call transfer/hold
- Voicemail detection
- STIR/SHAKEN caller ID verification
