"""Twilio Voice service — browser calling via WebRTC.

Handles access token generation, TwiML building, and E.164 formatting.
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
        incoming_allow=False,
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
    room_key = clean.replace('+', '')

    response = VoiceResponse()

    if settings.DEEPGRAM_API_KEY:
        stream_url = f"wss://{host}/ws/twilio-media/{room_key}"
        start = response.start()
        start.stream(url=stream_url, track="inbound_track")
        logger.info("TwiML: injecting <Stream> -> %s", stream_url)

    dial = response.dial(
        caller_id=settings.TWILIO_PHONE_NUMBER,
        record="record-from-answer-dual",
        recording_status_callback=f"https://{host}/v1/dialer/recording-callback",
        recording_status_callback_method="POST",
    )
    dial.number(clean)

    return str(response)
