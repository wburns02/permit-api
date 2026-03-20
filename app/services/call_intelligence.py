"""Call Intelligence — AI-powered call summaries, action items, and transcription analysis.

Uses Claude API (Anthropic) to:
1. Summarize call notes into a concise paragraph
2. Extract action items with due dates
3. Analyze call transcriptions for key insights
4. Score lead quality based on conversation signals
"""

import json
import logging
from datetime import date, timedelta

from app.config import settings

logger = logging.getLogger(__name__)

# Anthropic API key from environment
ANTHROPIC_API_KEY = getattr(settings, "ANTHROPIC_API_KEY", None) or None

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None


def _get_client():
    if not Anthropic:
        logger.warning("anthropic package not installed — pip install anthropic")
        return None
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — AI features disabled")
        return None
    return Anthropic(api_key=ANTHROPIC_API_KEY)


async def summarize_call(notes: str, lead_context: dict | None = None) -> dict:
    """
    Generate AI summary + action items from call notes.

    Returns: {
        "summary": "Concise 1-2 sentence summary",
        "action_items": [{"task": "...", "due_date": "YYYY-MM-DD", "priority": "high|medium|low"}],
        "lead_score": 1-10,
        "sentiment": "positive|neutral|negative",
    }
    """
    client = _get_client()
    if not client:
        # Fallback: simple extraction without AI
        return _fallback_summary(notes)

    context_str = ""
    if lead_context:
        context_str = f"""
Lead context:
- Address: {lead_context.get('address', 'N/A')}
- Permit type: {lead_context.get('permit_type', 'N/A')}
- Valuation: ${lead_context.get('valuation', 'N/A')}
- Contractor: {lead_context.get('contractor_company', 'N/A')}
"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": f"""Analyze these sales call notes from a home services company calling a permit holder. Extract a summary, action items, lead quality score, and sentiment.
{context_str}
Call notes:
{notes}

Respond in this exact JSON format:
{{
    "summary": "1-2 sentence summary of the call",
    "action_items": [
        {{"task": "specific action to take", "due_date": "{(date.today() + timedelta(days=3)).isoformat()}", "priority": "high"}}
    ],
    "lead_score": 7,
    "sentiment": "positive"
}}

Lead score: 1=cold/not interested, 5=maybe later, 10=ready to buy now.
Only include action items that were actually discussed or implied."""
            }],
        )
        text = response.content[0].text.strip()
        # Parse JSON from response (handle markdown code blocks)
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception as e:
        logger.warning("AI summary failed: %s", e)
        return _fallback_summary(notes)


async def analyze_transcription(transcription: str, lead_context: dict | None = None) -> dict:
    """
    Analyze a full call transcription for insights.

    Returns: {
        "summary": "...",
        "action_items": [...],
        "key_quotes": ["..."],
        "objections": ["..."],
        "buying_signals": ["..."],
        "lead_score": 1-10,
        "recommended_next_step": "...",
    }
    """
    client = _get_client()
    if not client:
        return {
            "summary": transcription[:200] + "..." if len(transcription) > 200 else transcription,
            "action_items": [],
            "key_quotes": [],
            "objections": [],
            "buying_signals": [],
            "lead_score": 5,
            "recommended_next_step": "Follow up based on conversation",
        }

    context_str = ""
    if lead_context:
        context_str = f"""
Lead: {lead_context.get('address', 'N/A')} — {lead_context.get('permit_type', 'N/A')} permit, ${lead_context.get('valuation', 'N/A')} valuation
"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": f"""Analyze this sales call transcription between a home services rep and a permit holder.
{context_str}
Transcription:
{transcription[:3000]}

Respond in this exact JSON format:
{{
    "summary": "2-3 sentence summary of the conversation",
    "action_items": [{{"task": "specific action", "due_date": "{(date.today() + timedelta(days=3)).isoformat()}", "priority": "high"}}],
    "key_quotes": ["direct quote from the customer showing interest or concern"],
    "objections": ["any pushback or concerns raised"],
    "buying_signals": ["any positive signals indicating interest"],
    "lead_score": 7,
    "recommended_next_step": "Send quote by Friday"
}}"""
            }],
        )
        text = response.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception as e:
        logger.warning("Transcription analysis failed: %s", e)
        return {
            "summary": transcription[:200],
            "action_items": [],
            "key_quotes": [],
            "objections": [],
            "buying_signals": [],
            "lead_score": 5,
            "recommended_next_step": "Review transcription manually",
        }


def _fallback_summary(notes: str) -> dict:
    """Simple keyword-based fallback when AI is not available."""
    notes_lower = notes.lower()

    # Detect sentiment
    positive_words = ["interested", "yes", "schedule", "quote", "appointment", "great", "perfect", "love"]
    negative_words = ["no", "not interested", "don't", "remove", "stop calling", "busy"]
    pos = sum(1 for w in positive_words if w in notes_lower)
    neg = sum(1 for w in negative_words if w in notes_lower)
    sentiment = "positive" if pos > neg else "negative" if neg > pos else "neutral"

    # Score
    score = 5 + pos - neg
    score = max(1, min(10, score))

    # Action items from keywords
    actions = []
    if any(w in notes_lower for w in ["callback", "call back", "follow up", "followup"]):
        actions.append({
            "task": "Follow up call",
            "due_date": (date.today() + timedelta(days=3)).isoformat(),
            "priority": "high",
        })
    if any(w in notes_lower for w in ["quote", "estimate", "bid", "proposal"]):
        actions.append({
            "task": "Send quote/estimate",
            "due_date": (date.today() + timedelta(days=1)).isoformat(),
            "priority": "high",
        })
    if any(w in notes_lower for w in ["schedule", "appointment", "visit"]):
        actions.append({
            "task": "Schedule appointment",
            "due_date": (date.today() + timedelta(days=2)).isoformat(),
            "priority": "high",
        })

    # Summary
    summary = notes[:150].strip()
    if len(notes) > 150:
        summary += "..."

    return {
        "summary": summary,
        "action_items": actions,
        "lead_score": score,
        "sentiment": sentiment,
    }
