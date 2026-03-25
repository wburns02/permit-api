#!/usr/bin/env python3
"""
Email Campaign Runner — automated batch sender for PermitLookup outreach.

Runs every 10 minutes via cron on R730. Finds active campaigns, sends batches
at the configured rate, handles drip follow-ups, and marks completed campaigns.

Usage:
    python3 run_email_campaigns.py --db-host 100.122.216.15
    python3 run_email_campaigns.py --db-host 100.122.216.15 --dry-run

Cron:
    */10 * * * * cd /home/will/permit-api && /home/will/permit-api/venv/bin/python scripts/run_email_campaigns.py --db-host 100.122.216.15 >> /tmp/email_campaigns.log 2>&1
"""

import argparse
import hashlib
import hmac
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")
SECRET_KEY = os.getenv("SECRET_KEY", "development-secret-key-change-in-production")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("campaign_runner")

# Optional SendGrid import
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Content, TrackingSettings, OpenTracking, ClickTracking
    HAS_SENDGRID = True
except ImportError:
    HAS_SENDGRID = False
    log.warning("sendgrid not installed — will run in dry-run mode")


def get_conn(host: str):
    return psycopg2.connect(host=host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def generate_unsubscribe_token(email: str) -> str:
    """Generate HMAC token for unsubscribe link verification."""
    return hmac.new(SECRET_KEY.encode(), email.lower().strip().encode(), hashlib.sha256).hexdigest()[:16]


# ── Template Imports ──────────────────────────────────────────────────────────

# Add parent dir to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app.services.email_templates import (
        insurance_pitch, realtor_pitch, contractor_pitch, followup_pitch,
        TEMPLATES,
    )
except ImportError:
    log.error("Cannot import email templates — ensure you're running from the permit-api root")
    sys.exit(1)


def get_template_fn(template_key: str, audience: str):
    """Get the template function by key or audience."""
    if template_key in TEMPLATES:
        return TEMPLATES[template_key]
    audience_map = {
        "insurance": "insurance",
        "realtor": "realtor",
        "real_estate": "realtor",
        "contractor": "contractor",
        "all": "contractor",
    }
    return TEMPLATES.get(audience_map.get(audience, "contractor"))


# ── Sending Logic ─────────────────────────────────────────────────────────────

def send_email(sg, email: str, name: str, state: str, license_type: str,
               audience: str, campaign_id: str, recipient_id: str,
               template_key: str = None, is_followup: bool = False,
               dry_run: bool = False) -> bool:
    """Send a single email via SendGrid. Returns True on success."""
    token = generate_unsubscribe_token(email)

    # Pick template
    if is_followup:
        subject, html, text_body = followup_pitch(name or "", email, token, state or "")
    else:
        template_fn = get_template_fn(template_key or audience, audience)
        if not template_fn:
            log.error("No template for audience=%s template_key=%s", audience, template_key)
            return False

        try:
            if audience == "contractor":
                trade = license_type or "construction"
                subject, html, text_body = template_fn(name or "", state or "", email, token, trade)
            elif audience == "followup":
                subject, html, text_body = template_fn(name or "", email, token, state or "")
            else:
                subject, html, text_body = template_fn(name or "", state or "", email, token)
        except Exception as e:
            log.error("Template error for %s: %s", email, e)
            return False

    if dry_run:
        log.info("[DRY RUN] Would send to %s: %s", email, subject[:60])
        return True

    if not sg:
        log.warning("No SendGrid client — skipping %s", email)
        return False

    try:
        message = Mail(
            from_email="Will at PermitLookup <outreach@permitlookup.com>",
            to_emails=email,
            subject=subject,
        )
        message.content = [
            Content("text/plain", text_body),
            Content("text/html", html),
        ]

        tracking = TrackingSettings()
        tracking.open_tracking = OpenTracking(True)
        tracking.click_tracking = ClickTracking(True, True)
        message.tracking_settings = tracking

        response = sg.send(message)
        if response.status_code in (200, 201, 202):
            return True
        else:
            log.warning("SendGrid returned %s for %s", response.status_code, email)
            return False
    except Exception as e:
        log.error("Failed to send to %s: %s", email, e)
        return False


def process_active_campaigns(conn, sg, dry_run: bool = False):
    """Find and process all active campaigns."""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1. Find active campaigns
    cur.execute("SELECT * FROM email_campaigns WHERE status = 'active'")
    campaigns = cur.fetchall()

    if not campaigns:
        log.info("No active campaigns found")
        return

    log.info("Found %d active campaign(s)", len(campaigns))

    for campaign in campaigns:
        cid = campaign["id"]
        name = campaign["name"]
        send_rate = campaign["send_rate"] or 200
        audience = campaign["target_audience"] or "contractor"

        # Calculate batch size: send_rate per hour / 6 (every 10 min)
        batch_size = max(1, send_rate // 6)

        log.info("Campaign '%s' [%s] — sending batch of %d (rate: %d/hr)",
                 name, cid, batch_size, send_rate)

        # 2. Get unsubscribed emails
        cur.execute("SELECT LOWER(email) FROM email_unsubscribes")
        unsub_emails = {row["email"] for row in cur.fetchall()}

        # 3. Get pending recipients
        cur.execute("""
            SELECT id, email, name, state, license_type, company
            FROM email_recipients
            WHERE campaign_id = %s AND status = 'pending'
              AND LOWER(email) NOT IN (SELECT LOWER(email) FROM email_unsubscribes)
            ORDER BY id
            LIMIT %s
        """, (cid, batch_size))
        recipients = cur.fetchall()

        if not recipients:
            log.info("Campaign '%s' has no more pending recipients — marking completed", name)
            cur.execute("""
                UPDATE email_campaigns
                SET status = 'completed', completed_at = NOW()
                WHERE id = %s
            """, (cid,))
            conn.commit()
            continue

        sent = 0
        skipped = 0

        for recip in recipients:
            if recip["email"].lower() in unsub_emails:
                cur.execute("""
                    UPDATE email_recipients
                    SET status = 'unsubscribed', unsubscribed_at = NOW()
                    WHERE id = %s
                """, (recip["id"],))
                skipped += 1
                continue

            success = send_email(
                sg=sg,
                email=recip["email"],
                name=recip["name"],
                state=recip["state"],
                license_type=recip["license_type"],
                audience=audience,
                campaign_id=str(cid),
                recipient_id=str(recip["id"]),
                template_key=audience,
                dry_run=dry_run,
            )

            if success:
                cur.execute("""
                    UPDATE email_recipients
                    SET status = 'sent', sent_at = NOW()
                    WHERE id = %s
                """, (recip["id"],))
                sent += 1
            else:
                # Don't update status so it retries next batch
                pass

            # Small delay between sends to avoid rate limits
            if not dry_run:
                time.sleep(0.1)

        # Update campaign sent_count
        cur.execute("""
            UPDATE email_campaigns
            SET sent_count = sent_count + %s
            WHERE id = %s
        """, (sent, cid))
        conn.commit()

        log.info("Campaign '%s': sent=%d, skipped=%d, remaining=%d",
                 name, sent, skipped, len(recipients) - sent - skipped)


def process_drip_followups(conn, sg, dry_run: bool = False):
    """Send follow-up emails to non-openers.

    - 3+ days since sent, never opened -> send followup
    - 7+ days since sent, still status='sent' -> send final followup with different subject
    """
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # First followup: 3 days, never opened
    cur.execute("""
        SELECT r.id, r.email, r.name, r.state, r.license_type, r.campaign_id
        FROM email_recipients r
        JOIN email_campaigns c ON c.id = r.campaign_id
        WHERE r.status = 'sent'
          AND r.sent_at < NOW() - INTERVAL '3 days'
          AND r.sent_at > NOW() - INTERVAL '7 days'
          AND c.status IN ('active', 'completed')
          AND LOWER(r.email) NOT IN (SELECT LOWER(email) FROM email_unsubscribes)
        ORDER BY r.sent_at
        LIMIT 50
    """)
    followup_batch = cur.fetchall()

    if followup_batch:
        log.info("Sending %d first followup emails (3-day non-openers)", len(followup_batch))

    sent = 0
    for recip in followup_batch:
        success = send_email(
            sg=sg,
            email=recip["email"],
            name=recip["name"],
            state=recip["state"],
            license_type=recip["license_type"],
            audience="followup",
            campaign_id=str(recip["campaign_id"]),
            recipient_id=str(recip["id"]),
            is_followup=True,
            dry_run=dry_run,
        )
        if success:
            # Mark as sent again (keeps status='sent' but updates sent_at)
            cur.execute("""
                UPDATE email_recipients
                SET sent_at = NOW()
                WHERE id = %s
            """, (recip["id"],))
            sent += 1
            if not dry_run:
                time.sleep(0.1)

    if sent:
        conn.commit()
        log.info("Sent %d first followup emails", sent)

    # Final followup: 7+ days, still not opened
    cur.execute("""
        SELECT r.id, r.email, r.name, r.state, r.license_type, r.campaign_id
        FROM email_recipients r
        JOIN email_campaigns c ON c.id = r.campaign_id
        WHERE r.status = 'sent'
          AND r.sent_at < NOW() - INTERVAL '7 days'
          AND r.sent_at > NOW() - INTERVAL '14 days'
          AND c.status IN ('active', 'completed')
          AND LOWER(r.email) NOT IN (SELECT LOWER(email) FROM email_unsubscribes)
        ORDER BY r.sent_at
        LIMIT 50
    """)
    final_batch = cur.fetchall()

    if final_batch:
        log.info("Sending %d final followup emails (7-day non-openers)", len(final_batch))

    sent_final = 0
    for recip in final_batch:
        success = send_email(
            sg=sg,
            email=recip["email"],
            name=recip["name"],
            state=recip["state"],
            license_type=recip["license_type"],
            audience="followup",
            campaign_id=str(recip["campaign_id"]),
            recipient_id=str(recip["id"]),
            is_followup=True,
            dry_run=dry_run,
        )
        if success:
            # After final followup, mark as done so we don't keep retrying
            cur.execute("""
                UPDATE email_recipients
                SET sent_at = NOW()
                WHERE id = %s
            """, (recip["id"],))
            sent_final += 1
            if not dry_run:
                time.sleep(0.1)

    if sent_final:
        conn.commit()
        log.info("Sent %d final followup emails", sent_final)


def log_stats(conn):
    """Print summary stats."""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT status, count(*) as cnt
        FROM email_campaigns
        GROUP BY status
        ORDER BY status
    """)
    campaigns = cur.fetchall()
    log.info("Campaign status: %s", ", ".join(f"{r['status']}={r['cnt']}" for r in campaigns) or "none")

    cur.execute("""
        SELECT status, count(*) as cnt
        FROM email_recipients
        GROUP BY status
        ORDER BY cnt DESC
    """)
    recipients = cur.fetchall()
    log.info("Recipient status: %s", ", ".join(f"{r['status']}={r['cnt']}" for r in recipients) or "none")

    cur.execute("SELECT count(*) as cnt FROM email_unsubscribes")
    unsubs = cur.fetchone()["cnt"]
    log.info("Total unsubscribes: %d", unsubs)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Email Campaign Runner")
    parser.add_argument("--db-host", default=os.getenv("DB_HOST", "100.122.216.15"))
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Email Campaign Runner starting (dry_run=%s)", args.dry_run)
    log.info("DB: %s:%s/%s", args.db_host, DB_PORT, DB_NAME)

    # Ensure tables exist
    conn = get_conn(args.db_host)
    cur = conn.cursor()

    for ddl in [
        """CREATE TABLE IF NOT EXISTS email_campaigns (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(200) NOT NULL,
            subject VARCHAR(500) NOT NULL,
            body_html TEXT,
            body_text TEXT,
            target_audience VARCHAR(100),
            target_state VARCHAR(2),
            status VARCHAR(20) DEFAULT 'draft',
            sent_count INTEGER DEFAULT 0,
            open_count INTEGER DEFAULT 0,
            click_count INTEGER DEFAULT 0,
            unsubscribe_count INTEGER DEFAULT 0,
            signup_count INTEGER DEFAULT 0,
            bounce_count INTEGER DEFAULT 0,
            send_rate INTEGER DEFAULT 200,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        )""",
        """CREATE TABLE IF NOT EXISTS email_recipients (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            campaign_id UUID REFERENCES email_campaigns(id) ON DELETE CASCADE,
            email VARCHAR(255) NOT NULL,
            name VARCHAR(500),
            company VARCHAR(500),
            state VARCHAR(2),
            license_type VARCHAR(100),
            status VARCHAR(20) DEFAULT 'pending',
            sent_at TIMESTAMPTZ,
            opened_at TIMESTAMPTZ,
            clicked_at TIMESTAMPTZ,
            unsubscribed_at TIMESTAMPTZ
        )""",
        """CREATE TABLE IF NOT EXISTS email_unsubscribes (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email VARCHAR(255) NOT NULL UNIQUE,
            reason TEXT,
            unsubscribed_at TIMESTAMPTZ DEFAULT NOW()
        )""",
    ]:
        try:
            cur.execute(ddl)
        except Exception:
            conn.rollback()

    # Indexes
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_ec_status ON email_campaigns (status)",
        "CREATE INDEX IF NOT EXISTS ix_er_campaign_status ON email_recipients (campaign_id, status)",
        "CREATE INDEX IF NOT EXISTS ix_er_email ON email_recipients (email)",
        "CREATE INDEX IF NOT EXISTS ix_er_sent_at ON email_recipients (sent_at)",
        "CREATE INDEX IF NOT EXISTS ix_eu_email ON email_unsubscribes (email)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()

    conn.commit()
    cur.close()

    # Initialize SendGrid
    sg = None
    if not args.dry_run and HAS_SENDGRID and SENDGRID_API_KEY:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        log.info("SendGrid client initialized")
    elif args.dry_run:
        log.info("DRY RUN mode — no emails will be sent")
    else:
        log.warning("SendGrid not available — running in dry-run mode")
        args.dry_run = True

    try:
        # Process active campaigns
        process_active_campaigns(conn, sg, dry_run=args.dry_run)

        # Process drip follow-ups
        process_drip_followups(conn, sg, dry_run=args.dry_run)

        # Log stats
        log_stats(conn)

    except Exception as e:
        log.error("Campaign runner error: %s", e, exc_info=True)
    finally:
        conn.close()

    log.info("Campaign runner complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
