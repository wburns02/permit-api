#!/usr/bin/env python3
"""
Daily Scraper Report — emails a summary of all scraper results and data freshness.

Runs daily at 7:30 AM (after scrapers + loaders + freshness check).
Sends via SendGrid to the configured recipient.

Usage:
    python3 daily_scraper_report.py
    python3 daily_scraper_report.py --dry-run  # Print to stdout instead of sending
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta

import psycopg2

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
FROM_EMAIL = "alerts@permitlookup.com"
TO_EMAIL = os.getenv("REPORT_EMAIL", "willwalterburns@gmail.com")


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def build_report():
    conn = get_conn()
    cur = conn.cursor()
    today = date.today()
    yesterday = today - timedelta(days=1)

    # 1. Hot leads total + by state
    cur.execute("SELECT COUNT(*) FROM hot_leads")
    total_hot = cur.fetchone()[0]

    cur.execute("""
        SELECT state, COUNT(*) as cnt, MAX(issue_date) as latest
        FROM hot_leads
        GROUP BY state ORDER BY cnt DESC
    """)
    state_rows = cur.fetchall()

    # 2. Fresh vs stale sources
    cur.execute("""
        SELECT source, state, COUNT(*) as cnt, MAX(issue_date) as latest
        FROM hot_leads
        GROUP BY source, state ORDER BY cnt DESC LIMIT 30
    """)
    source_rows = cur.fetchall()

    fresh_sources = [r for r in source_rows if r[3] and (today - r[3]).days <= 7]
    stale_sources = [r for r in source_rows if not r[3] or (today - r[3]).days > 7]

    # 3. Hot leads loaded in last 24h (from tracking table if exists)
    new_today = 0
    try:
        cur.execute("""
            SELECT SUM(records_loaded) FROM hot_leads_sources
            WHERE loaded_at >= CURRENT_DATE - interval '24 hours'
        """)
        result = cur.fetchone()
        new_today = result[0] or 0
    except Exception:
        conn.rollback()

    # 4. Permits table total
    cur.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = 'permits'")
    total_permits = cur.fetchone()[0]

    # 5. Contractor licenses total
    cur.execute("SELECT COUNT(*) FROM contractor_licenses")
    total_contractors = cur.fetchone()[0]

    # 6. Check scraper logs on R730 (via last loader results)
    loader_results = []
    try:
        cur.execute("""
            SELECT source_name, state, records_loaded, records_skipped, latest_issue_date, loaded_at
            FROM hot_leads_sources
            WHERE loaded_at >= CURRENT_DATE - interval '24 hours'
            ORDER BY records_loaded DESC LIMIT 20
        """)
        loader_results = cur.fetchall()
    except Exception:
        conn.rollback()

    conn.close()

    # Build HTML email
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0a0a0f; color: #e8e8f0; padding: 20px; }}
            .container {{ max-width: 600px; margin: 0 auto; background: #12121a; border-radius: 12px; padding: 24px; border: 1px solid #2a2a3a; }}
            h1 {{ color: #818cf8; font-size: 22px; margin-bottom: 4px; }}
            h2 {{ color: #a0a0b8; font-size: 16px; margin-top: 24px; margin-bottom: 8px; border-bottom: 1px solid #2a2a3a; padding-bottom: 6px; }}
            .stat {{ display: inline-block; text-align: center; padding: 12px 20px; background: #1a1a25; border-radius: 8px; margin: 4px; }}
            .stat .number {{ font-size: 24px; font-weight: 700; color: #22c55e; }}
            .stat .label {{ font-size: 11px; color: #6a6a80; text-transform: uppercase; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 8px; }}
            th {{ text-align: left; padding: 6px 8px; background: #1a1a25; color: #6a6a80; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; }}
            td {{ padding: 6px 8px; border-bottom: 1px solid #1a1a25; }}
            .fresh {{ color: #22c55e; }}
            .stale {{ color: #ef4444; }}
            .warn {{ color: #f59e0b; }}
            .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }}
            .badge-green {{ background: rgba(34,197,94,.15); color: #22c55e; }}
            .badge-red {{ background: rgba(239,68,68,.15); color: #ef4444; }}
            .footer {{ margin-top: 24px; font-size: 11px; color: #6a6a80; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>PermitLookup Daily Report</h1>
            <p style="color: #6a6a80; font-size: 13px; margin-bottom: 20px;">{today.strftime('%A, %B %d, %Y')}</p>

            <div style="text-align: center; margin-bottom: 20px;">
                <div class="stat">
                    <div class="number">{total_hot:,}</div>
                    <div class="label">Hot Leads</div>
                </div>
                <div class="stat">
                    <div class="number">{total_permits:,}</div>
                    <div class="label">Total Permits</div>
                </div>
                <div class="stat">
                    <div class="number">{total_contractors:,}</div>
                    <div class="label">Contractors</div>
                </div>
                <div class="stat">
                    <div class="number">{new_today:,}</div>
                    <div class="label">Loaded Today</div>
                </div>
            </div>

            <h2>Coverage by State ({len(state_rows)} states)</h2>
            <table>
                <tr><th>State</th><th>Records</th><th>Latest Date</th><th>Status</th></tr>
    """

    for state, cnt, latest in state_rows:
        if latest:
            days_old = (today - latest).days
            status_class = "fresh" if days_old <= 7 else "warn" if days_old <= 30 else "stale"
            status_text = f"{days_old}d ago"
        else:
            status_class = "stale"
            status_text = "No date"
        html += f'<tr><td><strong>{state}</strong></td><td>{cnt:,}</td><td>{latest or "—"}</td><td class="{status_class}">{status_text}</td></tr>\n'

    html += "</table>"

    # Fresh vs stale summary
    html += f"""
            <h2>Source Health</h2>
            <p><span class="badge badge-green">{len(fresh_sources)} fresh</span> &nbsp;
               <span class="badge badge-red">{len(stale_sources)} stale</span></p>
    """

    if stale_sources:
        html += "<p style='font-size:12px;color:#ef4444;'>Stale sources: "
        html += ", ".join(f"{r[0]} ({r[1]})" for r in stale_sources[:10])
        if len(stale_sources) > 10:
            html += f" + {len(stale_sources)-10} more"
        html += "</p>"

    # Today's loader results
    if loader_results:
        html += """
            <h2>Today's Loader Results</h2>
            <table>
                <tr><th>Source</th><th>State</th><th>Loaded</th><th>Latest</th></tr>
        """
        for src, st, loaded, skipped, latest, loaded_at in loader_results:
            if loaded > 0:
                html += f'<tr><td>{src}</td><td>{st or "—"}</td><td class="fresh">{loaded:,}</td><td>{latest or "—"}</td></tr>\n'
        html += "</table>"

    html += f"""
            <div class="footer">
                <p>PermitLookup — permits.ecbtx.com</p>
                <p>Sent automatically at {datetime.now().strftime('%I:%M %p')} CST</p>
            </div>
        </div>
    </body>
    </html>
    """

    return html


def send_email(html, dry_run=False):
    subject = f"PermitLookup Daily Report — {date.today().strftime('%b %d, %Y')}"

    if dry_run:
        print(f"Subject: {subject}")
        print(f"To: {TO_EMAIL}")
        print(f"From: {FROM_EMAIL}")
        print("--- HTML body saved to /tmp/daily_report_preview.html ---")
        with open("/tmp/daily_report_preview.html", "w") as f:
            f.write(html)
        return True

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set")
        return False

    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail, Email, To, Content

        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        message = Mail(
            from_email=Email(FROM_EMAIL, "PermitLookup"),
            to_emails=To(TO_EMAIL),
            subject=subject,
            html_content=Content("text/html", html),
        )
        response = sg.send(message)
        print(f"Email sent: {response.status_code}")
        return response.status_code in (200, 201, 202)
    except Exception as e:
        print(f"Email failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"{datetime.now()} Building daily report...")
    html = build_report()
    print(f"{datetime.now()} Report built, sending...")
    success = send_email(html, dry_run=args.dry_run)
    print(f"{datetime.now()} {'Done' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
