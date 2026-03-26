#!/usr/bin/env python3
"""Seed demo data for CRM contacts, deals, and dialer queue.

Inserts realistic demo data into the PermitLookup database so that the CRM
and Dialer pages look populated out of the box.

Tables seeded:
  - contacts      (15 rows)  — CRM contact cards
  - deals         (5 rows)   — pipeline deals at various stages
  - hot_leads     (20 rows)  — dialer queue leads (TX permits)
  - call_logs     (8 rows)   — sample call history
  - lead_statuses (8 rows)   — lead dispositions for the demo user

Idempotent: checks for a sentinel contact (demo-seed@permitlookup.com) before
inserting.  Re-run safely.

Usage:
    python3 scripts/seed_demo_data.py
"""

import sys
from uuid import uuid4
from datetime import datetime, timedelta, date, timezone

import psycopg2

# ---------------------------------------------------------------------------
# Connection — T430 primary (writes go here, not the replica)
# ---------------------------------------------------------------------------
DB_URL = "postgresql://will@192.168.7.83:5432/permits"

# ---------------------------------------------------------------------------
# Demo user — use the existing demo@permitlookup.com user
# ---------------------------------------------------------------------------
DEMO_USER_ID = "100dd7e5-31b5-4ed9-b95f-53dfda37e9f5"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
now = datetime.now(timezone.utc)


def uid():
    return str(uuid4())


def past_date(days_ago: int) -> date:
    return (now - timedelta(days=days_ago)).date()


def past_ts(days_ago: int) -> datetime:
    return now - timedelta(days=days_ago)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

CONTACTS = [
    # (name, company, phone, email, address, city, state, zip, lead_source, tags)
    ("demo-seed@permitlookup.com", "Marcus Johnson", "Lone Star Roofing LLC", "512-555-0142", "marcus@lonestarroofing.com", "4201 S Congress Ave", "Austin", "TX", "78745", "permit", '["roofing","commercial"]'),
    (None, "Sandra Williams", "Hill Country HVAC", "210-555-0198", "sandra.w@hchvac.com", "1830 Fredericksburg Rd", "San Antonio", "TX", "78201", "permit", '["hvac","residential"]'),
    (None, "Robert Chen", "Chen & Associates Realty", "713-555-0234", "rchen@chenrealty.com", "2400 Westheimer Rd", "Houston", "TX", "77098", "referral", '["investor","multi-family"]'),
    (None, "Lisa Morales", "Morales General Contracting", "817-555-0167", "lisa@moralesgc.com", "901 W Magnolia Ave", "Fort Worth", "TX", "76104", "permit", '["general-contractor"]'),
    (None, "James Patterson", "Patterson Insurance Group", "214-555-0321", "jpatterson@piginsurance.com", "3000 Maple Ave Ste 400", "Dallas", "TX", "75201", "cold-call", '["insurance","commercial"]'),
    (None, "Angela Davis", "Davis Solar Installations", "512-555-0456", "angela@davissolar.com", "7800 Shoal Creek Blvd", "Austin", "TX", "78757", "permit", '["solar","residential"]'),
    (None, "David Nguyen", "Nguyen Plumbing Services", "281-555-0189", "david@nguyenplumbing.com", "15200 Highway 3", "Webster", "TX", "77598", "permit", '["plumbing"]'),
    (None, "Rachel Torres", "Torres Construction", "956-555-0278", "rtorres@torresconst.com", "801 N Main St", "McAllen", "TX", "78501", "permit", '["general-contractor","commercial"]'),
    (None, "Michael Brown", "Brown Electric Co", "972-555-0345", "mbrown@brownelectric.com", "1500 N Central Expy", "Richardson", "TX", "75080", "permit", '["electrical","industrial"]'),
    (None, "Jennifer Kim", "Bayou City Restorations", "832-555-0412", "jkim@bayoucityrest.com", "3100 Richmond Ave", "Houston", "TX", "77098", "website", '["restoration","insurance-work"]'),
    (None, "Carlos Hernandez", "Hernandez Framing & Drywall", "210-555-0534", "carlos@hfd-sa.com", "5600 Bandera Rd", "San Antonio", "TX", "78238", "permit", '["framing","drywall"]'),
    (None, "Amy Foster", "Foster Property Management", "469-555-0623", "afoster@fosterpm.com", "2200 Ross Ave", "Dallas", "TX", "75201", "referral", '["property-management","multi-family"]'),
    (None, "William Scott", "Scott Mechanical LLC", "817-555-0789", "wscott@scottmech.com", "600 E Weatherford St", "Fort Worth", "TX", "76102", "permit", '["hvac","commercial"]'),
    (None, "Patricia Lane", "Lane Homes Inc", "512-555-0890", "plane@lanehomes.com", "11600 Research Blvd", "Austin", "TX", "78759", "website", '["builder","residential"]'),
    (None, "Thomas Wright", "Wright Roofing & Siding", "361-555-0156", "twright@wrightrs.com", "4500 S Padre Island Dr", "Corpus Christi", "TX", "78411", "permit", '["roofing","siding"]'),
]

# The first entry uses the email field as sentinel; fix it up
# sentinel email is stored as 'demo-seed@permitlookup.com' in the first tuple's first slot

DEAL_STAGES = ["new_lead", "qualified", "proposal", "negotiation", "won"]

HOT_LEADS = [
    # (permit_number, permit_type, work_class, description, address, city, state, zip, county, issue_date, valuation, sqft, contractor_company, contractor_name, contractor_phone, contractor_trade, applicant_name, owner_name, jurisdiction, source)
    ("TX-2026-D001", "BP", "New", "New single-family residence, 2800 sqft", "1420 Bluebonnet Ln", "Round Rock", "TX", "78664", "Williamson", past_date(3), 485000, 2800, "Hill Country Builders", "Tom Garrison", "512-555-7001", "general", "Tom Garrison", "Bluebonnet Development LLC", "Round Rock", "demo-seed"),
    ("TX-2026-D002", "BP", "Alteration", "Roof replacement — hail damage repair", "3208 Oak Trail Dr", "Plano", "TX", "75074", "Collin", past_date(2), 18500, None, "DFW Storm Repair", "Kevin Nash", "972-555-7002", "roofing", "Kevin Nash", "Sarah Mitchell", "Plano", "demo-seed"),
    ("TX-2026-D003", "MP", "New", "HVAC system install — 5-ton split unit", "8901 Westover Hills Blvd", "San Antonio", "TX", "78251", "Bexar", past_date(5), 12000, None, "Comfort Zone HVAC", "Luis Ramirez", "210-555-7003", "hvac", "Luis Ramirez", "Westover Commons LP", "San Antonio", "demo-seed"),
    ("TX-2026-D004", "EP", "Alteration", "200 amp electrical panel upgrade", "621 Pecan St", "Austin", "TX", "78702", "Travis", past_date(1), 4500, None, "Spark Electric", "Ray Phillips", "512-555-7004", "electrical", "Ray Phillips", "Maria Santos", "Austin", "demo-seed"),
    ("TX-2026-D005", "PP", "New", "Complete plumbing — new construction duplex", "1105 Heights Blvd", "Houston", "TX", "77008", "Harris", past_date(4), 32000, 3200, "Gulf Coast Plumbing", "James Reed", "713-555-7005", "plumbing", "James Reed", "Heights Living LLC", "Houston", "demo-seed"),
    ("TX-2026-D006", "BP", "Alteration", "Solar panel installation — 12kW rooftop system", "2340 Ridgecrest Dr", "Leander", "TX", "78641", "Williamson", past_date(2), 28000, None, "SunTex Solar", "Mike Andrews", "512-555-7006", "solar", "Mike Andrews", "David & Karen Cole", "Leander", "demo-seed"),
    ("TX-2026-D007", "BP", "New", "Commercial build-out — dental office 2400 sqft", "4510 Medical Dr Ste 200", "San Antonio", "TX", "78229", "Bexar", past_date(6), 320000, 2400, "Alamo Commercial GC", "Pete Morales", "210-555-7007", "general", "Pete Morales", "Lone Star Dental PA", "San Antonio", "demo-seed"),
    ("TX-2026-D008", "BP", "Alteration", "Kitchen remodel — full gut renovation", "903 Exposition Blvd", "Austin", "TX", "78703", "Travis", past_date(3), 85000, 450, "Barton Creek Renovations", "Steve Hardy", "512-555-7008", "general", "Steve Hardy", "John & Megan Taylor", "Austin", "demo-seed"),
    ("TX-2026-D009", "BP", "New", "4-unit townhome development", "2800 White Oak Dr", "Houston", "TX", "77007", "Harris", past_date(7), 1200000, 6400, "Midtown Development Group", "Aaron Park", "832-555-7009", "general", "Aaron Park", "White Oak Ventures LLC", "Houston", "demo-seed"),
    ("TX-2026-D010", "BP", "Alteration", "Roof replacement — 40 square comp shingle", "1717 Lake Shore Dr", "Waco", "TX", "76708", "McLennan", past_date(1), 14200, None, "Central TX Roofing", "Brian Hall", "254-555-7010", "roofing", "Brian Hall", "Mark & Linda Owens", "Waco", "demo-seed"),
    ("TX-2026-D011", "MP", "Alteration", "Ductwork replacement — 3500 sqft home", "5402 Preston Oaks Rd", "Dallas", "TX", "75254", "Dallas", past_date(4), 9800, None, "Premier Air Systems", "Joe Walker", "214-555-7011", "hvac", "Joe Walker", "Richard Bennett", "Dallas", "demo-seed"),
    ("TX-2026-D012", "EP", "New", "Electrical — new warehouse 8000 sqft", "7700 John Ralston Rd", "Houston", "TX", "77049", "Harris", past_date(5), 65000, 8000, "Industrial Electric Inc", "Sam Cruz", "281-555-7012", "electrical", "Sam Cruz", "East Houston Logistics", "Houston", "demo-seed"),
    ("TX-2026-D013", "PP", "Alteration", "Sewer line replacement — 80 ft lateral", "310 S Zang Blvd", "Dallas", "TX", "75208", "Dallas", past_date(2), 8500, None, "Metroplex Plumbing", "Dan Foster", "469-555-7013", "plumbing", "Dan Foster", "Teresa Gomez", "Dallas", "demo-seed"),
    ("TX-2026-D014", "BP", "Alteration", "Commercial solar — 50kW rooftop array", "1200 E 6th St", "Austin", "TX", "78702", "Travis", past_date(3), 95000, None, "Capital Solar Co", "Greg Martin", "512-555-7014", "solar", "Greg Martin", "East Side Partners LLC", "Austin", "demo-seed"),
    ("TX-2026-D015", "BP", "New", "Custom home — 4200 sqft with pool", "401 Barton Creek Blvd", "Austin", "TX", "78735", "Travis", past_date(8), 950000, 4200, "Prestige Custom Homes", "Mark Evans", "512-555-7015", "general", "Mark Evans", "James & Rebecca Stone", "Austin", "demo-seed"),
    ("TX-2026-D016", "BP", "Alteration", "Storm damage repair — roof and siding", "2203 N Navarro St", "Victoria", "TX", "77901", "Victoria", past_date(1), 22000, None, "Crossroads Restoration", "Tony Blake", "361-555-7016", "roofing", "Tony Blake", "Dorothy Keller", "Victoria", "demo-seed"),
    ("TX-2026-D017", "MP", "New", "HVAC — new 3-story office building", "500 Throckmorton St", "Fort Worth", "TX", "76102", "Tarrant", past_date(6), 180000, 15000, "Cowtown Mechanical", "Bill Thompson", "817-555-7017", "hvac", "Bill Thompson", "Sundance Square LP", "Fort Worth", "demo-seed"),
    ("TX-2026-D018", "EP", "Alteration", "EV charging station — 8 Level 2 ports", "9500 N Central Expy", "Dallas", "TX", "75231", "Dallas", past_date(2), 42000, None, "Charge DFW Electric", "Ryan Cole", "214-555-7018", "electrical", "Ryan Cole", "NorthPark Mall Mgmt", "Dallas", "demo-seed"),
    ("TX-2026-D019", "PP", "Alteration", "Backflow preventer installation — commercial", "300 E Main St", "Round Rock", "TX", "78664", "Williamson", past_date(4), 3200, None, "Round Rock Plumbing", "Nick Dunn", "512-555-7019", "plumbing", "Nick Dunn", "Main Street Shops LLC", "Round Rock", "demo-seed"),
    ("TX-2026-D020", "BP", "Alteration", "Pool house addition — 600 sqft", "8100 Chalk Knoll Dr", "Austin", "TX", "78735", "Travis", past_date(3), 120000, 600, "Austin Pool Builders", "Chris Long", "512-555-7020", "general", "Chris Long", "Michael & Amy Harris", "Austin", "demo-seed"),
]


def main():
    print(f"Connecting to {DB_URL} ...")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # ------------------------------------------------------------------
    # Idempotency check — look for sentinel contact
    # ------------------------------------------------------------------
    cur.execute(
        "SELECT COUNT(*) FROM contacts WHERE email = 'demo-seed@permitlookup.com'"
    )
    if cur.fetchone()[0] > 0:
        print("Demo data already exists (sentinel contact found). Skipping.")
        conn.close()
        return

    # ------------------------------------------------------------------
    # Verify demo user exists
    # ------------------------------------------------------------------
    cur.execute("SELECT id FROM api_users WHERE id = %s", (DEMO_USER_ID,))
    if cur.fetchone() is None:
        print(f"ERROR: Demo user {DEMO_USER_ID} not found in api_users.")
        conn.close()
        sys.exit(1)

    # ------------------------------------------------------------------
    # Insert contacts
    # ------------------------------------------------------------------
    contact_ids = []
    for i, c in enumerate(CONTACTS):
        sentinel, name, company, phone, email, address, city, state, zip_, lead_source, tags = c
        # First contact uses sentinel email; rest use their own email
        actual_email = sentinel if sentinel else email
        cid = uid()
        contact_ids.append(cid)
        cur.execute(
            """
            INSERT INTO contacts (id, user_id, name, company, phone, email, address, city, state, zip, lead_source, tags, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            """,
            (cid, DEMO_USER_ID, name, company, phone, actual_email, address, city, state, zip_, lead_source, tags, past_ts(30 - i), past_ts(15 - i if i < 15 else 0)),
        )
    print(f"  Inserted {len(contact_ids)} contacts")

    # ------------------------------------------------------------------
    # Insert deals (link to first 5 contacts)
    # ------------------------------------------------------------------
    deal_data = [
        ("Lone Star Roofing — 50 unit complex bid", "new_lead", 245000, "BP-2026-44123", "BP", 30, None),
        ("Hill Country HVAC — hospital wing retrofit", "qualified", 180000, "MP-2026-33210", "MP", 21, None),
        ("Chen Realty — 12-unit condo reno inspection", "proposal", 95000, "BP-2026-55780", "BP", 14, None),
        ("Morales GC — municipal rec center", "negotiation", 520000, "BP-2026-66401", "BP", 7, None),
        ("Patterson Insurance — claims inspections contract", "won", 36000, None, None, 45, 3),
    ]
    deal_ids = []
    for i, (title, stage, value, permit_num, permit_type, created_ago, closed_ago) in enumerate(deal_data):
        did = uid()
        deal_ids.append(did)
        exp_close = past_date(-14 + i * 7)  # future dates
        actual_close = past_date(closed_ago) if closed_ago is not None else None
        cur.execute(
            """
            INSERT INTO deals (id, user_id, contact_id, title, stage, value, permit_number, permit_type, expected_close_date, actual_close_date, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (did, DEMO_USER_ID, contact_ids[i], title, stage, value, permit_num, permit_type, exp_close, actual_close, past_ts(created_ago), past_ts(max(0, created_ago - 5))),
        )
    print(f"  Inserted {len(deal_ids)} deals")

    # ------------------------------------------------------------------
    # Insert CRM notes for the deals
    # ------------------------------------------------------------------
    notes_data = [
        (contact_ids[0], deal_ids[0], "Initial meeting scheduled for next Tuesday. Marcus interested in annual contract for all roofing permits.", "note"),
        (contact_ids[1], deal_ids[1], "Sandra confirmed budget approval. Need to send revised scope by Friday.", "note"),
        (contact_ids[2], deal_ids[2], "Robert wants property inspection reports bundled with permit data. Sent proposal.", "note"),
        (contact_ids[3], deal_ids[3], "Lisa's team reviewing the contract. Follow up Wednesday.", "call"),
        (contact_ids[4], deal_ids[4], "CLOSED WON - James signed 12-month data subscription. $3K/mo.", "note"),
    ]
    for cid, did, content, note_type in notes_data:
        cur.execute(
            """
            INSERT INTO crm_notes (id, user_id, contact_id, deal_id, content, note_type, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (uid(), DEMO_USER_ID, cid, did, content, note_type, past_ts(5)),
        )
    print(f"  Inserted {len(notes_data)} CRM notes")

    # ------------------------------------------------------------------
    # Insert hot_leads (dialer queue — TX permits)
    # ------------------------------------------------------------------
    hot_lead_ids = []
    for hl in HOT_LEADS:
        hlid = uid()
        hot_lead_ids.append(hlid)
        (permit_number, permit_type, work_class, description, address, city,
         state, zip_, county, issue_date, valuation, sqft,
         contractor_company, contractor_name, contractor_phone,
         contractor_trade, applicant_name, owner_name, jurisdiction, source) = hl
        cur.execute(
            """
            INSERT INTO hot_leads
                (id, permit_number, permit_type, work_class, description,
                 address, city, state, zip, county,
                 issue_date, valuation, sqft,
                 contractor_company, contractor_name, contractor_phone,
                 contractor_trade, applicant_name, owner_name,
                 jurisdiction, source, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (hlid, permit_number, permit_type, work_class, description,
             address, city, state, zip_, county,
             issue_date, valuation, sqft,
             contractor_company, contractor_name, contractor_phone,
             contractor_trade, applicant_name, owner_name,
             jurisdiction, source, date.today()),
        )
    print(f"  Inserted {len(hot_lead_ids)} hot_leads (dialer queue)")

    # ------------------------------------------------------------------
    # Insert call_logs + lead_statuses for 8 of those hot_leads
    # so the dialer history page is populated
    # ------------------------------------------------------------------
    call_data = [
        # (hot_lead_idx, phone, duration, disposition, notes, days_ago)
        (0, "512-555-7001", 180, "connected", "Spoke with Tom — interested in annual permit data sub. Sending proposal.", 2),
        (1, "972-555-7002", 0, "no_answer", None, 3),
        (2, "210-555-7003", 45, "voicemail", "Left VM about HVAC permit data package.", 2),
        (3, "512-555-7004", 240, "connected", "Ray wants a demo next week. Very interested in electrical permit alerts.", 1),
        (4, "713-555-7005", 0, "wrong_number", "Number disconnected — need updated contact.", 4),
        (5, "512-555-7006", 120, "callback", "Mike asked to call back Thursday afternoon.", 1),
        (6, "210-555-7007", 300, "sold", "Pete signed up for Explorer plan on the call! $199/mo.", 1),
        (7, "512-555-7008", 90, "connected", "Steve wants to think about it. Will follow up next week.", 2),
    ]

    status_map = {
        "connected": "contacted",
        "voicemail": "contacted",
        "no_answer": "contacted",
        "wrong_number": "contacted",
        "callback": "callback",
        "sold": "won",
    }

    for hl_idx, phone, duration, disposition, notes, days_ago in call_data:
        lead_id = hot_lead_ids[hl_idx]
        clid = uid()

        callback_date = None
        if disposition == "callback":
            callback_date = past_ts(-2)  # 2 days in the future

        cur.execute(
            """
            INSERT INTO call_logs
                (id, user_id, lead_id, phone_number, duration_seconds,
                 disposition, notes, callback_date, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (clid, DEMO_USER_ID, lead_id, phone, duration,
             disposition, notes, callback_date, past_ts(days_ago)),
        )

        # Upsert lead_status
        mapped_status = status_map.get(disposition, "contacted")
        cur.execute(
            """
            INSERT INTO lead_statuses (id, user_id, lead_id, status, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, lead_id) DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
            """,
            (uid(), DEMO_USER_ID, lead_id, mapped_status, past_ts(days_ago)),
        )

    print(f"  Inserted {len(call_data)} call_logs + lead_statuses")

    # ------------------------------------------------------------------
    # Commit
    # ------------------------------------------------------------------
    conn.commit()
    cur.close()
    conn.close()

    print()
    print("Done! Seeded:")
    print(f"  - {len(contact_ids)} CRM contacts")
    print(f"  - {len(deal_ids)} deals (pipeline stages: {', '.join(DEAL_STAGES)})")
    print(f"  - {len(notes_data)} CRM notes")
    print(f"  - {len(hot_lead_ids)} hot_leads (TX dialer queue)")
    print(f"  - {len(call_data)} call_logs + lead_statuses")


if __name__ == "__main__":
    main()
