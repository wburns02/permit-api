# Actionable AI Analyst Results

**Date:** 2026-03-27
**Status:** Approved
**Project:** PermitLookup (permits.ecbtx.com)
**File:** `app/static/index.html` (single-file frontend, ~15.5K lines)
**Backend:** `app/api/v1/analyst.py`

## Summary

Transform the AI Analyst results table from a static, read-only data dump into an interactive, actionable lead workspace. Users should be able to click any result to see full details, call the lead instantly, select multiple results for batch export, and push leads to any CRM via webhook.

## Decisions Made During Brainstorming

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Primary use case | Both individual quick-action AND batch workflow | Serves roofers calling now AND sales managers building lists |
| CRM target | Flexible (customer's own CRM) | SaaS product, every customer has their own CRM |
| Call mechanism | tel: link + Send to Dialer (soft phone is spec #2) | Ships fast, works with any phone/softphone today |
| Detail view | Slide-out panel from right | Fast scanning, no page navigation, familiar pattern |
| Panel layout | Action-first (call button dominates top 30%) | Users want to dial, not read flood zone data |
| Batch exports | CSV download, Copy to clipboard, Webhook | 80/20 coverage without building native integrations |
| Date handling | Auto-detect date field, normalize to "DATE" header | Users don't care which internal column it came from |
| Panel theme | Match app dark theme (seamless) | Clean, no visual break |

## Architecture

All changes are frontend-only except for one small backend addition (webhook_url column + webhook proxy endpoint). The existing `/v1/analyst/query` and `/v1/analyst/report/html` endpoints already return all the data we need.

### What Changes

1. **`index.html` > `analystSubmit()`** (~line 14749): Replace static table renderer with interactive table
2. **`index.html`**: Add slide-out panel HTML, batch actions bar HTML, and supporting JS functions
3. **`analyst.py`**: Add webhook_url to user model, add POST `/v1/analyst/webhook/test` and POST `/v1/analyst/webhook/send` endpoints
4. **Database**: Add `webhook_url` TEXT column to `api_keys` table

### What Doesn't Change

- The `/v1/analyst/query` endpoint (request/response format unchanged)
- The `/v1/analyst/report/html` endpoint (used as-is for "Full Report" button)
- The chat bubble UI (question/response format stays the same)
- The AI/SQL generation logic

## Layer 1: Enhanced Table

### Column Structure

The table renderer auto-arranges columns for consistency regardless of what the SQL query returned.

| Order | Column | Width | Content |
|-------|--------|-------|---------|
| 1 | Checkbox | 32px | Select row for batch actions |
| 2 | DATE | 90px | Auto-detected date, relative format ("2d ago"), full date in title tooltip |
| 3 | PERMIT | 120px | Permit number |
| 4 | ADDRESS | flex | Street address |
| 5 | CITY | 80px | City |
| 6 | ST | 40px | State code |
| 7 | ZIP | 60px | Zip code |
| 8 | DESCRIPTION | flex | Truncated to ~50 chars, full text in title tooltip |
| 9 | Phone icon | 32px | Shown only if row has a phone field, tel: link on click |

### Date Auto-Detection

Scan the column keys of `data[0]` for known date fields in priority order:
1. `issue_date`
2. `date_created`
3. `sale_date`
4. `violation_date`
5. `filing_date`
6. `install_date`
7. `expiration_date`
8. `begin_date`
9. `period_end`
10. `formation_date`

First match becomes the DATE column. If no match, no DATE column is shown.

### Column Priority Logic

Known columns are pinned in the order above. Any remaining columns from the query that are NOT in the known list are appended after DESCRIPTION (before the phone icon). This handles arbitrary SQL results gracefully.

### Phone Detection

Scan column keys for: `contractor_phone`, `applicant_phone`, `phone`. If any field has a non-empty value for a row, show the phone icon. The `tel:` link uses the first non-empty phone value found.

### Row Interactions

- **Hover**: Background changes to `rgba(139,92,246,0.06)`, cursor: pointer
- **Click**: Opens slide-out panel for that row, row gets active highlight (left border accent + subtle bg)
- **Checkbox click**: Toggles row selection (stops propagation, doesn't open panel)
- **Select All**: Checkbox in thead selects/deselects all visible rows

## Layer 2: Slide-out Detail Panel

### Structure

```
<div id="analyst-panel"> (fixed, right:0, top:0, height:100vh, width:420px, z-index:1000)
  ├── Panel Header (close button, prev/next arrows, keyboard hint)
  ├── Action Zone (~30% of viewport height)
  │   ├── Contact name (large)
  │   ├── Address
  │   ├── Phone number (large, purple accent)
  │   ├── "Call Now" button (full-width, green gradient, tel: link)
  │   └── Secondary actions row: Copy Phone | Send to Dialer | Full Report
  ├── Permit Summary
  │   ├── 2x2 grid: Permit #, Date, Valuation, Status
  │   ├── Description (full text)
  │   └── Contractor info
  └── Property Intelligence (lazy-loaded)
      ├── Skeleton loader on open
      ├── Fetches /v1/analyst/report endpoint
      └── Shows: linked permits, violations, sales, liens, flood, demographics
</div>
```

### Behavior

- **Open**: Slides in from right, 200ms ease-out transition (transform: translateX)
- **Close**: X button, Escape key, or clicking the overlay/table area
- **Navigate**: Up/Down arrows in header, or j/k keyboard shortcuts cycle through result rows
- **No phone**: Call button replaced with muted "No phone on file" text, "Full Report" promoted to primary green button
- **Intelligence loading**: Only fetched when panel opens for a row. Cached per address so re-opening same row doesn't re-fetch.
- **Panel theme**: Same dark surface as app (`var(--surface)`), no visual break from the rest of the UI

### Panel Data Mapping

The panel extracts data from the raw row object returned by the analyst query:

| Panel Field | Row Keys Checked (first non-null wins) |
|-------------|----------------------------------------|
| Name | `owner_name`, `applicant_name`, `contractor_name`, `grantee`, `debtor_name` |
| Phone | `contractor_phone`, `applicant_phone`, `phone` |
| Address | `address` |
| City | `city` |
| State | `state_code`, `state`, `state_abbrev` |
| Zip | `zip_code`, `zip` |
| Permit # | `permit_number`, `filing_number`, `document_id`, `violation_id`, `license_number` |
| Date | Auto-detected date field (same logic as table) |
| Valuation | `valuation`, `sale_price`, `amount`, `loan_amount`, `fine_amount` |
| Status | `status` |
| Description | `description`, `event_narrative` |
| Contractor | `contractor_company`, `contractor_name`, `business_name` |

## Layer 3: Batch Actions Bar

### Structure

```
<div id="analyst-batch-bar"> (fixed, bottom:0, left:0, width:100%, z-index:999)
  ├── Left: "☑ X selected" + "Select All" + "Clear" buttons
  └── Right: Action buttons
      ├── 📥 Export CSV
      ├── 📋 Copy to Clipboard (tab-delimited)
      ├── 📡 Send to Webhook (greyed if no webhook configured)
      └── 📱 Send to Dialer (shows "X of Y have phone")
</div>
```

### Behavior

- **Visibility**: Hidden when 0 rows selected. Slides up (150ms) when first row is checked.
- **Select All**: Selects all rows in current results (up to 25 displayed)
- **CSV Export**: Downloads selected rows as CSV file with all columns
- **Copy to Clipboard**: Tab-delimited text (paste into Sheets/Excel). Shows toast "Copied X rows"
- **Send to Webhook**: POSTs selected rows as JSON array to user's configured webhook URL. Goes through backend proxy (`/v1/analyst/webhook/send`) to avoid CORS issues. Shows success/fail toast.
- **Send to Dialer**: Stores selected rows with phone numbers in sessionStorage, navigates to `#dialer` page. Dialer page checks sessionStorage on init and loads queued leads.
- **Theme**: Same dark surface, 1px top border with purple accent color

### Clipboard Format

Tab-delimited with headers. Example:
```
PERMIT	ADDRESS	CITY	STATE	ZIP	DATE	DESCRIPTION	PHONE
TX-2026-D010	1717 Lake Shore Dr	Waco	TX	76708	2026-03-25	Roof replacement...	254-555-7010
```

## Layer 4: Webhook Configuration

### Frontend

Add a "CRM Integration" section to the account/settings area (or inline in the batch bar as a setup flow):

- Text input: "Webhook URL" with placeholder `https://hooks.zapier.com/...`
- "Test" button: Sends a sample payload and shows success/fail
- Help text: "Works with Zapier, Make, n8n, or any URL that accepts POST JSON"
- Webhook URL stored in localStorage keyed to the API key, AND synced to backend

### Backend Additions

**Database:**
```sql
ALTER TABLE api_keys ADD COLUMN webhook_url TEXT;
```

**New endpoints:**

`PUT /v1/analyst/webhook/config`
```json
{ "webhook_url": "https://hooks.zapier.com/..." }
```
Saves webhook URL to the user's api_keys record.

`POST /v1/analyst/webhook/test`
```json
{}
```
Sends a test payload to the configured webhook URL. Returns success/failure.

`POST /v1/analyst/webhook/send`
```json
{ "rows": [ { ... }, { ... } ] }
```
Proxies the selected rows to the user's webhook URL. Returns delivery status. This avoids CORS issues since the browser can't POST to arbitrary webhook URLs directly.

### Webhook Payload Format

```json
{
  "source": "PermitLookup AI Analyst",
  "timestamp": "2026-03-27T14:30:00Z",
  "count": 4,
  "leads": [
    {
      "permit_number": "TX-2026-D010",
      "address": "1717 Lake Shore Dr",
      "city": "Waco",
      "state": "TX",
      "zip": "76708",
      "description": "Roof replacement...",
      "date": "2026-03-25",
      "valuation": 14200,
      "contact_name": "Brian Hall",
      "phone": "254-555-7010",
      "contractor": "...",
      "source_query": "Show me 10 new permits..."
    }
  ]
}
```

## Implementation Order

1. Enhanced table renderer (replaces current table HTML generation in `analystSubmit()`)
2. Slide-out panel (HTML + CSS + JS, all in index.html)
3. Batch actions bar (HTML + CSS + JS)
4. Backend webhook endpoints + database migration
5. Webhook config UI
6. Dialer integration (sessionStorage handoff)

## Out of Scope

- Built-in soft phone (Twilio WebRTC) — spec #2
- Native CRM integrations (GHL, HubSpot, Salesforce) — webhook covers them
- Call logging/recording — comes with soft phone
- AI call scripts/talking points — future feature
- Email actions — future feature
- Map view of results — future feature
