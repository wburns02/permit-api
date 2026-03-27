# Actionable AI Analyst Results — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the AI Analyst results table into an interactive lead workspace with clickable rows, slide-out detail panel, batch actions (CSV/clipboard/webhook), and CRM webhook integration.

**Architecture:** All frontend code lives in `app/static/index.html` (vanilla JS, ~15.5K lines). Backend is FastAPI at `app/api/v1/analyst.py`. The existing `analystSubmit()` function (~line 14689) renders a static table. We replace the table renderer, add a slide-out panel, add a batch actions bar, and add 3 small backend endpoints for webhook support. The existing `deliver_webhook()` utility in `app/services/webhook_delivery.py` handles the actual HTTP POST with retries.

**Tech Stack:** Vanilla JS (no framework), FastAPI, SQLAlchemy async, httpx (webhook delivery), PostgreSQL

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `app/static/index.html` | Modify (~lines 14748-14805) | Replace table renderer, add panel HTML/CSS/JS, add batch bar, add webhook config UI |
| `app/api/v1/analyst.py` | Modify (append new endpoints) | Add webhook config, test, and send endpoints |
| `app/models/api_key.py` | Modify (add column) | Add `webhook_url` column to `ApiUser` model |

No new files needed. No alembic migrations (project uses `Base.metadata.create_all` at startup in `database.py:init_db()`). The new column is added to the ORM model and will be picked up automatically, but since the table already exists in production, we need a one-line raw SQL migration in the startup or a manual `ALTER TABLE`.

---

### Task 1: Add `webhook_url` Column to ApiUser Model

**Files:**
- Modify: `app/models/api_key.py:54-66` (ApiUser class)
- Modify: `app/api/v1/analyst.py` (append webhook endpoints)

- [ ] **Step 1: Add webhook_url column to ApiUser model**

In `app/models/api_key.py`, add a `webhook_url` column to the `ApiUser` class, after the `stripe_subscription_id` line:

```python
# In class ApiUser(Base), after line 62:
webhook_url = Column(String(500))  # CRM webhook URL for batch export
```

The import `String` is already available from the existing `from sqlalchemy import` line.

- [ ] **Step 2: Add auto-migration for the new column in main.py**

In `app/main.py`, after the existing `init_db()` call in the startup event, add a safe `ALTER TABLE` that adds the column if it doesn't exist. Find the startup event handler (search for `init_db` or `@app.on_event("startup")` or `lifespan`) and add:

```python
# After init_db() call:
from sqlalchemy import text
async with primary_engine.begin() as conn:
    await conn.execute(text(
        "ALTER TABLE api_users ADD COLUMN IF NOT EXISTS webhook_url VARCHAR(500)"
    ))
```

- [ ] **Step 3: Add webhook endpoints to analyst.py**

Append these 3 endpoints at the end of `app/api/v1/analyst.py`:

```python
# ---------------------------------------------------------------------------
# Webhook Configuration & Delivery
# ---------------------------------------------------------------------------

class WebhookConfigRequest(BaseModel):
    webhook_url: str | None = Field(None, max_length=500)


class WebhookSendRequest(BaseModel):
    rows: list[dict]
    source_query: str = ""


@router.put("/webhook/config")
async def configure_webhook(
    body: WebhookConfigRequest,
    user: ApiUser = Depends(get_current_user),
):
    """Save or clear the user's CRM webhook URL."""
    _require_pro_leads(user)

    from app.database import primary_session_maker
    from sqlalchemy import update
    from app.models.api_key import ApiUser as ApiUserModel

    async with primary_session_maker() as db:
        await db.execute(
            update(ApiUserModel)
            .where(ApiUserModel.id == user.id)
            .values(webhook_url=body.webhook_url)
        )
        await db.commit()

    return {"status": "ok", "webhook_url": body.webhook_url}


@router.get("/webhook/config")
async def get_webhook_config(
    user: ApiUser = Depends(get_current_user),
):
    """Get the user's current webhook URL."""
    _require_pro_leads(user)

    from app.database import replica_session_maker
    from sqlalchemy import select
    from app.models.api_key import ApiUser as ApiUserModel

    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiUserModel.webhook_url).where(ApiUserModel.id == user.id)
        )
        url = result.scalar_one_or_none()

    return {"webhook_url": url}


@router.post("/webhook/test")
async def test_webhook(
    user: ApiUser = Depends(get_current_user),
):
    """Send a test payload to the user's configured webhook URL."""
    _require_pro_leads(user)

    from app.database import replica_session_maker
    from sqlalchemy import select
    from app.models.api_key import ApiUser as ApiUserModel
    from app.services.webhook_delivery import deliver_webhook

    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiUserModel.webhook_url).where(ApiUserModel.id == user.id)
        )
        url = result.scalar_one_or_none()

    if not url:
        raise HTTPException(status_code=400, detail="No webhook URL configured. Set one first via PUT /analyst/webhook/config.")

    test_payload = {
        "source": "PermitLookup AI Analyst",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "test",
        "count": 1,
        "leads": [{
            "permit_number": "TEST-001",
            "address": "123 Test Street",
            "city": "Austin",
            "state": "TX",
            "zip": "78701",
            "description": "Test webhook payload from PermitLookup",
            "date": "2026-03-27",
            "valuation": 50000,
            "contact_name": "Test Contact",
            "phone": "555-000-0000",
            "contractor": "Test Contractor LLC",
            "source_query": "Webhook test",
        }],
    }

    success = await deliver_webhook(url, test_payload)
    if not success:
        raise HTTPException(status_code=502, detail="Webhook delivery failed. Check that the URL accepts POST requests with JSON body.")

    return {"status": "ok", "message": "Test payload delivered successfully."}


@router.post("/webhook/send")
async def send_to_webhook(
    body: WebhookSendRequest,
    user: ApiUser = Depends(get_current_user),
):
    """Proxy selected analyst rows to the user's configured webhook URL."""
    _require_pro_leads(user)

    if not body.rows:
        raise HTTPException(status_code=400, detail="No rows to send.")
    if len(body.rows) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 rows per webhook send.")

    from app.database import replica_session_maker
    from sqlalchemy import select
    from app.models.api_key import ApiUser as ApiUserModel
    from app.services.webhook_delivery import deliver_webhook

    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiUserModel.webhook_url).where(ApiUserModel.id == user.id)
        )
        url = result.scalar_one_or_none()

    if not url:
        raise HTTPException(status_code=400, detail="No webhook URL configured.")

    payload = {
        "source": "PermitLookup AI Analyst",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "count": len(body.rows),
        "leads": body.rows,
        "source_query": body.source_query,
    }

    success = await deliver_webhook(url, payload)
    if not success:
        raise HTTPException(status_code=502, detail="Webhook delivery failed after retries.")

    return {"status": "ok", "delivered": len(body.rows)}
```

- [ ] **Step 4: Verify backend starts**

```bash
cd /home/will/permit-api && source backend_venv/bin/activate 2>/dev/null || true && python -c "from app.api.v1.analyst import router; print('Router loaded, endpoints:', [r.path for r in router.routes])"
```

Expected: List includes `/analyst/webhook/config`, `/analyst/webhook/test`, `/analyst/webhook/send`.

- [ ] **Step 5: Commit**

```bash
cd /home/will/permit-api
git add app/models/api_key.py app/api/v1/analyst.py app/main.py
git commit -m "feat: add webhook config/test/send endpoints for analyst CRM export"
```

---

### Task 2: Enhanced Interactive Table Renderer

**Files:**
- Modify: `app/static/index.html` (~lines 14748-14760, the `tableHtml` block inside `analystSubmit()`)

This task replaces the static table with an interactive one that has checkboxes, auto-detected date column, prioritized columns, phone icons, and clickable rows.

- [ ] **Step 1: Add CSS for the interactive table and batch bar**

Find the closing `</style>` tag inside the `page-analyst` section (around line 4806, after the `.analyst-chip:hover` rule). Insert these styles just before that `</style>`:

```css
/* Analyst interactive table */
.analyst-table{width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap}
.analyst-table thead th{padding:8px 10px;text-align:left;background:var(--surface2);border-bottom:1px solid var(--border);font-weight:700;color:var(--text3);text-transform:uppercase;font-size:10px;letter-spacing:.5px;position:sticky;top:0;z-index:1}
.analyst-table tbody tr{cursor:pointer;transition:background .15s}
.analyst-table tbody tr:hover{background:rgba(99,102,241,.06)}
.analyst-table tbody tr.analyst-row-active{background:rgba(99,102,241,.1);border-left:2px solid var(--accent)}
.analyst-table tbody tr.analyst-row-selected{background:rgba(99,102,241,.04)}
.analyst-table tbody td{padding:6px 10px;border-bottom:1px solid var(--border);color:var(--text);max-width:220px;overflow:hidden;text-overflow:ellipsis}
.analyst-table .col-check{width:32px;text-align:center}
.analyst-table .col-date{width:90px;color:var(--text2)}
.analyst-table .col-permit{width:120px;font-weight:600;color:var(--accent2)}
.analyst-table .col-city{width:80px}.analyst-table .col-st{width:40px}.analyst-table .col-zip{width:60px}
.analyst-table .col-phone{width:32px;text-align:center}
.analyst-table .col-phone a{color:var(--green);text-decoration:none;font-size:14px}
.analyst-table .col-phone a:hover{filter:brightness(1.3)}
.analyst-table input[type="checkbox"]{accent-color:var(--accent);width:14px;height:14px;cursor:pointer}
.analyst-date-relative{color:var(--text2);font-variant-numeric:tabular-nums}
/* Batch actions bar */
#analyst-batch-bar{position:fixed;bottom:0;left:0;right:0;z-index:999;background:var(--surface);border-top:1px solid var(--accent);padding:10px 20px;display:flex;align-items:center;justify-content:space-between;transform:translateY(100%);transition:transform .15s ease;font-size:13px}
#analyst-batch-bar.visible{transform:translateY(0)}
#analyst-batch-bar .batch-left{display:flex;align-items:center;gap:12px;color:var(--text)}
#analyst-batch-bar .batch-right{display:flex;align-items:center;gap:8px}
#analyst-batch-bar button{padding:6px 14px;border-radius:6px;border:1px solid var(--border);background:var(--surface2);color:var(--text2);font-size:12px;cursor:pointer;transition:all .15s;font-family:var(--font)}
#analyst-batch-bar button:hover{border-color:var(--accent);color:var(--accent2)}
#analyst-batch-bar button:disabled{opacity:.4;cursor:not-allowed}
#analyst-batch-bar .batch-count{font-weight:700;color:var(--accent2)}
```

- [ ] **Step 2: Add the batch actions bar HTML**

Find the `<div id="analyst-chat"` element (around line 4821). Just BEFORE it (but still inside `page-analyst`), insert the batch bar HTML:

```html
<!-- Batch actions bar -->
<div id="analyst-batch-bar">
  <div class="batch-left">
    <span class="batch-count" id="analyst-batch-count">0 selected</span>
    <button onclick="analystSelectAll()">Select All</button>
    <button onclick="analystClearSelection()">Clear</button>
  </div>
  <div class="batch-right">
    <button onclick="analystBatchCsv()">&#x1f4e5; Export CSV</button>
    <button onclick="analystBatchClipboard()">&#x1f4cb; Copy to Clipboard</button>
    <button onclick="analystBatchWebhook()" id="analyst-batch-webhook-btn" disabled title="Configure webhook in settings first">&#x1f4e1; Send to Webhook</button>
    <button onclick="analystBatchDialer()" id="analyst-batch-dialer-btn">&#x1f4f1; Send to Dialer</button>
  </div>
</div>
```

- [ ] **Step 3: Replace the table renderer in analystSubmit()**

In the `analystSubmit()` function, find the block that builds `tableHtml` (around lines 14748-14760). It currently looks like:

```javascript
    let tableHtml = '';
    if (data.data && data.data.length > 0) {
      const cols = Object.keys(data.data[0]);
      tableHtml = `<div style="overflow-x:auto;margin-top:16px;border:1px solid var(--border);border-radius:8px">
        <table style="width:100%;border-collapse:collapse;font-size:12px;white-space:nowrap">
          <thead><tr>${cols.map(c => `<th style="padding:8px 12px;text-align:left;background:var(--surface2);border-bottom:1px solid var(--border);font-weight:700;color:var(--text2);text-transform:uppercase;font-size:10px;letter-spacing:.5px">${escapeHtml(c)}</th>`).join('')}</tr></thead>
          <tbody>${data.data.slice(0, 25).map(row => `<tr>${cols.map(c => `<td style="padding:6px 12px;border-bottom:1px solid var(--border);color:var(--text)">${row[c] != null ? escapeHtml(String(row[c])) : '<span style="color:var(--text3)">--</span>'}</td>`).join('')}</tr>`).join('')}</tbody>
        </table>
      </div>`;
      if (data.data.length > 25) {
        tableHtml += `<div style="font-size:11px;color:var(--text3);text-align:center;padding:8px">Showing 25 of ${data.row_count} results</div>`;
      }
    }
```

Replace that entire block with:

```javascript
    let tableHtml = '';
    if (data.data && data.data.length > 0) {
      // Store data globally for panel/batch access
      const tblId = queryId;
      window['_analyst_rows_' + tblId] = data.data;
      window['_analyst_selected_' + tblId] = new Set();
      window._analyst_active_table = tblId;
      window._analyst_active_row = -1;

      // Auto-detect date column
      const DATE_FIELDS = ['issue_date','date_created','sale_date','violation_date','filing_date','install_date','expiration_date','begin_date','period_end','formation_date'];
      const allCols = Object.keys(data.data[0]);
      const dateCol = DATE_FIELDS.find(f => allCols.includes(f));

      // Auto-detect phone columns
      const PHONE_FIELDS = ['contractor_phone','applicant_phone','phone'];
      const phoneCol = PHONE_FIELDS.find(f => allCols.includes(f));
      const hasAnyPhone = phoneCol && data.data.some(r => r[phoneCol]);

      // Priority column order
      const PRIORITY = ['permit_number','address','city','state_code','state','state_abbrev','zip_code','zip','description','event_narrative'];
      const prioritized = PRIORITY.filter(c => allCols.includes(c));
      const skip = new Set([...prioritized, ...(dateCol ? [dateCol] : []), ...PHONE_FIELDS]);
      const extraCols = allCols.filter(c => !skip.has(c) && c !== 'id');
      const displayCols = [...prioritized, ...extraCols];

      // Column display names
      const colLabel = c => {
        if (c === 'state_code' || c === 'state_abbrev') return 'ST';
        if (c === 'state') return 'ST';
        if (c === 'zip_code') return 'ZIP';
        if (c === 'event_narrative') return 'DESCRIPTION';
        return c.replace(/_/g, ' ').toUpperCase();
      };

      // Relative date formatter
      const relDate = v => {
        if (!v) return '--';
        const d = new Date(v);
        if (isNaN(d)) return String(v);
        const now = new Date();
        const diffMs = now - d;
        const diffD = Math.floor(diffMs / 86400000);
        if (diffD === 0) return 'today';
        if (diffD === 1) return '1d ago';
        if (diffD < 30) return diffD + 'd ago';
        if (diffD < 365) return Math.floor(diffD / 30) + 'mo ago';
        return Math.floor(diffD / 365) + 'y ago';
      };

      const esc = escapeHtml;
      const rows = data.data.slice(0, 25);
      let thead = '<tr><th class="col-check"><input type="checkbox" onchange="analystToggleAll(this,\'' + tblId + '\')"></th>';
      if (dateCol) thead += '<th class="col-date">DATE</th>';
      thead += displayCols.map(c => {
        let cls = '';
        if (c === 'permit_number') cls = ' class="col-permit"';
        else if (c === 'city') cls = ' class="col-city"';
        else if (c === 'state_code' || c === 'state' || c === 'state_abbrev') cls = ' class="col-st"';
        else if (c === 'zip_code' || c === 'zip') cls = ' class="col-zip"';
        return '<th' + cls + '>' + esc(colLabel(c)) + '</th>';
      }).join('');
      if (hasAnyPhone) thead += '<th class="col-phone">&phone;</th>';
      thead += '</tr>';

      let tbody = rows.map((row, i) => {
        const phoneVal = PHONE_FIELDS.map(f => row[f]).find(v => v);
        let tr = '<tr data-tbl="' + tblId + '" data-idx="' + i + '" onclick="analystRowClick(this)">';
        tr += '<td class="col-check"><input type="checkbox" data-tbl="' + tblId + '" data-idx="' + i + '" onclick="event.stopPropagation();analystToggleRow(this)" /></td>';
        if (dateCol) {
          const dv = row[dateCol];
          tr += '<td class="col-date" title="' + esc(String(dv || '')) + '"><span class="analyst-date-relative">' + esc(relDate(dv)) + '</span></td>';
        }
        tr += displayCols.map(c => {
          let val = row[c] != null ? String(row[c]) : '';
          const full = val;
          if ((c === 'description' || c === 'event_narrative') && val.length > 50) val = val.slice(0, 50) + '...';
          return '<td title="' + esc(full) + '">' + (val ? esc(val) : '<span style="color:var(--text3)">--</span>') + '</td>';
        }).join('');
        if (hasAnyPhone) {
          tr += '<td class="col-phone">' + (phoneVal ? '<a href="tel:' + esc(phoneVal) + '" onclick="event.stopPropagation()" title="Call ' + esc(phoneVal) + '">&#x1f4de;</a>' : '') + '</td>';
        }
        tr += '</tr>';
        return tr;
      }).join('');

      tableHtml = '<div style="overflow-x:auto;margin-top:16px;border:1px solid var(--border);border-radius:8px">'
        + '<table class="analyst-table"><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table></div>';
      if (data.data.length > 25) {
        tableHtml += '<div style="font-size:11px;color:var(--text3);text-align:center;padding:8px">Showing 25 of ' + data.row_count + ' results</div>';
      }
    }
```

- [ ] **Step 4: Add the table interaction JS functions**

Find the `analystExportCsv` function (around line 14812). Just BEFORE it, insert these helper functions:

```javascript
// ─── ANALYST TABLE INTERACTIONS ─────────────────────────────────────────

function analystRowClick(tr) {
  const tblId = tr.dataset.tbl;
  const idx = parseInt(tr.dataset.idx);
  // Deactivate previous row
  document.querySelectorAll('.analyst-row-active').forEach(r => r.classList.remove('analyst-row-active'));
  tr.classList.add('analyst-row-active');
  window._analyst_active_row = idx;
  analystOpenPanel(tblId, idx);
}

function analystToggleRow(cb) {
  const tblId = cb.dataset.tbl;
  const idx = parseInt(cb.dataset.idx);
  const sel = window['_analyst_selected_' + tblId];
  if (cb.checked) sel.add(idx); else sel.delete(idx);
  cb.closest('tr').classList.toggle('analyst-row-selected', cb.checked);
  analystUpdateBatchBar(tblId);
}

function analystToggleAll(cb, tblId) {
  const rows = window['_analyst_rows_' + tblId];
  const sel = window['_analyst_selected_' + tblId];
  const max = Math.min(rows.length, 25);
  const table = cb.closest('table');
  for (let i = 0; i < max; i++) {
    if (cb.checked) sel.add(i); else sel.delete(i);
    const rowCb = table.querySelector('input[data-idx="' + i + '"]');
    if (rowCb) { rowCb.checked = cb.checked; rowCb.closest('tr').classList.toggle('analyst-row-selected', cb.checked); }
  }
  analystUpdateBatchBar(tblId);
}

function analystSelectAll() {
  const tblId = window._analyst_active_table;
  if (!tblId) return;
  const headerCb = document.querySelector('.analyst-table thead input[type="checkbox"]');
  if (headerCb) { headerCb.checked = true; analystToggleAll(headerCb, tblId); }
}

function analystClearSelection() {
  const tblId = window._analyst_active_table;
  if (!tblId) return;
  const sel = window['_analyst_selected_' + tblId];
  sel.clear();
  document.querySelectorAll('.analyst-table input[type="checkbox"]').forEach(cb => { cb.checked = false; });
  document.querySelectorAll('.analyst-row-selected').forEach(r => r.classList.remove('analyst-row-selected'));
  analystUpdateBatchBar(tblId);
}

function analystUpdateBatchBar(tblId) {
  const sel = window['_analyst_selected_' + tblId];
  const bar = document.getElementById('analyst-batch-bar');
  const count = sel.size;
  document.getElementById('analyst-batch-count').textContent = count + ' selected';
  bar.classList.toggle('visible', count > 0);

  // Update dialer button to show phone count
  const rows = window['_analyst_rows_' + tblId];
  const PHONE_FIELDS = ['contractor_phone','applicant_phone','phone'];
  let phoneCount = 0;
  sel.forEach(i => {
    if (rows[i] && PHONE_FIELDS.some(f => rows[i][f])) phoneCount++;
  });
  const dialerBtn = document.getElementById('analyst-batch-dialer-btn');
  dialerBtn.textContent = '\u{1f4f1} Dialer' + (count > 0 ? ' (' + phoneCount + ' of ' + count + ' have phone)' : '');
  dialerBtn.disabled = phoneCount === 0;
}

function _analystGetSelectedRows() {
  const tblId = window._analyst_active_table;
  if (!tblId) return [];
  const rows = window['_analyst_rows_' + tblId];
  const sel = window['_analyst_selected_' + tblId];
  return Array.from(sel).sort((a, b) => a - b).map(i => rows[i]).filter(Boolean);
}
```

- [ ] **Step 5: Add batch action functions**

Immediately after the helpers from step 4, add the batch action functions:

```javascript
// ─── BATCH ACTIONS ──────────────────────────────────────────────────────

function analystBatchCsv() {
  const selected = _analystGetSelectedRows();
  if (!selected.length) return;
  const cols = Object.keys(selected[0]);
  const csvRows = [cols.join(',')];
  for (const row of selected) {
    csvRows.push(cols.map(c => {
      let val = row[c] != null ? String(row[c]) : '';
      if (val.includes(',') || val.includes('"') || val.includes('\n')) val = '"' + val.replace(/"/g, '""') + '"';
      return val;
    }).join(','));
  }
  const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
  downloadBlob(blob, 'analyst_export_' + new Date().toISOString().slice(0, 10) + '.csv');
  showToast('Exported ' + selected.length + ' rows as CSV', 'success');
}

function analystBatchClipboard() {
  const selected = _analystGetSelectedRows();
  if (!selected.length) return;
  const cols = Object.keys(selected[0]);
  const lines = [cols.join('\t')];
  for (const row of selected) {
    lines.push(cols.map(c => row[c] != null ? String(row[c]) : '').join('\t'));
  }
  navigator.clipboard.writeText(lines.join('\n')).then(() => {
    showToast('Copied ' + selected.length + ' rows to clipboard', 'success');
  }).catch(() => showToast('Clipboard copy failed', 'error'));
}

async function analystBatchWebhook() {
  const selected = _analystGetSelectedRows();
  if (!selected.length) return;
  if (!currentKey) { openSignup(); return; }

  const btn = document.getElementById('analyst-batch-webhook-btn');
  btn.disabled = true;
  btn.textContent = '\u{1f4e1} Sending...';

  try {
    const resp = await fetch(API + '/v1/analyst/webhook/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
      body: JSON.stringify({ rows: selected, source_query: '' }),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || 'Webhook send failed');
    }
    showToast('Sent ' + selected.length + ' leads to webhook', 'success');
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = '\u{1f4e1} Send to Webhook';
  }
}

function analystBatchDialer() {
  const selected = _analystGetSelectedRows();
  const PHONE_FIELDS = ['contractor_phone','applicant_phone','phone'];
  const withPhone = selected.filter(r => PHONE_FIELDS.some(f => r[f]));
  if (!withPhone.length) { showToast('No selected rows have phone numbers', 'error'); return; }

  // Store in sessionStorage for dialer page pickup
  sessionStorage.setItem('analyst_dialer_queue', JSON.stringify(withPhone));
  showToast('Queued ' + withPhone.length + ' leads for dialer', 'success');
  setTimeout(() => showPage('dialer'), 300);
}
```

- [ ] **Step 6: Add dialer page pickup for analyst leads**

Find the `initDialerPage()` function (around line 13013). After the existing auth check block (around line 13024, after `loadDialerHistory();`), add:

```javascript
  // Check for analyst-queued leads
  const analystQueue = sessionStorage.getItem('analyst_dialer_queue');
  if (analystQueue) {
    sessionStorage.removeItem('analyst_dialer_queue');
    try {
      const leads = JSON.parse(analystQueue);
      if (leads.length) {
        dialerQueue = leads;
        dialerIndex = 0;
        document.getElementById('dialer-queue-status').textContent = leads.length + ' leads from AI Analyst';
        showCurrentDialerLead();
        showToast('Loaded ' + leads.length + ' leads from AI Analyst', 'success');
        return; // Skip auto-filters since we have analyst data
      }
    } catch(e) { console.warn('Failed to parse analyst dialer queue', e); }
  }
```

- [ ] **Step 7: Commit**

```bash
cd /home/will/permit-api
git add app/static/index.html
git commit -m "feat: interactive analyst table with checkboxes, dates, phone icons, batch actions"
```

---

### Task 3: Slide-out Detail Panel

**Files:**
- Modify: `app/static/index.html` (add panel HTML, CSS, and JS)

- [ ] **Step 1: Add panel CSS**

In the same `<style>` block where we added the table styles (Task 2, Step 1), append:

```css
/* Analyst slide-out panel */
#analyst-panel{position:fixed;top:0;right:0;width:420px;height:100vh;background:var(--surface);border-left:1px solid var(--border);z-index:1000;transform:translateX(100%);transition:transform .2s ease-out;overflow-y:auto;display:flex;flex-direction:column}
#analyst-panel.open{transform:translateX(0)}
#analyst-panel-overlay{position:fixed;inset:0;background:rgba(0,0,0,.3);z-index:999;opacity:0;pointer-events:none;transition:opacity .2s}
#analyst-panel-overlay.open{opacity:1;pointer-events:auto}
.analyst-panel-header{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid var(--border);background:var(--surface);position:sticky;top:0;z-index:1}
.analyst-panel-header button{background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px;padding:4px 8px;border-radius:4px;transition:color .15s}
.analyst-panel-header button:hover{color:var(--text)}
.analyst-panel-nav{display:flex;gap:4px}
.analyst-action-zone{padding:20px;background:linear-gradient(135deg,rgba(99,102,241,.06),rgba(139,92,246,.06));border-bottom:1px solid var(--border)}
.analyst-action-zone .lead-name{font-size:20px;font-weight:700;color:var(--text);margin-bottom:2px}
.analyst-action-zone .lead-address{font-size:13px;color:var(--text2);margin-bottom:4px}
.analyst-action-zone .lead-phone{font-size:18px;font-weight:600;color:#a78bfa;margin-bottom:16px;letter-spacing:.5px}
.analyst-call-btn{display:flex;align-items:center;justify-content:center;gap:8px;padding:14px;background:linear-gradient(135deg,#22c55e,#16a34a);color:#fff;border-radius:12px;font-size:16px;font-weight:700;text-decoration:none;margin-bottom:10px;cursor:pointer;border:none;width:100%;box-shadow:0 4px 12px rgba(34,197,94,.25);transition:filter .15s}
.analyst-call-btn:hover{filter:brightness(1.1)}
.analyst-call-btn.no-phone{background:var(--surface2);color:var(--text3);box-shadow:none;cursor:default}
.analyst-secondary-actions{display:flex;gap:8px}
.analyst-secondary-actions button{flex:1;padding:8px;background:var(--surface);border:1px solid var(--border);border-radius:8px;text-align:center;font-size:12px;cursor:pointer;color:var(--text2);font-family:var(--font);transition:all .15s}
.analyst-secondary-actions button:hover{border-color:var(--accent);color:var(--accent2)}
.analyst-permit-summary{padding:16px;border-bottom:1px solid var(--border)}
.analyst-permit-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:13px}
.analyst-permit-grid .label{font-size:10px;color:var(--text3);text-transform:uppercase}
.analyst-permit-grid .value{font-weight:600;color:var(--text)}
.analyst-permit-grid .value.money{color:var(--green)}
.analyst-intel{padding:16px;flex:1}
.analyst-intel-loading{font-size:12px;color:var(--text3);font-style:italic;padding:20px;text-align:center;background:var(--surface2);border-radius:8px}
.analyst-intel-section{margin-bottom:12px}
.analyst-intel-section h4{font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.analyst-intel-section .intel-item{font-size:12px;color:var(--text2);padding:4px 0;border-bottom:1px solid var(--border)}
```

- [ ] **Step 2: Add panel HTML**

Just BEFORE the `<div id="analyst-batch-bar">` element (added in Task 2), insert:

```html
<!-- Analyst slide-out panel overlay -->
<div id="analyst-panel-overlay" onclick="analystClosePanel()"></div>
<!-- Analyst slide-out detail panel -->
<div id="analyst-panel">
  <div class="analyst-panel-header">
    <span style="font-size:13px;font-weight:600;color:var(--text2)">Permit Detail</span>
    <div style="display:flex;align-items:center;gap:4px">
      <div class="analyst-panel-nav">
        <button onclick="analystPanelNav(-1)" title="Previous (k)">&#x25B2;</button>
        <button onclick="analystPanelNav(1)" title="Next (j)">&#x25BC;</button>
      </div>
      <button onclick="analystClosePanel()" title="Close (Esc)">&#x2715;</button>
    </div>
  </div>
  <div class="analyst-action-zone" id="analyst-panel-action"></div>
  <div class="analyst-permit-summary" id="analyst-panel-summary"></div>
  <div class="analyst-intel" id="analyst-panel-intel"></div>
</div>
```

- [ ] **Step 3: Add panel JS functions**

Immediately after the batch action functions from Task 2 Step 5, add:

```javascript
// ─── SLIDE-OUT DETAIL PANEL ─────────────────────────────────────────────

const _analystIntelCache = {};

function _analystExtract(row, keys) {
  for (const k of keys) { if (row[k] != null && row[k] !== '') return row[k]; }
  return null;
}

function analystOpenPanel(tblId, idx) {
  const rows = window['_analyst_rows_' + tblId];
  if (!rows || !rows[idx]) return;
  const row = rows[idx];
  window._analyst_active_table = tblId;
  window._analyst_active_row = idx;

  // Extract fields using priority mapping
  const name = _analystExtract(row, ['owner_name','applicant_name','contractor_name','grantee','debtor_name']) || 'Unknown';
  const phone = _analystExtract(row, ['contractor_phone','applicant_phone','phone']);
  const address = row.address || '';
  const city = _analystExtract(row, ['city']) || '';
  const state = _analystExtract(row, ['state_code','state','state_abbrev']) || '';
  const zip = _analystExtract(row, ['zip_code','zip']) || '';
  const fullAddr = [address, city, state, zip].filter(Boolean).join(', ');
  const permitNum = _analystExtract(row, ['permit_number','filing_number','document_id','violation_id','license_number']) || '--';
  const DATE_FIELDS = ['issue_date','date_created','sale_date','violation_date','filing_date','install_date','expiration_date','begin_date','period_end','formation_date'];
  const dateCol = DATE_FIELDS.find(f => row[f] != null);
  const dateVal = dateCol ? row[dateCol] : null;
  const valuation = _analystExtract(row, ['valuation','sale_price','amount','loan_amount','fine_amount']);
  const status = row.status || '--';
  const desc = _analystExtract(row, ['description','event_narrative']) || '--';
  const contractor = _analystExtract(row, ['contractor_company','contractor_name','business_name']) || '--';

  // Action zone
  const actionEl = document.getElementById('analyst-panel-action');
  const esc = escapeHtml;
  if (phone) {
    actionEl.innerHTML = '<div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Lead Contact</div>'
      + '<div class="lead-name">' + esc(name) + '</div>'
      + '<div class="lead-address">' + esc(fullAddr) + '</div>'
      + '<div class="lead-phone">\u{1f4de} ' + esc(phone) + '</div>'
      + '<a href="tel:' + esc(phone) + '" class="analyst-call-btn">\u{1f4de} Call Now</a>'
      + '<div class="analyst-secondary-actions">'
      + '<button onclick="navigator.clipboard.writeText(\'' + esc(phone) + '\');showToast(\'Phone copied\',\'success\')">\u{1f4cb} Copy Phone</button>'
      + '<button onclick="sessionStorage.setItem(\'analyst_dialer_queue\',JSON.stringify([' + JSON.stringify(JSON.stringify(row)) + ']));showPage(\'dialer\')">\u{1f4f1} Send to Dialer</button>'
      + '<button onclick="analystOpenReport(\'' + esc(address) + '\',\'' + esc(city) + '\',\'' + esc(state) + '\')">\u{1f4c4} Full Report</button>'
      + '</div>';
  } else {
    actionEl.innerHTML = '<div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Lead Contact</div>'
      + '<div class="lead-name">' + esc(name) + '</div>'
      + '<div class="lead-address">' + esc(fullAddr) + '</div>'
      + '<div class="lead-phone" style="color:var(--text3)">No phone on file</div>'
      + '<button onclick="analystOpenReport(\'' + esc(address) + '\',\'' + esc(city) + '\',\'' + esc(state) + '\')" class="analyst-call-btn" style="background:linear-gradient(135deg,var(--accent),#a855f7)">\u{1f4c4} View Full Report</button>'
      + '<div class="analyst-secondary-actions">'
      + '<button onclick="navigator.clipboard.writeText(\'' + esc(fullAddr) + '\');showToast(\'Address copied\',\'success\')">\u{1f4cb} Copy Address</button>'
      + '</div>';
  }

  // Permit summary
  const summaryEl = document.getElementById('analyst-panel-summary');
  const fmtDate = dateVal ? new Date(dateVal).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) : '--';
  const fmtVal = valuation ? '$' + Number(valuation).toLocaleString() : '--';
  summaryEl.innerHTML = '<div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Permit Details</div>'
    + '<div class="analyst-permit-grid">'
    + '<div><div class="label">PERMIT #</div><div class="value">' + esc(permitNum) + '</div></div>'
    + '<div><div class="label">DATE</div><div class="value">' + esc(fmtDate) + '</div></div>'
    + '<div><div class="label">VALUATION</div><div class="value money">' + esc(fmtVal) + '</div></div>'
    + '<div><div class="label">STATUS</div><div class="value">' + esc(status) + '</div></div>'
    + '</div>'
    + '<div style="margin-top:10px"><div class="label" style="font-size:10px;color:var(--text3);text-transform:uppercase">DESCRIPTION</div><div style="font-size:13px;line-height:1.5;color:var(--text);margin-top:4px">' + esc(desc) + '</div></div>'
    + '<div style="margin-top:8px"><div class="label" style="font-size:10px;color:var(--text3);text-transform:uppercase">CONTRACTOR</div><div style="font-size:13px;color:var(--text);margin-top:2px">' + esc(contractor) + '</div></div>';

  // Intelligence section — lazy load
  const intelEl = document.getElementById('analyst-panel-intel');
  const cacheKey = (address + '|' + state).toLowerCase();
  if (_analystIntelCache[cacheKey]) {
    intelEl.innerHTML = _analystIntelCache[cacheKey];
  } else if (address && state) {
    intelEl.innerHTML = '<div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Property Intelligence</div>'
      + '<div class="analyst-intel-loading">\u{23F3} Loading linked data...<br><span style="font-size:11px">Permits \u00b7 Violations \u00b7 Sales \u00b7 Liens \u00b7 Flood \u00b7 Demographics</span></div>';
    _analystLoadIntel(address, city, state, cacheKey);
  } else {
    intelEl.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:20px;text-align:center">No address available for property lookup</div>';
  }

  // Open panel
  document.getElementById('analyst-panel').classList.add('open');
  document.getElementById('analyst-panel-overlay').classList.add('open');
}

async function _analystLoadIntel(address, city, state, cacheKey) {
  if (!currentKey) return;
  const intelEl = document.getElementById('analyst-panel-intel');
  try {
    const params = new URLSearchParams({ address, state });
    if (city) params.set('city', city);
    const resp = await fetch(API + '/v1/analyst/report?' + params.toString(), {
      headers: { 'X-API-Key': currentKey },
    });
    if (!resp.ok) throw new Error('Report failed');
    const data = await resp.json();

    let html = '<div style="font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Property Intelligence</div>';

    const sections = [
      { key: 'permits', label: 'Linked Permits', icon: '\u{1f4c4}' },
      { key: 'violations', label: 'Code Violations', icon: '\u{26a0}\u{fe0f}' },
      { key: 'sales', label: 'Sales History', icon: '\u{1f4b0}' },
      { key: 'liens', label: 'Liens', icon: '\u{1f6a8}' },
      { key: 'septic', label: 'Septic', icon: '\u{1f6b0}' },
      { key: 'flood_zone', label: 'Flood Zone', icon: '\u{1f30a}' },
      { key: 'demographics', label: 'Demographics', icon: '\u{1f4ca}' },
      { key: 'market', label: 'Market Data', icon: '\u{1f3e0}' },
    ];

    for (const s of sections) {
      const items = data[s.key];
      if (!items || !items.length) continue;
      html += '<div class="analyst-intel-section"><h4>' + s.icon + ' ' + s.label + ' (' + items.length + ')</h4>';
      for (const item of items.slice(0, 5)) {
        const summary = Object.values(item).filter(v => v != null).slice(0, 4).join(' \u00b7 ');
        html += '<div class="intel-item">' + escapeHtml(summary) + '</div>';
      }
      if (items.length > 5) html += '<div style="font-size:11px;color:var(--text3);padding:4px 0">+ ' + (items.length - 5) + ' more</div>';
      html += '</div>';
    }

    if (data.risk_score != null) {
      const color = data.risk_score > 60 ? 'var(--red)' : data.risk_score > 30 ? 'var(--orange)' : 'var(--green)';
      html += '<div style="margin-top:12px;padding:12px;background:var(--surface2);border-radius:8px;text-align:center">'
        + '<div style="font-size:10px;color:var(--text3);text-transform:uppercase;margin-bottom:4px">RISK SCORE</div>'
        + '<div style="font-size:28px;font-weight:700;color:' + color + '">' + data.risk_score + '</div></div>';
    }

    _analystIntelCache[cacheKey] = html;
    // Only update if panel is still showing this address
    if (document.getElementById('analyst-panel').classList.contains('open')) {
      intelEl.innerHTML = html;
    }
  } catch (err) {
    intelEl.innerHTML = '<div style="font-size:12px;color:var(--text3);padding:20px;text-align:center">Could not load property intelligence</div>';
  }
}

function analystClosePanel() {
  document.getElementById('analyst-panel').classList.remove('open');
  document.getElementById('analyst-panel-overlay').classList.remove('open');
  document.querySelectorAll('.analyst-row-active').forEach(r => r.classList.remove('analyst-row-active'));
  window._analyst_active_row = -1;
}

function analystPanelNav(dir) {
  const tblId = window._analyst_active_table;
  const rows = window['_analyst_rows_' + tblId];
  if (!rows) return;
  let idx = window._analyst_active_row + dir;
  const max = Math.min(rows.length, 25) - 1;
  if (idx < 0) idx = max;
  if (idx > max) idx = 0;
  // Update table row highlight
  document.querySelectorAll('.analyst-row-active').forEach(r => r.classList.remove('analyst-row-active'));
  const newTr = document.querySelector('tr[data-tbl="' + tblId + '"][data-idx="' + idx + '"]');
  if (newTr) { newTr.classList.add('analyst-row-active'); newTr.scrollIntoView({ block: 'nearest' }); }
  analystOpenPanel(tblId, idx);
}

function analystOpenReport(address, city, state) {
  openHtmlReport({ address, city, state });
}

// Keyboard shortcuts for panel (j/k/Escape)
document.addEventListener('keydown', function(e) {
  if (!document.getElementById('analyst-panel').classList.contains('open')) return;
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  if (e.key === 'j' || e.key === 'ArrowDown') { e.preventDefault(); analystPanelNav(1); }
  if (e.key === 'k' || e.key === 'ArrowUp') { e.preventDefault(); analystPanelNav(-1); }
  if (e.key === 'Escape') { analystClosePanel(); }
});
```

- [ ] **Step 4: Commit**

```bash
cd /home/will/permit-api
git add app/static/index.html
git commit -m "feat: add slide-out detail panel with action-first layout and keyboard nav"
```

---

### Task 4: Webhook Configuration UI

**Files:**
- Modify: `app/static/index.html` (add webhook config section, load webhook state on page init)

- [ ] **Step 1: Add webhook config inline in the analyst page**

Find the `<div id="analyst-chat"` element (around line 4821). Just BEFORE the chat div but AFTER the batch bar HTML, add a collapsible webhook config section:

```html
<!-- Webhook config (inline) -->
<div id="analyst-webhook-config" style="max-width:720px;margin:0 auto 16px;display:none">
  <div style="padding:14px 18px;background:var(--surface);border:1px solid var(--border);border-radius:10px;">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <span style="font-size:13px;font-weight:600;color:var(--text2)">&#x1f4e1; CRM Webhook</span>
      <button onclick="document.getElementById('analyst-webhook-config').style.display='none'" style="background:none;border:none;color:var(--text3);cursor:pointer;font-size:14px">&#x2715;</button>
    </div>
    <div style="display:flex;gap:8px">
      <input type="text" id="analyst-webhook-url" placeholder="https://hooks.zapier.com/..." style="flex:1;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface2);color:var(--text);font-size:13px;font-family:var(--font)">
      <button onclick="analystSaveWebhook()" style="padding:8px 14px;border-radius:6px;background:var(--accent);color:#fff;border:none;font-size:12px;font-weight:600;cursor:pointer">Save</button>
      <button onclick="analystTestWebhook()" style="padding:8px 14px;border-radius:6px;background:var(--surface2);border:1px solid var(--border);color:var(--text2);font-size:12px;cursor:pointer">Test</button>
    </div>
    <div style="font-size:11px;color:var(--text3);margin-top:6px">Works with Zapier, Make, n8n, or any URL that accepts POST JSON</div>
  </div>
</div>
```

- [ ] **Step 2: Add webhook config JS**

After the panel JS functions (from Task 3 Step 3), add:

```javascript
// ─── WEBHOOK CONFIG ─────────────────────────────────────────────────────

async function analystLoadWebhookConfig() {
  if (!currentKey) return;
  try {
    const resp = await fetch(API + '/v1/analyst/webhook/config', {
      headers: { 'X-API-Key': currentKey },
    });
    if (resp.ok) {
      const data = await resp.json();
      if (data.webhook_url) {
        document.getElementById('analyst-webhook-url').value = data.webhook_url;
        // Enable webhook batch button
        const btn = document.getElementById('analyst-batch-webhook-btn');
        btn.disabled = false;
        btn.title = 'Send to ' + data.webhook_url;
        window._analystWebhookConfigured = true;
      }
    }
  } catch(e) { /* non-critical */ }
}

async function analystSaveWebhook() {
  if (!currentKey) { openSignup(); return; }
  const url = document.getElementById('analyst-webhook-url').value.trim();
  try {
    const resp = await fetch(API + '/v1/analyst/webhook/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
      body: JSON.stringify({ webhook_url: url || null }),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || 'Save failed');
    }
    const btn = document.getElementById('analyst-batch-webhook-btn');
    if (url) {
      btn.disabled = false;
      btn.title = 'Send to ' + url;
      window._analystWebhookConfigured = true;
      showToast('Webhook URL saved', 'success');
    } else {
      btn.disabled = true;
      btn.title = 'Configure webhook first';
      window._analystWebhookConfigured = false;
      showToast('Webhook URL cleared', 'success');
    }
  } catch (err) {
    showToast(err.message, 'error');
  }
}

async function analystTestWebhook() {
  if (!currentKey) { openSignup(); return; }
  try {
    const resp = await fetch(API + '/v1/analyst/webhook/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || 'Test failed');
    }
    showToast('Test payload sent successfully!', 'success');
  } catch (err) {
    showToast(err.message, 'error');
  }
}
```

- [ ] **Step 3: Wire up webhook config loading in initAnalystPage()**

In the `initAnalystPage()` function (around line 14666), at the end of the `else` block (where `currentKey` exists, around line 14678), add:

```javascript
    analystLoadWebhookConfig();
```

- [ ] **Step 4: Add a gear icon to open the webhook config**

In the batch bar HTML (the `batch-right` div), add a gear button BEFORE the webhook send button:

```html
<button onclick="document.getElementById('analyst-webhook-config').style.display=''" title="Configure CRM Webhook" style="font-size:14px">&#x2699;</button>
```

- [ ] **Step 5: Commit**

```bash
cd /home/will/permit-api
git add app/static/index.html
git commit -m "feat: add webhook config UI with save/test for CRM integration"
```

---

### Task 5: Integration Testing & Polish

**Files:**
- Modify: `app/static/index.html` (minor polish)

- [ ] **Step 1: Test the full flow manually**

Start the backend and frontend:
```bash
cd /home/will/permit-api
# Start backend (if not already running)
# uvicorn app.main:app --reload --port 8000
```

Open `permits.ecbtx.com/#analyst` (or local) and:
1. Ask a query: "Show me 10 new permits with phone numbers that would be good for a new roof"
2. Verify DATE column appears with relative dates
3. Verify phone icons appear on rows with phone data
4. Click a row — panel should slide in from right
5. Verify Call Now button shows with phone number
6. Press j/k to navigate between rows
7. Press Escape to close panel
8. Check checkboxes on 3 rows
9. Verify batch bar slides up with "3 selected"
10. Click "Copy to Clipboard" — verify toast
11. Click "Export CSV" — verify file downloads

- [ ] **Step 2: Fix responsive width for mobile**

Add a media query for the panel to go full-width on small screens. In the panel CSS section, append:

```css
@media(max-width:768px){
  #analyst-panel{width:100vw}
}
```

- [ ] **Step 3: Ensure panel closes when navigating away from analyst page**

In the `showPage()` function (around line 5093), add at the top of the function body:

```javascript
  // Close analyst panel when leaving page
  if (typeof analystClosePanel === 'function') analystClosePanel();
  // Hide batch bar
  const batchBar = document.getElementById('analyst-batch-bar');
  if (batchBar) batchBar.classList.remove('visible');
```

- [ ] **Step 4: Final commit and push**

```bash
cd /home/will/permit-api
git add app/static/index.html app/api/v1/analyst.py app/models/api_key.py app/main.py
git commit -m "feat: actionable analyst results — interactive table, detail panel, batch actions, webhook CRM"
git push
```

---

## Summary

| Task | What It Does | Files |
|------|-------------|-------|
| 1 | Backend: webhook_url column + 3 webhook endpoints | `api_key.py`, `analyst.py`, `main.py` |
| 2 | Frontend: interactive table with checkboxes, dates, phone icons, batch bar | `index.html` |
| 3 | Frontend: slide-out detail panel with action-first layout | `index.html` |
| 4 | Frontend: webhook config UI with save/test | `index.html` |
| 5 | Integration testing, responsive polish, cleanup | `index.html` |
