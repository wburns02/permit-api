# PermitLookup Frontend Expansion — Detailed Implementation Plan

## Context

**Codebase**: `/home/will/permit-api/`
**Frontend**: Single-file SPA at `app/static/index.html` (1597 lines, vanilla JS, dark theme)
**Backend**: FastAPI at `app/main.py` — all new API endpoints already deployed and tested
**Production**: https://permits.ecbtx.com
**Design**: Dark mode — `--bg:#0a0a0f`, `--accent:#6366f1`, `--surface:#12121a`
**Auth**: `localStorage.getItem('pl_api_key')` → `X-API-Key` header
**Global vars**: `currentKey`, `API` (base URL), `alertsList`
**Pattern**: Pages are `<div class="page" id="page-{name}">`, toggled by `showPage(name)`
**Toasts**: `showToast(msg, 'success'|'error')`
**HTML escaping**: `esc(string)` function

## HARD RULES
- All changes go in `app/static/index.html` — this is the ONLY frontend file
- Follow existing CSS variable system and dark theme exactly
- Follow existing JS patterns (fetch + X-API-Key, showToast, showPage)
- After every commit: `git push` then `railway up --detach`
- After deploy: test with Playwright browser tools to verify
- Never claim something works without Playwright proof

---

## Task 1: Alert Test Button + Execution History

### What to build
Add a "Test" button to each alert card that calls `POST /v1/alerts/{id}/test` and shows results inline. Add a "History" button that shows execution log from `GET /v1/alerts/{id}/history`.

### CSS to add (in `<style>` block)

```css
/* Alert test results */
.alert-test-results{margin-top:12px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:16px;max-height:300px;overflow-y:auto}
.alert-test-results h5{font-size:13px;color:var(--text2);margin-bottom:8px}
.alert-test-results table{width:100%;font-size:12px;border-collapse:collapse}
.alert-test-results th{text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);color:var(--text3);font-weight:500}
.alert-test-results td{padding:6px 8px;border-bottom:1px solid var(--border);color:var(--text2)}

/* Alert history */
.alert-history{margin-top:12px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:16px}
.alert-history .history-entry{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px}
.alert-history .history-entry:last-child{border-bottom:none}
.history-status{font-size:12px;padding:2px 8px;border-radius:4px;font-weight:500}
.history-status.success{background:var(--green-glow);color:var(--green)}
.history-status.failed{background:rgba(239,68,68,.15);color:var(--red)}
.history-status.partial{background:rgba(245,158,11,.15);color:var(--orange)}
```

### HTML changes in `renderAlertCard(a)` function

Add two buttons after the Delete button in `.alert-card-actions`:
```html
<button onclick="testAlert('${a.id}')">Test</button>
<button onclick="showAlertHistory('${a.id}')">History</button>
```

Add a container div at the bottom of each `.alert-card` (before closing `</div>`):
```html
<div id="alert-detail-${a.id}"></div>
```

### JS functions to add

```javascript
async function testAlert(id) {
  const container = document.getElementById('alert-detail-' + id);
  container.innerHTML = '<div class="alert-test-results"><h5>Running test...</h5></div>';
  try {
    const r = await fetch(`${API}/v1/alerts/${id}/test`, {
      method: 'POST',
      headers: { 'X-API-Key': currentKey }
    });
    if (!r.ok) throw new Error('Test failed');
    const d = await r.json();
    let html = `<div class="alert-test-results">
      <h5>${d.match_count} matches found (dry run — no notifications sent)</h5>`;
    if (d.matches && d.matches.length) {
      html += `<table><thead><tr><th>Permit #</th><th>Address</th><th>Type</th><th>Date</th></tr></thead><tbody>`;
      d.matches.slice(0, 10).forEach(m => {
        html += `<tr><td>${esc(m.permit_number||'')}</td><td>${esc(m.address||'')}</td><td>${esc(m.permit_type||'')}</td><td>${m.issue_date||''}</td></tr>`;
      });
      html += `</tbody></table>`;
      if (d.match_count > 10) html += `<p style="color:var(--text3);font-size:12px;margin-top:8px">Showing 10 of ${d.match_count} matches</p>`;
    }
    html += `</div>`;
    container.innerHTML = html;
  } catch(e) {
    container.innerHTML = `<div class="alert-test-results"><h5 style="color:var(--red)">Test failed: ${esc(e.message)}</h5></div>`;
  }
}

async function showAlertHistory(id) {
  const container = document.getElementById('alert-detail-' + id);
  container.innerHTML = '<div class="alert-history"><h5>Loading history...</h5></div>';
  try {
    const r = await fetch(`${API}/v1/alerts/${id}/history`, {
      headers: { 'X-API-Key': currentKey }
    });
    if (!r.ok) throw new Error('Failed to load history');
    const d = await r.json();
    if (!d.history || !d.history.length) {
      container.innerHTML = '<div class="alert-history"><p style="color:var(--text3);font-size:13px">No executions yet. The scheduler will run this alert based on its frequency.</p></div>';
      return;
    }
    let html = '<div class="alert-history">';
    d.history.forEach(h => {
      html += `<div class="history-entry">
        <span>${new Date(h.run_at).toLocaleString()}</span>
        <span>${h.match_count} matches</span>
        <span>${esc(h.delivery_method||'')}</span>
        <span class="history-status ${h.delivery_status}">${h.delivery_status}</span>
        ${h.error ? `<span style="color:var(--red);font-size:11px">${esc(h.error)}</span>` : ''}
      </div>`;
    });
    html += '</div>';
    container.innerHTML = html;
  } catch(e) {
    container.innerHTML = `<div class="alert-history"><h5 style="color:var(--red)">${esc(e.message)}</h5></div>`;
  }
}
```

### Playwright verification
1. Navigate to `https://permits.ecbtx.com/`
2. Log in (enter API key via signup or localStorage)
3. Navigate to Alerts page
4. Verify alert cards render with Test and History buttons
5. Click Test button → verify results table appears
6. Click History button → verify history section appears

---

## Task 2: Properties Page (Insurance/Underwriting)

### What to build
New page `#page-properties` with an address input that calls `GET /v1/properties/history?address=...` and displays permit list + risk signals. Include a CSV upload area for bulk reports.

### Nav changes
In the `<nav>` links section, add after the Alerts link:
```html
<a href="#" onclick="showPage('properties');return false">Properties</a>
```

### SPA route
Add `"/properties"` to the SPA routes array at line ~120 in `showPage()`:
```javascript
// In the showPage function, add 'properties' to valid pages
```
Also add to the catch-all routes list in main.py (already done).

### HTML to add (new page div, after `#page-alerts` closing div)

```html
<div class="page" id="page-properties">
  <div class="container" style="padding:60px 24px">
    <h2 style="font-size:28px;font-weight:700;margin-bottom:8px">Property Intelligence</h2>
    <p style="color:var(--text2);margin-bottom:32px">Full permit history and risk signals for any property address.</p>

    <!-- Single lookup -->
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px">
      <h3 style="font-size:16px;margin-bottom:16px">Property Lookup</h3>
      <div style="display:flex;gap:12px">
        <input type="text" id="property-address" placeholder="Enter full property address..." style="flex:1;padding:12px 16px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;outline:none" />
        <button onclick="lookupProperty()" class="btn-primary" id="property-lookup-btn" style="padding:12px 24px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600">Lookup</button>
      </div>
    </div>

    <!-- Bulk upload (auth gated) -->
    <div id="property-bulk-section" style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px;display:none">
      <h3 style="font-size:16px;margin-bottom:8px">Bulk Property Report</h3>
      <p style="color:var(--text3);font-size:13px;margin-bottom:16px">Upload a CSV with an "address" column. Each address counts as 1 lookup. Requires Starter plan or higher.</p>
      <div style="border:2px dashed var(--border);border-radius:var(--radius);padding:32px;text-align:center;cursor:pointer" onclick="document.getElementById('bulk-csv-input').click()">
        <p style="color:var(--text2)">📄 Click to upload CSV or drag & drop</p>
        <input type="file" id="bulk-csv-input" accept=".csv" style="display:none" onchange="uploadBulkReport(this.files[0])" />
      </div>
      <div id="bulk-results" style="margin-top:16px"></div>
    </div>

    <!-- Auth gate for bulk -->
    <div id="property-auth-gate" style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;text-align:center;display:none">
      <p style="color:var(--text2)">🔒 Sign up for an API key to use Property Intelligence</p>
      <button onclick="document.getElementById('signup-modal').classList.add('visible')" style="margin-top:12px;padding:10px 24px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600">Get API Key</button>
    </div>

    <!-- Results -->
    <div id="property-results" style="display:none"></div>
  </div>
</div>
```

### JS functions to add

```javascript
async function lookupProperty() {
  const addr = document.getElementById('property-address').value.trim();
  if (!addr) { showToast('Enter a property address', 'error'); return; }
  if (!currentKey) { showToast('API key required. Sign up first.', 'error'); return; }

  const btn = document.getElementById('property-lookup-btn');
  btn.disabled = true; btn.textContent = 'Looking up...';
  const results = document.getElementById('property-results');

  try {
    const r = await fetch(`${API}/v1/properties/history?address=${encodeURIComponent(addr)}`, {
      headers: { 'X-API-Key': currentKey }
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || 'Lookup failed'); }
    const d = await r.json();
    results.style.display = '';
    results.innerHTML = renderPropertyResults(d);
  } catch(e) {
    showToast(e.message, 'error');
    results.style.display = 'none';
  } finally {
    btn.disabled = false; btn.textContent = 'Lookup';
  }
}

function renderPropertyResults(d) {
  const rs = d.risk_signals || {};
  const permits = d.permits || [];

  let html = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px">
    <h3 style="font-size:16px;margin-bottom:16px">Risk Signals — ${esc(d.address)}</h3>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;margin-bottom:16px">
      <div style="background:var(--surface2);padding:16px;border-radius:var(--radius-sm)">
        <div style="font-size:24px;font-weight:700">${rs.permit_count}</div>
        <div style="font-size:12px;color:var(--text3)">Total Permits</div>
      </div>
      <div style="background:var(--surface2);padding:16px;border-radius:var(--radius-sm)">
        <div style="font-size:24px;font-weight:700">${rs.years_since_last_permit ?? 'N/A'}</div>
        <div style="font-size:12px;color:var(--text3)">Years Since Last</div>
      </div>
      <div style="background:var(--surface2);padding:16px;border-radius:var(--radius-sm)">
        <div style="font-size:24px;font-weight:700;color:${rs.has_unpermitted_gap ? 'var(--red)' : 'var(--green)'}">${rs.has_unpermitted_gap ? 'YES' : 'NO'}</div>
        <div style="font-size:12px;color:var(--text3)">10yr+ Gap</div>
      </div>
      <div style="background:var(--surface2);padding:16px;border-radius:var(--radius-sm)">
        <div style="font-size:24px;font-weight:700">${rs.renovation_intensity}</div>
        <div style="font-size:12px;color:var(--text3)">Permits/Year</div>
      </div>
    </div>
    ${Object.keys(rs.permit_type_breakdown||{}).length ? `<div style="display:flex;gap:8px;flex-wrap:wrap">${Object.entries(rs.permit_type_breakdown).map(([k,v])=>`<span style="background:var(--accent-glow);color:var(--accent2);padding:4px 10px;border-radius:4px;font-size:12px">${esc(k)}: ${v}</span>`).join('')}</div>` : ''}
  </div>`;

  if (permits.length) {
    html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px">
      <h3 style="font-size:16px;margin-bottom:16px">Permit History (${permits.length})</h3>
      <div style="overflow-x:auto"><table style="width:100%;font-size:13px;border-collapse:collapse">
        <thead><tr style="color:var(--text3)">
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Permit #</th>
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Type</th>
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Status</th>
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Issue Date</th>
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Valuation</th>
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Contractor</th>
        </tr></thead>
        <tbody>${permits.slice(0,50).map(p => `<tr style="color:var(--text2)">
          <td style="padding:8px;border-bottom:1px solid var(--border)">${esc(p.permit_number||'')}</td>
          <td style="padding:8px;border-bottom:1px solid var(--border)">${esc(p.permit_type||'')}</td>
          <td style="padding:8px;border-bottom:1px solid var(--border)">${esc(p.status||'')}</td>
          <td style="padding:8px;border-bottom:1px solid var(--border)">${p.issue_date||''}</td>
          <td style="padding:8px;border-bottom:1px solid var(--border)">${p.valuation ? '$'+Number(p.valuation).toLocaleString() : ''}</td>
          <td style="padding:8px;border-bottom:1px solid var(--border)">${esc(p.contractor_name||p.contractor_company||'')}</td>
        </tr>`).join('')}</tbody>
      </table></div>
      ${permits.length > 50 ? `<p style="color:var(--text3);font-size:12px;margin-top:8px">Showing 50 of ${permits.length} permits</p>` : ''}
    </div>`;
  }
  return html;
}

// Show/hide bulk section based on auth
function updatePropertyPage() {
  const bulk = document.getElementById('property-bulk-section');
  const gate = document.getElementById('property-auth-gate');
  if (currentKey) {
    bulk.style.display = '';
    gate.style.display = 'none';
  } else {
    bulk.style.display = 'none';
    gate.style.display = '';
  }
}

async function uploadBulkReport(file) {
  if (!file) return;
  if (!currentKey) { showToast('API key required', 'error'); return; }

  const resultsDiv = document.getElementById('bulk-results');
  resultsDiv.innerHTML = '<p style="color:var(--text2)">Processing CSV...</p>';

  const formData = new FormData();
  formData.append('file', file);

  try {
    const r = await fetch(`${API}/v1/properties/bulk-report`, {
      method: 'POST',
      headers: { 'X-API-Key': currentKey },
      body: formData
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || 'Upload failed'); }
    const d = await r.json();

    let html = `<h4 style="margin-bottom:12px">${d.total_addresses} addresses processed</h4>`;
    d.results.forEach(prop => {
      const rs = prop.risk_signals;
      html += `<div style="background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);padding:12px;margin-bottom:8px">
        <strong>${esc(prop.address)}</strong>
        <span style="margin-left:12px;color:var(--text3);font-size:12px">${rs.permit_count} permits · Last: ${rs.last_permit_date||'N/A'} · Gap: ${rs.has_unpermitted_gap?'⚠️ Yes':'No'} · Intensity: ${rs.renovation_intensity}/yr</span>
      </div>`;
    });
    resultsDiv.innerHTML = html;
  } catch(e) {
    resultsDiv.innerHTML = `<p style="color:var(--red)">${esc(e.message)}</p>`;
  }
}
```

### Integration
- In `showPage()` function: add `if (name === 'properties') updatePropertyPage();`
- Call `updatePropertyPage()` after login/signup

### Playwright verification
1. Navigate to Properties page
2. Enter "123 Main St" in lookup field
3. Click Lookup → verify risk signals cards + permit table appear
4. Verify risk signal values render (permit count, years since last, gap indicator)
5. Mobile resize → verify table scrolls horizontally

---

## Task 3: Market Intelligence Page

### What to build
New page `#page-market` with ZIP/city/state inputs. Calls `GET /v1/market/activity` and `GET /v1/market/hotspots`. Free users see a "Pro required" gate.

### Nav changes
Add after Properties link:
```html
<a href="#" onclick="showPage('market');return false">Market</a>
```

### HTML to add (new page div)

```html
<div class="page" id="page-market">
  <div class="container" style="padding:60px 24px">
    <h2 style="font-size:28px;font-weight:700;margin-bottom:8px">Market Intelligence</h2>
    <p style="color:var(--text2);margin-bottom:32px">Permit activity trends, top contractors, and growth hotspots. <span style="background:var(--accent-glow);color:var(--accent2);padding:2px 8px;border-radius:4px;font-size:12px">Pro+</span></p>

    <!-- Pro gate -->
    <div id="market-pro-gate" style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:48px;text-align:center">
      <h3 style="margin-bottom:12px">Pro Plan Required</h3>
      <p style="color:var(--text2);margin-bottom:20px">Market intelligence is available on Pro ($149/mo) and Enterprise ($499/mo) plans.</p>
      <button onclick="showPage('home');setTimeout(()=>document.getElementById('pricing-section')?.scrollIntoView({behavior:'smooth'}),100)" style="padding:12px 32px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600">View Pricing</button>
    </div>

    <!-- Market content (hidden until Pro user) -->
    <div id="market-content" style="display:none">
      <!-- Activity search -->
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px">
        <h3 style="font-size:16px;margin-bottom:16px">Market Activity</h3>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
          <input type="text" id="market-zip" placeholder="ZIP code" style="width:120px;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;outline:none" />
          <input type="text" id="market-city" placeholder="City" style="width:160px;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;outline:none" />
          <select id="market-state" style="width:100px;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px">
            <option value="">State</option>
            <!-- Same state options as alert form -->
          </select>
          <select id="market-months" style="width:120px;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px">
            <option value="3">3 months</option>
            <option value="6" selected>6 months</option>
            <option value="12">12 months</option>
            <option value="24">24 months</option>
          </select>
          <button onclick="loadMarketActivity()" style="padding:10px 24px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600">Analyze</button>
        </div>
      </div>

      <!-- Activity results -->
      <div id="market-activity-results" style="display:none"></div>

      <!-- Hotspots -->
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px">
        <h3 style="font-size:16px;margin-bottom:16px">Growth Hotspots</h3>
        <div style="display:flex;gap:12px;align-items:center">
          <select id="hotspot-state" style="width:100px;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px">
            <!-- Same state options -->
          </select>
          <button onclick="loadHotspots()" style="padding:10px 24px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600">Find Hotspots</button>
        </div>
      </div>
      <div id="hotspot-results" style="display:none"></div>
    </div>
  </div>
</div>
```

**IMPORTANT**: Copy the state `<option>` elements from the alert form's `#alert-state` select into `#market-state` and `#hotspot-state`. They already exist in the HTML.

### JS functions to add

```javascript
async function loadMarketActivity() {
  const zip = document.getElementById('market-zip').value.trim();
  const city = document.getElementById('market-city').value.trim();
  const state = document.getElementById('market-state').value;
  const months = document.getElementById('market-months').value;

  if (!zip && !city && !state) { showToast('Enter at least one filter', 'error'); return; }

  const params = new URLSearchParams();
  if (zip) params.set('zip', zip);
  if (city) params.set('city', city);
  if (state) params.set('state', state);
  params.set('months', months);

  const results = document.getElementById('market-activity-results');
  results.style.display = '';
  results.innerHTML = '<p style="color:var(--text2)">Analyzing market data...</p>';

  try {
    const r = await fetch(`${API}/v1/market/activity?${params}`, {
      headers: { 'X-API-Key': currentKey }
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || 'Failed'); }
    const d = await r.json();
    results.innerHTML = renderMarketActivity(d);
  } catch(e) {
    results.innerHTML = `<p style="color:var(--red)">${esc(e.message)}</p>`;
  }
}

function renderMarketActivity(d) {
  let html = '';

  // Monthly volume as horizontal bars
  if (d.monthly_volume && d.monthly_volume.length) {
    const maxCount = Math.max(...d.monthly_volume.map(m => m.permit_count));
    html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px">
      <h4 style="margin-bottom:16px">Monthly Permit Volume</h4>
      ${d.monthly_volume.map(m => {
        const pct = maxCount ? (m.permit_count / maxCount * 100) : 0;
        const monthName = new Date(m.year, m.month-1).toLocaleString('default',{month:'short',year:'numeric'});
        return `<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
          <span style="width:80px;font-size:12px;color:var(--text3)">${monthName}</span>
          <div style="flex:1;height:24px;background:var(--surface2);border-radius:4px;overflow:hidden">
            <div style="height:100%;width:${pct}%;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:4px;transition:width .3s"></div>
          </div>
          <span style="width:80px;font-size:12px;color:var(--text2);text-align:right">${m.permit_count.toLocaleString()}</span>
        </div>`;
      }).join('')}
    </div>`;
  }

  // Top contractors
  if (d.top_contractors && d.top_contractors.length) {
    html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;margin-bottom:24px">
      <h4 style="margin-bottom:16px">Top Contractors</h4>
      <table style="width:100%;font-size:13px;border-collapse:collapse">
        <thead><tr style="color:var(--text3)"><th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">Contractor</th><th style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">Permits</th></tr></thead>
        <tbody>${d.top_contractors.map(c => `<tr style="color:var(--text2)"><td style="padding:8px;border-bottom:1px solid var(--border)">${esc(c.contractor)}</td><td style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">${c.permits.toLocaleString()}</td></tr>`).join('')}</tbody>
      </table>
    </div>`;
  }

  // Permit type breakdown
  if (d.permit_type_breakdown && Object.keys(d.permit_type_breakdown).length) {
    html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px">
      <h4 style="margin-bottom:16px">Permit Type Breakdown</h4>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        ${Object.entries(d.permit_type_breakdown).sort((a,b)=>b[1]-a[1]).map(([k,v]) =>
          `<span style="background:var(--surface2);padding:8px 14px;border-radius:var(--radius-sm);font-size:13px"><strong>${esc(k)}</strong> <span style="color:var(--text3)">${v.toLocaleString()}</span></span>`
        ).join('')}
      </div>
    </div>`;
  }

  return html;
}

async function loadHotspots() {
  const state = document.getElementById('hotspot-state').value;
  if (!state) { showToast('Select a state', 'error'); return; }

  const results = document.getElementById('hotspot-results');
  results.style.display = '';
  results.innerHTML = '<p style="color:var(--text2)">Finding hotspots...</p>';

  try {
    const r = await fetch(`${API}/v1/market/hotspots?state=${state}&months=6`, {
      headers: { 'X-API-Key': currentKey }
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || 'Failed'); }
    const d = await r.json();

    if (!d.hotspots || !d.hotspots.length) {
      results.innerHTML = '<p style="color:var(--text3)">No hotspots found with 50+ permits.</p>';
      return;
    }

    let html = `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px">
      <h4 style="margin-bottom:16px">${d.state} Growth Hotspots (${d.months}-month window)</h4>
      <table style="width:100%;font-size:13px;border-collapse:collapse">
        <thead><tr style="color:var(--text3)">
          <th style="padding:8px;text-align:left;border-bottom:1px solid var(--border)">ZIP</th>
          <th style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">Permits</th>
          <th style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">Avg Value</th>
          <th style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">Prior Period</th>
          <th style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">Growth</th>
        </tr></thead>
        <tbody>${d.hotspots.map(h => `<tr style="color:var(--text2)">
          <td style="padding:8px;border-bottom:1px solid var(--border);font-weight:600">${esc(h.zip)}</td>
          <td style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">${h.permit_count.toLocaleString()}</td>
          <td style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">${h.avg_valuation ? '$'+Math.round(h.avg_valuation).toLocaleString() : '—'}</td>
          <td style="padding:8px;text-align:right;border-bottom:1px solid var(--border)">${h.prior_period_count.toLocaleString()}</td>
          <td style="padding:8px;text-align:right;border-bottom:1px solid var(--border);font-weight:600;color:${h.growth_pct > 0 ? 'var(--green)' : h.growth_pct < 0 ? 'var(--red)' : 'var(--text3)'}">${h.growth_pct !== null ? (h.growth_pct > 0 ? '+' : '') + h.growth_pct + '%' : '—'}</td>
        </tr>`).join('')}</tbody>
      </table>
    </div>`;
    results.innerHTML = html;
  } catch(e) {
    results.innerHTML = `<p style="color:var(--red)">${esc(e.message)}</p>`;
  }
}
```

### Integration
- In `showPage()`: handle `'market'` — show `#market-pro-gate` or `#market-content` based on user plan
- Since we can't easily check plan on the frontend, always show both sections and let the API return 403 (catch it and show upgrade CTA)
- Alternative approach: always show the form, and when the 403 comes back, display a styled upgrade message

### Playwright verification
1. Navigate to Market page
2. Verify Pro gate or form renders
3. For free-tier user: enter ZIP + click Analyze → verify 403 message shown cleanly
4. Verify hotspot state selector populated

---

## Task 4: Saved Searches Page

### What to build
Add saved searches to the Dashboard page. Show a list of saved searches with "Run" buttons.

### HTML to add (inside `#page-dashboard`, after API keys section)

```html
<div id="saved-searches-section" style="margin-top:32px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
    <h3 style="font-size:18px;font-weight:600">Saved Searches</h3>
    <button onclick="openSavedSearchForm()" style="padding:8px 20px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600;font-size:13px">+ New</button>
  </div>
  <div id="saved-searches-list"></div>
</div>

<!-- Saved search modal -->
<div class="modal-overlay" id="saved-search-modal">
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:32px;width:100%;max-width:480px">
    <h3 id="saved-search-form-title" style="margin-bottom:20px">New Saved Search</h3>
    <label style="font-size:13px;color:var(--text2);display:block;margin-bottom:4px">Name</label>
    <input type="text" id="ss-name" style="width:100%;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;margin-bottom:16px;outline:none" />
    <label style="font-size:13px;color:var(--text2);display:block;margin-bottom:4px">State</label>
    <select id="ss-state" style="width:100%;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;margin-bottom:16px">
      <option value="">Any</option>
      <!-- Copy state options from alert form -->
    </select>
    <label style="font-size:13px;color:var(--text2);display:block;margin-bottom:4px">City</label>
    <input type="text" id="ss-city" style="width:100%;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;margin-bottom:16px;outline:none" />
    <label style="font-size:13px;color:var(--text2);display:block;margin-bottom:4px">Permit Type</label>
    <select id="ss-permit-type" style="width:100%;padding:10px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);font-size:14px;margin-bottom:24px">
      <option value="">Any</option>
      <option value="building">Building</option>
      <option value="electrical">Electrical</option>
      <option value="plumbing">Plumbing</option>
      <option value="mechanical">Mechanical</option>
      <option value="demolition">Demolition</option>
    </select>
    <div style="display:flex;gap:12px;justify-content:flex-end">
      <button onclick="closeSavedSearchForm()" style="padding:10px 20px;background:var(--surface2);color:var(--text2);border:1px solid var(--border);border-radius:var(--radius-sm)">Cancel</button>
      <button onclick="saveSavedSearch()" id="ss-save-btn" style="padding:10px 20px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-weight:600">Save</button>
    </div>
  </div>
</div>
```

### CSS
```css
/* Reuse modal-overlay from signup/alert modals */
```

### JS functions

```javascript
async function loadSavedSearches() {
  if (!currentKey) return;
  const list = document.getElementById('saved-searches-list');
  if (!list) return;

  try {
    const r = await fetch(`${API}/v1/saved-searches`, { headers: { 'X-API-Key': currentKey } });
    if (!r.ok) return;
    const d = await r.json();

    if (!d.saved_searches.length) {
      list.innerHTML = '<p style="color:var(--text3);font-size:13px">No saved searches yet.</p>';
      return;
    }

    list.innerHTML = d.saved_searches.map(s => {
      const filters = s.filters || {};
      const tags = [];
      if (filters.state) tags.push(filters.state);
      if (filters.city) tags.push(filters.city);
      if (filters.permit_type) tags.push(filters.permit_type);

      return `<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:16px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center">
        <div>
          <strong>${esc(s.name)}</strong>
          <div style="margin-top:4px">${tags.map(t => `<span style="background:var(--accent-glow);color:var(--accent2);padding:2px 8px;border-radius:4px;font-size:11px;margin-right:4px">${esc(t)}</span>`).join('')}</div>
          ${s.last_run_at ? `<span style="font-size:11px;color:var(--text3);margin-top:4px;display:block">Last run: ${new Date(s.last_run_at).toLocaleString()}</span>` : ''}
        </div>
        <div style="display:flex;gap:8px">
          <button onclick="runSavedSearch('${s.id}')" style="padding:6px 16px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius-sm);font-size:12px;font-weight:600">Run</button>
          <button onclick="deleteSavedSearch('${s.id}')" style="padding:6px 12px;background:none;color:var(--red);border:1px solid var(--red);border-radius:var(--radius-sm);font-size:12px">Delete</button>
        </div>
      </div>`;
    }).join('');
  } catch(e) { /* silent */ }
}

function openSavedSearchForm() {
  document.getElementById('saved-search-modal').classList.add('visible');
}
function closeSavedSearchForm() {
  document.getElementById('saved-search-modal').classList.remove('visible');
}

async function saveSavedSearch() {
  const name = document.getElementById('ss-name').value.trim();
  if (!name) { showToast('Name required', 'error'); return; }

  const filters = {};
  const state = document.getElementById('ss-state').value;
  const city = document.getElementById('ss-city').value.trim();
  const pt = document.getElementById('ss-permit-type').value;
  if (state) filters.state = state;
  if (city) filters.city = city;
  if (pt) filters.permit_type = pt;

  try {
    const r = await fetch(`${API}/v1/saved-searches`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-API-Key': currentKey },
      body: JSON.stringify({ name, filters })
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail); }
    closeSavedSearchForm();
    showToast('Search saved!', 'success');
    loadSavedSearches();
  } catch(e) {
    showToast(e.message, 'error');
  }
}

async function runSavedSearch(id) {
  showToast('Running search...', 'success');
  try {
    const r = await fetch(`${API}/v1/saved-searches/${id}/run`, {
      method: 'POST',
      headers: { 'X-API-Key': currentKey }
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail); }
    const d = await r.json();
    // Show results on search page
    showPage('home');
    const resultsDiv = document.getElementById('search-results');
    if (resultsDiv) {
      // Reuse existing search results rendering
      showToast(`Found ${d.total.toLocaleString()} permits`, 'success');
    }
  } catch(e) {
    showToast(e.message, 'error');
  }
}

async function deleteSavedSearch(id) {
  if (!confirm('Delete this saved search?')) return;
  try {
    await fetch(`${API}/v1/saved-searches/${id}`, {
      method: 'DELETE',
      headers: { 'X-API-Key': currentKey }
    });
    showToast('Deleted', 'success');
    loadSavedSearches();
  } catch(e) {
    showToast('Failed to delete', 'error');
  }
}
```

### Integration
- Call `loadSavedSearches()` when dashboard page loads
- In `showPage()`: if `name === 'dashboard'` → call `loadSavedSearches()`

### Playwright verification
1. Navigate to Dashboard
2. Verify "Saved Searches" section visible
3. Click "+ New" → verify modal opens
4. Fill form and save → verify search appears in list
5. Click "Run" → verify toast shows result count
6. Click "Delete" → verify removed from list

---

## Task 5: Landing Page Feature Sections Update

### What to build
Update the hero section and add feature cards below the code samples section highlighting the three verticals: Lead Gen, Insurance, and Real Estate.

### HTML to add (after the developer-first code section, before pricing)

```html
<section style="padding:80px 0;background:var(--surface)" id="features-section">
  <div class="container">
    <h2 style="text-align:center;font-size:28px;font-weight:700;margin-bottom:12px">Built for Every Industry</h2>
    <p style="text-align:center;color:var(--text2);margin-bottom:48px;max-width:600px;margin-left:auto;margin-right:auto">Three verticals, one API. Whether you're generating leads, underwriting risk, or analyzing markets — we have the data.</p>

    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:24px">
      <!-- Lead Gen -->
      <div style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:32px;transition:border-color .2s" onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
        <div style="font-size:32px;margin-bottom:16px">🔔</div>
        <h3 style="font-size:18px;margin-bottom:8px">Lead Generation</h3>
        <p style="color:var(--text2);font-size:14px;line-height:1.6;margin-bottom:16px">Real-time permit alerts for roofers, solar installers, HVAC contractors, and home service companies. New permits mean new customers.</p>
        <ul style="color:var(--text3);font-size:13px;list-style:none;padding:0">
          <li style="margin-bottom:6px">✓ Instant, daily, or weekly alerts</li>
          <li style="margin-bottom:6px">✓ Email + webhook delivery</li>
          <li style="margin-bottom:6px">✓ Filter by type, area, contractor</li>
          <li>✓ Up to 100 active alerts</li>
        </ul>
      </div>

      <!-- Insurance -->
      <div style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:32px;transition:border-color .2s" onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
        <div style="font-size:32px;margin-bottom:16px">🛡️</div>
        <h3 style="font-size:18px;margin-bottom:8px">Insurance & Underwriting</h3>
        <p style="color:var(--text2);font-size:14px;line-height:1.6;margin-bottom:16px">Property-level permit history with risk signals. Identify unpermitted gaps, renovation intensity, and permit type patterns.</p>
        <ul style="color:var(--text3);font-size:13px;list-style:none;padding:0">
          <li style="margin-bottom:6px">✓ Per-property risk signals</li>
          <li style="margin-bottom:6px">✓ Bulk CSV portfolio analysis</li>
          <li style="margin-bottom:6px">✓ 10yr+ unpermitted gap detection</li>
          <li>✓ Up to 10,000 addresses/request</li>
        </ul>
      </div>

      <!-- Real Estate -->
      <div style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:32px;transition:border-color .2s" onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'">
        <div style="font-size:32px;margin-bottom:16px">📊</div>
        <h3 style="font-size:18px;margin-bottom:8px">Real Estate & PropTech</h3>
        <p style="color:var(--text2);font-size:14px;line-height:1.6;margin-bottom:16px">Market intelligence for investors and platforms. Monthly trends, growth hotspots, top contractors, and valuation data by ZIP code.</p>
        <ul style="color:var(--text3);font-size:13px;list-style:none;padding:0">
          <li style="margin-bottom:6px">✓ Monthly permit volume trends</li>
          <li style="margin-bottom:6px">✓ ZIP code growth hotspots</li>
          <li style="margin-bottom:6px">✓ Top contractors per market</li>
          <li>✓ Saved searches with re-run</li>
        </ul>
      </div>
    </div>
  </div>
</section>
```

### Playwright verification
1. Navigate to home page
2. Scroll down past code section
3. Verify 3 feature cards visible with correct titles
4. Mobile resize → verify cards stack vertically

---

## Task 6: Usage Analytics on Dashboard

### What to build
Enhance the existing dashboard page with a usage chart (daily lookups bar chart using CSS — no external libraries needed) and top endpoints table.

### Backend endpoint needed
The existing `GET /v1/usage` endpoint already returns usage data. Check its response format and use it.

### HTML to add (inside `#page-dashboard`, after KPI cards)

```html
<div id="usage-chart-section" style="margin-top:32px">
  <h3 style="font-size:18px;font-weight:600;margin-bottom:16px">Usage — Last 7 Days</h3>
  <div id="usage-chart" style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px;min-height:200px"></div>
</div>
```

### JS to add

```javascript
async function loadUsageChart() {
  if (!currentKey) return;
  try {
    const r = await fetch(`${API}/v1/usage`, { headers: { 'X-API-Key': currentKey } });
    if (!r.ok) return;
    const d = await r.json();

    // d.daily_usage is array of {date, count} for recent days
    const daily = d.daily_usage || [];
    if (!daily.length) {
      document.getElementById('usage-chart').innerHTML = '<p style="color:var(--text3);font-size:13px">No usage data yet.</p>';
      return;
    }

    const maxCount = Math.max(...daily.map(u => u.count), 1);
    let html = '<div style="display:flex;align-items:flex-end;gap:8px;height:160px">';
    daily.slice(-7).forEach(u => {
      const pct = (u.count / maxCount * 100);
      const day = new Date(u.date).toLocaleDateString('default', {weekday:'short'});
      html += `<div style="flex:1;text-align:center">
        <div style="font-size:11px;color:var(--text2);margin-bottom:4px">${u.count}</div>
        <div style="height:${Math.max(pct, 4)}%;background:linear-gradient(180deg,var(--accent),var(--accent2));border-radius:4px 4px 0 0;min-height:4px"></div>
        <div style="font-size:10px;color:var(--text3);margin-top:4px">${day}</div>
      </div>`;
    });
    html += '</div>';
    document.getElementById('usage-chart').innerHTML = html;
  } catch(e) { /* silent */ }
}
```

### Integration
- Call `loadUsageChart()` in `showPage('dashboard')` alongside other dashboard loads

### Playwright verification
1. Navigate to Dashboard
2. Verify "Usage — Last 7 Days" section visible
3. Verify bar chart renders (or "No usage data" message)

---

## Task 7: Webhook Test on Alert Form

### What to build
Add a "Test Webhook" button next to the webhook URL input in the alert create/edit form. Sends a test payload to the URL to verify it works before saving.

### HTML change in alert form
After the webhook URL input, add:
```html
<button type="button" onclick="testWebhookUrl()" id="test-webhook-btn" style="margin-top:4px;padding:4px 12px;background:none;color:var(--accent2);border:1px solid var(--border);border-radius:4px;font-size:12px;display:none">Test Webhook</button>
```

### JS to add

```javascript
// Show/hide test button based on webhook URL input
document.getElementById('alert-webhook').addEventListener('input', function() {
  document.getElementById('test-webhook-btn').style.display = this.value.trim() ? '' : 'none';
});

async function testWebhookUrl() {
  const url = document.getElementById('alert-webhook').value.trim();
  if (!url) return;

  const btn = document.getElementById('test-webhook-btn');
  btn.disabled = true; btn.textContent = 'Testing...';

  try {
    // We can't directly test the webhook from the browser (CORS).
    // Instead, if we have an alert ID, use the test endpoint.
    // For new alerts, just validate the URL format.
    const urlObj = new URL(url);
    if (!['http:', 'https:'].includes(urlObj.protocol)) throw new Error('Must be HTTP/HTTPS');
    showToast('URL format valid. Webhook will be tested when alert fires.', 'success');
  } catch(e) {
    showToast('Invalid webhook URL: ' + e.message, 'error');
  } finally {
    btn.disabled = false; btn.textContent = 'Test Webhook';
  }
}
```

### Playwright verification
1. Open alert form
2. Type a URL in webhook field → verify "Test Webhook" button appears
3. Click Test Webhook → verify toast message

---

## Execution Order

1. **Task 5** — Landing page feature sections (quickest, most visible)
2. **Task 1** — Alert test + history buttons (completes alert workflow)
3. **Task 7** — Webhook test button (small, quick)
4. **Task 2** — Properties page (new full page)
5. **Task 3** — Market Intelligence page (new full page)
6. **Task 4** — Saved Searches (dashboard enhancement)
7. **Task 6** — Usage analytics chart (dashboard enhancement)

After each task:
1. `git add app/static/index.html`
2. `git commit -m "descriptive message"`
3. `git push`
4. `railway up --detach` (wait ~2 min for deploy)
5. Test with Playwright (navigate, click, verify snapshots)

---

## `showPage()` Integration Summary

The `showPage()` function needs these additions:
```javascript
// Add to the page list that showPage recognizes:
// 'properties', 'market'

// Add callbacks:
if (name === 'properties') updatePropertyPage();
if (name === 'market') { /* market page is static, API gating handles access */ }
if (name === 'dashboard') { loadSavedSearches(); loadUsageChart(); }
```

And add nav links for Properties and Market in the `<nav>` HTML.

## Files Modified
- `app/static/index.html` — ALL frontend changes go here (the only file)

## No Backend Changes Needed
All API endpoints are already deployed and tested. This plan is frontend-only.
