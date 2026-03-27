# Smart Result Funnel & Broadening Suggestions

**Date:** 2026-03-27
**Status:** Approved
**Project:** PermitLookup (permits.ecbtx.com)
**Scope:** Backend prompt change + small frontend chip rendering

## Problem

When users ask the AI Analyst a query with multiple filters (e.g., "Roofing permits in Austin this week with phone numbers over $10K"), they get sparse results (2) with no visibility into what was filtered out. They don't know that dropping the $10K filter would give 19 results, or that 4 permits sit at $8K-$10K. Users abandon the tool thinking the data is thin, when it's actually their filter stack that's too narrow.

## Solution

When the analyst query returns fewer than 5 results, modify the summary generation prompt to:

1. **Explain the filter funnel** — Describe how each filter narrowed the results in natural language (e.g., "919 Austin permits this week -> 19 roofing -> 19 with phones -> 2 over $10K")
2. **Suggest 2-3 broader queries** — Include plain English questions the user could ask, formatted with a `>>` prefix so the frontend can detect and render them as clickable chips

When results are 5+, the summary prompt is unchanged.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| When to show | Sparse results only (<5) | Don't clutter good results |
| Where logic lives | Backend AI prompt only | Claude already knows the filters, no extra API calls |
| Funnel data source | AI estimation from schema knowledge | No actual COUNT queries, keeps it fast and simple |
| Suggestion format | `>>` prefixed lines in summary text | Frontend can detect and render as chips with zero schema changes |
| Response schema | Unchanged | Suggestions embedded in `summary` string, no new fields |

## Implementation

### Backend Change

**File:** `app/api/v1/analyst.py`, lines ~338-360 (summary generation step)

**Current behavior:** After executing the SQL and getting results, the code builds a summary prompt:
```
"Write a concise, insightful 2-4 sentence summary of these results..."
```

**New behavior:** When `len(serialized_rows) < 5`, use an augmented prompt:

```
Write a concise summary of these results. IMPORTANT: The query returned very few results ({count}).

First, explain the filter funnel — describe how each filter in the user's question likely narrowed the results. Use approximate counts based on your knowledge of the data (e.g., "There are likely hundreds of Austin permits this week, but filtering to roofing narrows it significantly, and the $10K minimum cuts it further"). Be specific and helpful.

Then suggest 2-3 broader searches the user could try to find more leads. Format each suggestion on its own line starting with >> like:
>> Roofing permits in Austin this week with phone numbers
>> All permits in Austin this week over $10K with phone numbers
>> Roofing permits in Texas this week with phone numbers

Make suggestions that remove one filter at a time from the original query so the user can see which filter to relax.
```

When `len(serialized_rows) >= 5`, keep the existing prompt unchanged.

### Frontend Change

**File:** `app/static/index.html`, inside `analystSubmit()` where the summary is rendered (~line 14764)

**Current:** The summary is rendered as plain escaped text:
```javascript
`<div style="font-size:14px;...">${escapeHtml(data.summary)}</div>`
```

**New:** After rendering the summary, scan for lines starting with `>>`. Extract them, strip the `>>` prefix, and render as clickable chip buttons below the summary:

```javascript
// Extract broadening suggestions from summary
const summaryLines = data.summary.split('\n');
const suggestions = summaryLines.filter(l => l.trim().startsWith('>>'));
const cleanSummary = summaryLines.filter(l => !l.trim().startsWith('>>')).join(' ').trim();

// Render summary text
let summaryHtml = escapeHtml(cleanSummary);

// Render suggestion chips if any
if (suggestions.length > 0) {
  summaryHtml += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:12px">';
  for (const s of suggestions) {
    const text = s.replace(/^>>\s*/, '').trim();
    summaryHtml += '<button class="analyst-chip" onclick="analystAsk(\'' + escapeHtml(text).replace(/'/g, "\\'") + '\')">' + escapeHtml(text) + '</button>';
  }
  summaryHtml += '</div>';
}
```

This reuses the existing `.analyst-chip` CSS class and the existing `analystAsk()` function. No new components, styles, or endpoints.

## What Does NOT Change

- SQL generation step (Step 1-3 in analyst_query)
- The `AnalystResponse` schema (no new fields)
- Normal results (5+ rows): summary prompt unchanged
- No additional API calls or COUNT queries
- No new database tables or columns
- No new endpoints

## Out of Scope

- Actual COUNT queries for precise funnel numbers (would add latency and cost)
- Structured `filter_funnel` field in the API response (YAGNI)
- Broadening suggestions for aggregation queries (top N, counts, averages)
- UI for manually toggling individual filters on/off
