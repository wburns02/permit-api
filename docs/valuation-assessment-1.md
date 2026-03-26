# PermitLookup Valuation Assessment — Business & Product Analysis

*Prepared March 2026 — Pre-revenue assessment by an experienced SaaS founder perspective*

---

## 1. One-Time Build Cost Equivalent

What would it realistically cost a professional team to build this from scratch?

| Component | Effort | Cost (2026 rates) |
|-----------|--------|-------------------|
| 312 scrapers + data normalization pipeline | 3 senior devs × 4 months | $240K |
| FastAPI backend (179 endpoints, auth, billing, security) | 2 senior devs × 3 months | $120K |
| AI integrations (analyst, predictions, call intelligence) | 1 ML engineer × 3 months | $75K |
| CRM + dialer + quote builder + team mgmt | 2 full-stack devs × 2 months | $80K |
| Frontend SPA (15K lines) + PWA | 1 frontend dev × 2 months | $30K |
| Email campaign system | 1 dev × 2 weeks | $10K |
| DevOps/infra (on-prem servers, Tailscale, replication) | 1 DevOps × 1 month | $20K |
| Data acquisition (scraping time, API rate limits, cleaning) | 6 months calendar time | $0 (time cost) |
| Hardware (2x R730, T430, GPUs, switch) | Purchase | $15-22K |
| **Total** | **~8-10 months with 5-7 devs** | **$590K-$620K** |

**The hardest part isn't the code — it's the 6+ months of running 312 scrapers to accumulate 960M records.** You can't buy that time. A buyer who wants this data TODAY would need to either acquire it or wait 6-12 months to replicate.

**Realistic build cost equivalent: $500K-$750K** (including time-to-data).

**Important caveat:** A large percentage of the 960M records are low-value padding. NYC boiler inspections (838K), USDA crop data, earthquake events, FMCSA carriers — these aren't what a permit/property customer pays for. The genuinely monetizable core is ~827M records (permits, sales, violations, entities, septic, FEMA, valuations).

---

## 2. Realistic Annual Value to Customer Segments

### Contractors ($79-249/mo target)
- **Claimed ROI:** $200-400K additional revenue for a $2M roofer
- **Reality:** Assumes the roofer changes their entire sales process and converts at 5x current rate. Most won't.
- **Realistic value:** 5-10 extra jobs/year at $5K avg = $25-50K additional revenue. Justifies $249/mo easily.
- **Adoption hurdle:** Contractors are notoriously bad at software adoption. Churn will be 8-15%/month without dedicated onboarding.
- **Realistic willingness to pay:** $79-149/mo. $249/mo is a stretch without onboarding.

### Insurance Agents ($599/mo target)
- **Claimed ROI:** $139K/year savings
- **Reality:** Agents don't save time on risk assessment — they pay services to do it. Real value is BETTER risk data.
- **Realistic value:** Avoiding 2-3 bad policies/year ($20-150K in loss avoidance). But they don't attribute that to a $599/mo subscription.
- **Adoption hurdle:** Already have data vendors (ATTOM, CoreLogic, LexisNexis). Need 6-12 months of track record.
- **Realistic willingness to pay:** $199-399/mo for agents. $5K-50K/yr for agencies/carriers.

### Title Companies ($249-599/mo target)
- **Claimed ROI:** $6-9K/year savings — **this is actually realistic**
- **Adoption hurdle:** Use established title plants and underwriter-approved sources. Getting approved takes time.
- **Realistic willingness to pay:** Per-property pricing ($3-8/property) is better than monthly for this segment.

### RE Investors ($249-599/mo target)
- **Best early segment.** Tech-savvy, comfortable with SaaS, already pay for multiple subscriptions.
- **Realistic value:** Replacing 3-4 separate subscriptions ($500+/mo combined). LLC piercing and permit-to-sale pipeline are genuinely unique.
- **Realistic willingness to pay:** $149-349/mo. $599/mo realistic for active flippers/wholesalers.

### PropTech Companies ($599-1,499/mo target)
- **Realistic:** Will pay IF data is reliable and API well-documented.
- **Adoption hurdle:** Need production-grade SLA guarantees.
- **Realistic willingness to pay:** $499-999/mo startups, $2K-10K/mo established.

---

## 3. Recommended Pricing Strategy

| Tier | Price | Target |
|------|-------|--------|
| **Free** | $0 (10/day) | Trial/evaluation — keep |
| **Starter** | $49/mo | Solo contractors, small investors — NEW lower barrier |
| **Pro** | $149/mo | Active contractors, agents — was $249, lower to reduce friction |
| **Business** | $499/mo | Agencies, brokerages — replaces Real-Time |
| **Enterprise** | Custom ($1K-5K/mo) | Insurance carriers, proptech — sales-led |

Kill the Intelligence tier. Add per-property pricing alongside subscriptions ($5/report, $2/batch address).

**Floor:** $49/mo × 100 customers = $4,900 MRR
**Ceiling:** $499/mo × 50 + $149/mo × 200 = $54,700 MRR

---

## 4. Key Risks and Downsides

1. **Zero customer validation** — 960M records and 179 endpoints mean nothing if no one pays
2. **Bus factor = 1** — one developer, no documentation beyond CLAUDE.md
3. **Infrastructure fragility** — residential power, consumer networking, SD card boot drives
4. **Data freshness dependency** — 312 scrapers hitting government APIs that can change without notice
5. **Monetizable data is narrower than it appears** — most customers only need 5-10% of the data layers
6. **Competitors have distribution** — ATTOM has 20 years, Shovels has VC funding

---

## 5. Honest Recommendation

**Focus on ONE segment: contractors.** Lowest barrier, shortest sales cycle, dialer is a genuine differentiator.

### Realistic MRR Projection (18 months, strong execution)

| Month | MRR |
|-------|-----|
| 1-2 | $500-1K |
| 3-4 | $3-5K |
| 5-8 | $8-15K |
| 9-12 | $20-40K |
| 13-18 | $50-80K |

### Valuation

- **Floor (sell today, zero revenue):** $300K-$500K
- **Realistic (6mo traction, $10K MRR):** $1.5M-$2.5M
- **Ceiling (18mo, $50K+ MRR):** $7M-$15M

**The single thing that changes everything: first 10 paying customers.**
