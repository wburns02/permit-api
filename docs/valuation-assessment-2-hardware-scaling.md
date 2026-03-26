# PermitLookup Hardware Capacity & Scaling Assessment

*Prepared March 2026 — Infrastructure analysis for the path to $50K MRR*

---

## Hardware Capacity & Concurrency Limits

### Current Request Path (Railway + SOCKS proxy)
```
Browser → Railway (Oregon) → Tailscale → Python SOCKS5 proxy → Tailscale → Home network → PostgreSQL
```
Each request: 6+ network hops, 500ms-2s overhead per query.

### Current Request Path (R730-2 Direct — just activated)
```
Browser → Tailscale Funnel → R730-2 → localhost PostgreSQL
```
Each request: 2 hops, <200ms overhead. **This is the new architecture.**

### Realistic Concurrent User Capacity (Direct R730-2)

| Resource | Limit | Notes |
|----------|-------|-------|
| R730-2 uvicorn (4 workers) | ~200 concurrent connections | Can increase workers |
| PostgreSQL (768GB RAM) | ~5,000 read queries/sec | Nearly all data cached |
| Claude API (AI Analyst) | ~10 concurrent calls | Anthropic rate limits |
| Network (Tailscale Funnel) | ~500 Mbps | Adequate for API responses |

### Customers Before Degradation

"Degrades" = response time >3s for search, >10s for AI Analyst, >5% error rate.

| Scenario | Users | Lookups/Day | Can Handle? |
|----------|-------|-------------|:-----------:|
| 50 Pro Leads | 5-10 concurrent | 7,500 | Yes easily |
| 200 mixed | 20-40 concurrent | 25,000 | Yes |
| 500 customers ($50K MRR) | 50-100 concurrent | 75,000 | **Yes with 8 workers** |
| 1,000 customers | 100-200 concurrent | 150,000 | Borderline — need optimization |

The R730-2 direct architecture is dramatically better than the SOCKS proxy. 768GB RAM means virtually zero disk reads for queries.

---

## Scaling Plan to $50K MRR

### Infrastructure Timeline

| Trigger | Action | Cost |
|---------|--------|------|
| **Now** | API running on R730-2 via Tailscale Funnel | $0 |
| **$5K MRR** | Add Cloudflare CDN for static + DDoS protection. Add Redis for caching. | $35/mo |
| **$10K MRR** | Custom domain on R730-2 (Cloudflare Tunnel). Separate scraper workloads. | $50/mo |
| **$20K MRR** | Add monitoring (uptime, APM). Consider UPS for all servers. | $100-200/mo |
| **$50K MRR** | Add cloud read replica as failover. Or 3rd R730. | $500-1,500/mo |

### Monthly Infra Cost at $50K MRR

| Item | Cost |
|------|------|
| Electricity (3 servers) | $100-150/mo |
| Cloudflare Pro | $20/mo |
| Redis Cloud | $15/mo |
| Anthropic API (Claude) | $200-500/mo |
| SendGrid | $50-100/mo |
| Monitoring | $50/mo |
| **Total** | **$435-835/mo** |

That's **1.6% of revenue** — exceptional SaaS margin.

---

## On-Prem: Strength or Liability?

**Strength up to $50K MRR.** Marginal cost per customer is near zero. Cloud equivalent (managed PostgreSQL with 768GB RAM) = $5,000-10,000/mo on AWS.

**Liability beyond $100K MRR:** No redundancy, no geographic distribution, no SLA guarantees for enterprise customers.

**Recommendation:** Hybrid approach. Keep scrapers + data on-prem (cheap). Move API to cloud when affordable (~$30K MRR).

---

## Single Biggest Capacity Boost

**Already done: moved API from Railway to R730-2.** Zero SOCKS proxy, direct localhost PostgreSQL, 768GB RAM. This alone improved query speed by 15-500x and increased capacity from ~50 to ~5,000 concurrent connections.

Cost: $0.
