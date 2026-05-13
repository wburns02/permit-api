# Density Bonus Law

- **Code sections**: Gov Code §65915 (State Density Bonus Law), amended by AB 1287 (2023), AB 1893 (2024)
- **Statute current as of**: 2026-05-11
- **Source**: https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=65915&lawCode=GOV
- **last_verified**: 2026-05-11

## Summary
California's Density Bonus Law requires local governments to grant a **density bonus** (additional units beyond base zoning), **concessions/incentives**, **waivers** of development standards, and **parking reductions** to housing developers who include affordable units in their projects. Stacks on top of any other state law program (SB-9, SB-684/1123, AB-2011/SB-6, etc.) and base zoning.

AB-1287 (effective Jan 1, 2024) raised the maximum bonus from 35% to **50%** for single-affordability projects, and allows stacking to **up to 100%** when combining categories.

## Eligibility / Triggering Affordability Thresholds

A project triggers a density bonus by including any of the following:
- **5%** very-low-income units → baseline bonus
- **10%** low-income units → baseline bonus
- **10%** moderate-income (for-sale only) units → baseline bonus
- **100% affordable** project → maximum bonus + special concessions
- **Senior** (≥35 units age-restricted) → 20% bonus
- **Special needs / supportive housing** → 20% bonus

### Bonus Tiers (post AB-1287)
| Affordability Type | Min % Required | Bonus | Cap |
|---|---|---|---|
| Very-Low Income | 5% | 20% | up to 50% with higher % (sliding scale) |
| Low Income | 10% | 20% | up to 50% with higher % |
| Moderate (for-sale) | 10% | 5% | up to 50% with higher % |
| 100% affordable | 100% | 80% | + extra concessions |
| Stacking categories | (combined) | — | up to 100% (AB-1287) |

## Yield Math

```
base_units = base_density × acres
bonus_pct = lookup_table(affordability_type, affordability_pct)
total_units = base_units × (1 + bonus_pct)
```

**Example**: 1-acre parcel in R-3 (20 du/ac base) with 10% low-income:
- Base = 20 units
- 10% low-income → 20% bonus
- Total = 20 × 1.20 = **24 units** (4 of which are affordable, 20 market-rate)

**Sliding scale**: more affordable % = higher bonus. Detailed math in Gov Code §65915(f).

## Concessions / Incentives (separate from density bump)
- 1 concession at minimum affordability triggers
- 2 concessions at higher %
- 3-4 concessions at highest tiers and 100% affordable
- Concessions can include: reduced parking, reduced setbacks, increased height, FAR bumps, design flexibility

## Waivers (separate from concessions)
Developer may request waivers of any development standard that "physically precludes" achieving the project at the granted density. Unlimited waivers if justified.

## Parking Reductions
- 0.5 spaces per studio/1BR unit
- 1.0 spaces per 2-3 BR unit
- 1.5 spaces per 4+ BR unit
- **Zero parking** if within 0.5 mi of major transit stop (high-quality transit corridor)
- Local cannot impose more than these reductions allow

## Caveats

### [VERIFY]
- [ ] **Affordability commitment** is durable (55-year regulatory agreement typical)
- [ ] **Local Density Bonus ordinance** — verify city has adopted (most have; some are out of date)
- [ ] **Math validation**: bonus % varies by exact affordability %; use Gov Code §65915(f) lookup table or city's published calculator
- [ ] **Concession requests** must be cost-reducing or feasibility-enabling; city can deny if not justified

### Strategic Notes
- Density bonus is **most powerful when stacked with other state law programs** (SB-684, AB-2011, base zoning)
- Affordability premium reduces revenue per project — model net economics carefully
- For 100% affordable projects, layered subsidies (LIHTC, state/federal grants) typically required to make economics work
- AB-1287 effectively unlocks 100% density bonus in single-purpose affordable projects — game-changer for nonprofit affordable developers

## Stacking with Other State Laws

| State Law | Density Bonus Stacks? | Notes |
|---|---|---|
| SB-9 | Limited | SB-9 yield is itself 4 units; density bonus typically not applied (small projects) |
| SB-684 / SB-1123 | Yes | Apply bonus on top of 10-lot subdivision |
| SB-1211 (MF ADUs) | Limited | ADUs are not "primary" units; bonus typically applies to primary density |
| State ADU | No | ADUs explicit excluded from density bonus calculations |
| AB-2011 / SB-6 | Yes | Common stack — density bonus on commercial corridor housing |
| Base zoning | Yes | Standard application |

## Local Implementation
- Verify city has a current Density Bonus ordinance
- Check city's bonus calculation worksheet (some publish online)
- Confirm affordability monitoring agreement terms (55-year typical)
