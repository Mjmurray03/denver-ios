# Denver IOS Analysis - Quick Summary

**For:** Matt Haley, Apricus Realty Capital
**From:** Michael Murray
**Date:** December 6, 2025

---

## What I Built

Automated property sourcing system for IOS investments in Adams County, Denver metro.

---

## Data Analyzed

**187,407 parcels** from Adams County Assessor, Zoning, and Building Footprints

---

## Results

| Category | Count |
|----------|-------|
| A-grade properties (best matches) | 275 |
| B-grade properties (strong candidates) | 3,770 |
| **A-grade + exact Apricus criteria** | **95** |
| **A/B grade + relaxed criteria** | **727** |

---

## Scoring Based On Your Published Criteria

| Your Criteria | How We Scored It | Weight |
|---------------|------------------|--------|
| Sub 20% building coverage | Optimal at 5-15%, good up to 20% | 30% |
| 3-15 acres preferred | Optimal score for 3-15 acres | 25% |
| Industrial zoning | I-1, I-2, I-3 score 100 | 20% |
| IOS tenant uses (truck/trailer, equipment, containers, fleet parking) | Keywords in land use scoring | 15% |

---

## Deliverables

1. **denver_ios_map_a_grade.html** - 95 best properties (A grade + exact criteria)
2. **denver_ios_map.html** - 727 qualified properties (A/B + relaxed criteria)
3. **denver_ios_top_candidates.xlsx** - Full data with 73 columns

Maps include satellite imagery toggle - click any property for full details.

---

## Filters Applied

| Filter | Exact | Relaxed |
|--------|-------|---------|
| Grade | A only | A or B |
| Acres | 3-15 | 2-20 |
| Coverage | <20% | <25% |
| Value | $3-15M | $2-20M |

---

## Note on Values

Used **assessed value** as proxy for $3-15M filter. Assessed ≠ market value, so properties near boundaries may still qualify.

---

## Questions for You

- Scoring weights look right?
- Focus on specific submarkets?
- Need owner contact info?

---

*See IOS_Scoring_Methodology.md for full technical details*
