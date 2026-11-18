# Commodity OI z-score: results

**Pre-registration:** `docs/OI_Zscore_Preregistration.md`, commit `274c4a9`, 18 Nov 2026 10:46 IST. It was written and committed before anything below was computed.

**Code:** `research/oi_zscore_study.py`, plus `research/oi_expiries.py` for the futures expiry dates from MCX's Bhavcopy. Both are read-only.

**Data:** `mcx_commodity_daily` (MCX Bhavcopy OI, backfilled 17 Nov 2026).
- 3 Jan 2022 → 17 Nov 2026: 11,772 complex-days.
- 11 complexes pass the cron's filter: ALUMINIUM, COPPER, COTTON, CRUDEOIL, GOLD, LEAD, NATURALGAS, NICKEL, SILVER, SILVER100, ZINC.
- Futures OI is present on 87.5% of complex-days.
- Training period: 2022–2024. Test period: 2025 → 17 Nov 2026.

## Verdict (pre-registered rule): **OI stays out of the composite**

| Variant | (a) test incremental IC > 0, t ≥ 2 | (b) training has the same sign | (c) composite +0.02 | (d) h5/h20 not negative | Pass |
|---|---|---|---|---|---|
| OI_lots | ✗ (0.053, t 1.0) | ✓ | ✗ (−0.034) | ✓ | no |
| **OI_val** | **✓ (0.110, t 2.0)** | ✓ | **✗ (−0.006)** | ✓ | no |
| OI_lots_adj | ✗ (0.028, t 0.5) | ✓ | ✗ (−0.058) | ✓ | no |
| OI_val_adj | ✗ (0.068, t 1.3) | ✓ | ✗ (−0.034) | ✓ | no |

No variant passes, so `OI_IN_COMPOSITE` stays `False`. The dashboard is unchanged by this study.

## What the numbers say

### 1. OI predicts a commodity's next weeks of activity, but most of that is already in turnover and volume
The figures below are mean cross-sectional rank IC against `fwd_act_h`, with t from non-overlapping dates.

**Test period (2025–2026)**

| Predictor | h = 5 | h = 10 | h = 20 |
|---|---|---|---|
| C (current composite) | 0.406 (t 12.5) | 0.307 (t 6.5) | 0.223 (t 3.4) |
| OI_lots | 0.116 (3.4) | 0.128 (2.6) | 0.158 (2.2) |
| OI_val | 0.179 (5.1) | 0.190 (3.8) | 0.214 (3.3) |
| OI_lots_adj | 0.056 (1.7) | 0.063 (1.3) | 0.092 (1.2) |
| OI_val_adj | 0.113 (3.2) | 0.111 (2.2) | 0.137 (2.0) |

The training-period figures are similar: C 0.44 / 0.35 / 0.30, OI_val 0.20 / 0.19 / 0.16.

The current composite is by far the strongest predictor. OI's predictive value grows with the horizon, while the composite's fades.

### 2. ₹ value beats lots everywhere
OI_val beats OI_lots at every horizon, in both periods and on every statistic.

Summing lots across a complex adds up contracts of very different sizes. For gold, for example, 430,000 petal lots (1 g each) are added to 19,000 one-kilogram lots. **Question 2 of the pre-registration is answered: if OI is used, use ₹ value.**

### 3. Incremental information is real but small
This is the rank IC of each OI variant against the part of the target the composite doesn't explain.

| | h = 5 | h = 10 | h = 20 |
|---|---|---|---|
| OI_val, training | 0.107 (t 3.6) | 0.092 (t 2.3) | 0.069 (t 1.2) |
| OI_val, test | 0.094 (t 2.6) | 0.110 (t 2.0) | 0.139 (t 1.9) |
| OI_lots, test | 0.042 (1.2) | 0.053 (1.0) | 0.086 (1.2) |

OI_val holds up out of sample, consistent with training. OI in lots adds nothing.

### 4. …but averaging it in at equal weight makes the 1–2 week read worse
This compares, on the test period, the IC of the composite with OI averaged in against C alone.

| | h = 5 | h = 10 | h = 20 |
|---|---|---|---|
| C + OI_val | 0.366 vs 0.406 (−0.040) | 0.301 vs 0.307 (−0.006) | 0.282 vs 0.223 (**+0.059**) |
| C + OI_lots | −0.062 | −0.034 | +0.034 |

Giving OI a third of the weight dilutes the stronger turnover and volume terms at short horizons. It helps only at 20 days. Under criterion (c), which is set at 10 days, it fails.

### 5. Removing the expiry cycle removes signal rather than noise
The expiry-adjusted versions are weaker everywhere. Part of what makes OI predictive is the expiry cycle itself: activity also rises and falls predictably around expiry. Taking the cycle out of the predictor takes out some real information about the target.

### 6. Flips
Adding OI flips about a third of test-period signals:
- OI_lots: 33%
- OI_val: 31%
- OI_lots_adj: 36%
- OI_val_adj: 33%

On the days that flip, forward activity sides with the new OI_val signal slightly more often than the old one: 534 vs 483 of 1,354 flipped days. For OI_val_adj it sides with the old one more often (578 vs 507). The case for OI_val is that small.

## Deviations from the pre-registration
- **Unspecified implementation detail:** a cross-sectional IC needs at least 5 complexes on the date (`MIN_XS = 5`). The pre-registration did not specify this. It was chosen before running and not varied.
- **Expiry source for complexes:** `dte` uses the parent symbol's own futures expiries, as written. For GOLD and SILVER the parent contract is bimonthly while the minis are monthly, so their expiry adjustment follows the parent's cycle only.
- Nothing else deviated. Every variant and horizon listed is reported.

## Recommendations
1. **Keep `OI_IN_COMPOSITE = False`.** This is the pre-registered outcome.
2. **Show OI in ₹ value on the Commodities page, not lots.** The displayed OI z currently uses lots summed across mixed contract sizes. This is a display change only, with no effect on signals, so it goes in a separate PR if you want it.
3. **Not decided here (exploratory, would need its own pre-registration):**
   - OI_val looks useful at the 3–4 week horizon (+0.059 at h = 20).
   - It looks useful at a lower weight, e.g. a 0.2 weight instead of ⅓.
   - Testing either would need a new pre-registered test on data after 17 Nov 2026, so that this sample isn't reused to pick the weight.

## Limitations (as stated in advance)
- The complexes co-move, and there are only 11 of them, so the t-statistics are approximate.
- The composite's turnover term shares its definition with the target, which gives the composite a head start. The question tested is what OI adds on top, which this design measures fairly.
