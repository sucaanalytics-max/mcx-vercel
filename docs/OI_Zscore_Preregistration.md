# Commodity OI z-score: pre-registration

Written 18 Nov 2026, **before** any result below was computed. The rules are fixed:
- Every variant listed is reported, and nothing is added or dropped after seeing outcomes.
- Any later change goes in a "Deviations" section of the results, with the reason.

**Disclosure: what has been seen.** On 29 Oct 2026, with Dhan OI, I measured only how many commodity-day *signals* would flip if OI joined the composite: 35% over the full history, 39% over the last 30 days. Nobody has measured whether OI predicts anything. The OI series has since been replaced by MCX's own Bhavcopy totals (2022 →, backfilled 17 Nov 2026), and no predictive statistic has been computed on it.

## Question

The Commodities page shows "activity signals". Each signal averages, per commodity complex, rolling 60-day z-scores of turnover and volume (`lib/cron_commodity_signals.py`). An OI z-score is computed and displayed but kept out (`OI_IN_COMPOSITE = False`). For an exchange-revenue dashboard, a signal earns its place by telling us about a commodity's **future trading activity**. So:

1. Does the OI z-score predict a commodity's future turnover *beyond* what the current composite (turnover z + volume z) already says, once the monthly expiry cycle is removed?
2. Is OI better measured in ₹ value or in lots?
3. Should OI join the composite?

## Data
- **Panel:** `mcx_commodity_daily`, 3 Jan 2022 → the last stored session (read 18 Nov 2026).
  - Aggregated per (date, parent complex) exactly as the cron does it: `COMMODITY_MAP`, all instrument rows summed.
  - Same filter as the cron: at least 60 rows and an average turnover of at least ₹50 Cr.
- **OI:** futures only (FUTCOM / FUTIDX). A day where any futures leg of the complex has OI = 0 is treated as missing, as in the cron.
- **Expiry dates:** every futures contract's expiry, read from MCX's Bhavcopy on one date per month (2022-01 → 2026-11). Each date lists all live contracts, so monthly sampling sees every expiry.
  - `dte(t)` = trading days from t to the complex's nearest front futures expiry on or after t, using the parent symbol's contracts.

## Predictors (all as rolling 60-day z-scores of the day's level, the cron's `_zscore`, minimum 30 observations)
- **C (current composite):** mean of turnover z and volume z.
- **OI_lots:** total futures OI in lots, summed across the complex as the cron does.
- **OI_val:** total futures OI in ₹ Cr, `oi_value_cr`.
- **Expiry-adjusted versions, OI_lots_adj and OI_val_adj:** the raw z minus the mean raw z in the same `dte` bucket.
  - Buckets: 0–2, 3–5, 6–10, 11–15, 16+ trading days.
  - Bucket means are estimated per complex on the **training period only** and applied unchanged to the test period.

## Target
- **Primary, `fwd_act_10`:** ln(mean turnover over days t+1 … t+10 ÷ mean turnover over days t−59 … t), per complex.
  - This measures how busy the next two weeks are relative to the recent norm.
- **Secondary:** the same with horizons 5 and 20 (`fwd_act_5`, `fwd_act_20`).

## Split
- **Training:** 2022-01-03 → 2024-12-31. Bucket means are estimated here; the statistics are reported for orientation.
- **Test:** 2025-01-01 → last session. This is the only period the decision uses.

## Statistics (each reported on the training and test periods, at every horizon)
1. **Mean cross-sectional rank IC.** For each date, the Spearman correlation across complexes between the predictor and the target. Report the mean over dates and a t-statistic from non-overlapping dates: every h-th date, with the mean taken over the h offsets, as in the ADR study.
2. **Pooled time-series IC per complex.** Spearman correlation of the predictor with the target within each complex. Report the median across complexes and how many are positive.
3. **Incremental (partial) IC of each OI variant given C.** Per complex, on the training period, regress the target on C. Then take the rank IC of the OI variant against the residual. Report the same mean IC and t-statistic as in item 1, on both periods.
4. **Composite with OI.** C_OI = mean of turnover z, volume z and OI z, using each OI variant in turn. Report its item-1 IC minus C's item-1 IC on the test period.
5. **Signal flips.** The share of test-period commodity-days whose five-level signal changes when OI is added, per variant, and whether the flipped days' forward activity agrees better with the new signal or the old one. Agreement means sign(target) matches the BUY/SELL side; NEUTRAL counts as neither.

## Decision rule (fixed)
OI joins the composite **only if one variant** among OI_lots, OI_val, OI_lots_adj and OI_val_adj meets **all** of these:
- (a) its test-period incremental IC at h = 10 is > 0 with t ≥ 2;
- (b) its training-period incremental IC at h = 10 has the same sign;
- (c) C_OI beats C on test-period IC at h = 10 by ≥ 0.02;
- (d) at h = 5 and h = 20 the test incremental IC is not negative.

If more than one variant passes, choose the highest test IC at h = 10. If none passes, OI stays out and the page keeps showing the OI z with a note on what it does and doesn't tell you.

Nothing is changed on the dashboard by this study. Any change goes in a separate PR that cites these results.

## Known limitations, stated in advance
- The windows overlap and the complexes co-move (a busy macro day lifts every commodity at once). The t-statistics are therefore approximate even with non-overlapping sampling.
- The cross-section is small: about 10 complexes pass the filter.
- Turnover includes options premium notional. OI is futures only.
