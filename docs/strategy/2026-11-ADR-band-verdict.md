# ADR band strategy: verdict (MCX, BSE)

20 Nov 2026 · first pass, pre-registered (`2026-11-preregistration.md`) ·
reproduce with `scripts/mcx_uv.sh python -I research/strategy/run_verdict.py`

## The view

| CIO rule | Verdict | The numbers (out of sample, after costs) |
|---|---|---|
| **Price vs 45-day ADR line is fair value** | **Descriptive yes, anchor no** | R² is 0.87 (MCX) and 0.88 (BSE), but MCX price does **not** revert to the line: it fails the cointegration test (ADF −2.1 vs −3.3 needed) and a gap takes about 96 trading days to halve. BSE is borderline (10% level, half-life 28 days). |
| **Trade at ±2 SD** | **Not proven** | About 1 trade per stock per year. Only 6 trades in the combined test period: +15.5% average, t 2.4. After correcting for the 39 variants tried, the chance it's real is only 0.10 (MCX) and 0.17 (BSE). MCX's train period lost money (Sharpe −0.7). |
| **Turning up:** 10d > 45d and price ≥1 SD below | **Most promising, not yet proven** | 40 episodes across MCX and BSE: **+5.0% above the average 20-day return**, hit rate 74% (BSE) to 92% (MCX). Long-only test Sharpe is 1.5 (MCX) and 2.4 (BSE); the chance it's real is 0.47 and 0.80. |
| **Turning down / shorts at the top of the band** | **Fails** | After the MCX signals the stock still rose: shorts won only 32–40% of the time at 20 days. The top of the band means a below-average return, not a fall. |
| **"While 10d > 45d the stock doesn't fall"** | **False as stated** | With the 10-day above the 45-day, MCX still fell over the next 20 days 30% of the time, and BSE 38%. MCX did earn more in those windows (+7.0% vs +3.4%); BSE didn't (+5.0% vs +6.3%). |

**Bottom line**

- The ADR line is a good *picture* of fair value, but not a reliable *trading band*.
- Use the band's **lower half as a buy-watch flag**. Make **"turning up" the one
  live signal**, long only, with small size until it has more history.
- **Don't short** off the top of the band.
- The edge that does pass is **revenue momentum, long only**:
  - **MCX**: long only while the 10-day ADR is above the 45-day. Test Sharpe 2.3,
    max drawdown −20%, in the market 57% of the time; the chance it's real is 0.85.
    That passes the pre-set test.
  - **Revenue z-score rule** (March brief): test Sharpe 2.9, chance it's real 0.96.
  - This matches the earlier ADR stress test: revenue *trend* predicts, the
    valuation *gap* doesn't.

### Today (19 Nov 2026 close)

| | Price | Line | z (week avg) | −2 SD / +2 SD | 10d / 45d | State |
|---|---|---|---|---|---|---|
| MCX | ₹3,645 | ₹3,329 | +0.91 (+1.01) | ₹2,634 / ₹4,024 | 1.03 | In band, revenue trend up |
| BSE | ₹3,765 | ₹4,064 | −0.90 (−0.32) | ₹3,401 / ₹4,727 | 0.95 | In band, revenue trend down; turning up would need 10d > 45d with price below ₹3,733 |

**What the line implies for MCX** (expanding fit: price = −391 + 287 × ADR45):

| ADR | ₹12 Cr | ₹15 Cr (house FY28) | ₹18 Cr |
|---|---|---|---|
| Implied price | ₹3,054 | ₹3,916 | ₹4,777 |

Those are line values, not targets. FY27/FY28 targets come from the 4-check blend
(week 2).

---

## Appendix

### A. Data and method

- **MCX**
  - Revenue: 1,227 days, Jan 2022 – 19 Nov 2026, Muhurat excluded.
  - Price: MCX.NS, split-adjusted.
  - Evaluated from 1 Jul 2022. Train through Mar 2025, test from Apr 2025.
- **BSE**
  - Revenue: `codex_bse_daily`, 901 days.
  - Evaluated from Jan 2024. Train 2024, test 2025 onwards.
- **No look-ahead**
  - The line and SD are refitted each day on data up to the day before (minimum
    120 days).
  - Trades enter at the next open.
  - A test checks that changing future rows never changes past signals.
- **Costs**: 0.10% per round trip, plus 0.05% per monthly futures roll.
- **Deflated Sharpe** ("the chance it's real") corrects for trying 39 variants
  per stock. The pre-set pass bar is above 0.8.
- **Deviations from the pre-registration** (logged in that doc):
  - a time-stopped position can only re-enter after a fresh signal;
  - the MCX evaluation start date.

### B. Is there a band? (cointegration, full evaluation sample)

| | R² | ADF t (5% needs −3.34) | Rejects random drift at | Half-life (days) |
|---|---|---|---|---|
| MCX, level | 0.87 | −2.14 | no | 96 |
| MCX, log | 0.81 | −1.77 | no | 182 |
| MCX from Nov 2024 (current fees) | 0.71 | −1.50 | no | 56 |
| BSE, level | 0.88 | −3.05 | 10% | 28 |
| BSE, log | 0.88 | −3.23 | 10% | 28 |

### C. Event study

First day of each episode only. "Excess" is the return above the average 20-day
return over the same period. For shorts, results are signed so that + means the
rule was right.

| Event | MCX n | MCX 20d (excess, t, hit) | BSE n | BSE 20d (excess, t, hit) | Pooled n, excess |
|---|---|---|---|---|---|
| z ≤ −2, long | 5 | +15.8% (+10.4, 2.4, 100%) | 12 | +4.3% (−1.2, −0.3, 58%) | 17, +2.2% |
| z ≥ +2, short | 20 | −0.9% (+4.4, 2.0, 40%) | 15 | −2.5% (+3.0, 1.1, 40%) | 35, +3.8% |
| Turning up, long | 13 | +10.7% (+5.4, 2.4, 92%) | 27 | +10.3% (+4.8, 1.4, 74%) | 40, +5.0% |
| Turning down, short | 38 | −5.7% (−0.3, −0.2, 32%) | 22 | +1.9% (+7.4, 2.2, 77%) | 60, +2.5% |
| 10/45 cross up, long | 36 | +6.3% (+0.9, 0.5, 78%) | 45 | +8.7% (+3.2, 1.1, 53%) | 81, +2.2% |

At 60 days, z ≤ −2 longs gained +23% (MCX) and +27% (BSE) more than the average
day. That's the strongest single number in the study, but it's 17 episodes, and
some of them fall in the same weeks.

### D. Strategy backtests

Columns: Sharpe for train / test, max drawdown, test trades and the chance the
result is real (deflated Sharpe). "L/S" is long/short.

| Rule | MCX Sharpe | MCX DD | MCX trades, real | BSE Sharpe | BSE DD | BSE trades, real |
|---|---|---|---|---|---|---|
| R1 band 2 SD, L/S | −0.66 / 0.52 | −23% | 5, 0.10 | 0.30 / 1.20 | −19% | 3, 0.17 |
| R1 long only | 1.75 / 0.54 | −14% | 1, 0.11 | 1.58 / 1.22 | −17% | 1, 0.17 |
| R2 turning, L/S | −0.38 / −0.25 | −39% | 5, 0.01 | −0.10 / 1.90 | −35% | 8, 0.51 |
| R2 long only | 1.56 / 1.46 | −14% | 2, 0.47 | 1.18 / 2.42 | −17% | 4, 0.80 |
| R4 trend 10/45, L/S | 1.36 / 1.40 | −47% | 30, 0.44 | 0.43 / 2.10 | −25% | 50, 0.61 |
| **R4 long only** | **1.93 / 2.31** | **−20%** | **15, 0.85** | 1.32 / 1.95 | −17% | 25, 0.53 |
| Revenue z20 > 0.5, hold 5d | 1.14 / **2.87** | −15% | 22, **0.96** | 2.14 / 1.46 | −31% | 7, 0.27 * |
| Buy and hold | 1.59 / 2.16 | −24% | — | 1.85 / 1.11 | −37% | — |

\* For BSE this rule is in the market 96% of the time. Weekly expiry days push the
20-day z above 0.5 almost every week, so it behaves like buy-and-hold. It needs a
day-of-week adjustment before it means anything for BSE.

**Pooled test-period trades** (MCX + BSE):

| Rule | Trades | Average | t |
|---|---|---|---|
| R1 | 6 | +15.5% | 2.4 |
| R2 | 12 | +16.3% | 2.2 |
| R4 | 78 | +3.3% | 2.4 |

**MCX on the current-fee sample only** (fit from Nov 2024, test 2026):

- R1 test Sharpe 0.04; R4 long only 0.86; buy-and-hold 1.53.
- The edges are weaker in 2026, with only about 10 months of test.

### E. Grid robustness (test Sharpe, all variants)

- **BSE**: R1 is positive in 26 of 27 band variants (median about 1.1). R2
  expanding / rolling fits are 1.9–2.5, but the log fits are negative.
- **MCX**: R1 ranges from −0.6 to +1.4 with median about 0.2, so the sign depends
  on the settings. R2 is negative in 7 of 9. R4 is 1.4–2.3 across the
  10/30, 10/45 and 10/60 versions.
- The trend rule is the only family that is positive in every setting on both
  stocks.

### F. Limits

- **One market regime.** MCX rose about 12× from 2023 and BSE about 6× from Jan 2024.
  Every long rule looks good and every short looks bad. "Excess over the average"
  is the fairer read.
- **Old MCX fees.** MCX revenue before Oct 2024 is restated at today's flat fee
  rate. The Nov 2024 onwards sample is shown separately.
- **BSE short trades** before BSE's stock futures listing are hypothetical; check
  the inclusion date before trusting the BSE short legs.
- **Few events.** The band rules give about 1 trade per stock per year. Adding
  Angel One (week 2) and the other ~17 stocks is what will make these tests
  conclusive.

### G. Next

1. **Tracker page** (`/signals/bands`): state per stock, with the lower-band and
   turning-up flags; the trend state as context.
2. **Angel One** from `sb_daily_revenue`, then the 4-check blend: test each check
   on its own, then compare 45/45/10 with weights fitted on past data.
3. **BSE revenue** adjusted for day of week, so the z-score rule means something.
4. **Options overlay** for downside views, instead of shorting futures off the band.
