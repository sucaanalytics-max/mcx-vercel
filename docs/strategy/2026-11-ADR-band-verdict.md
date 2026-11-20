# ADR band strategy: verdict (MCX, BSE, Angel One)

20 Nov 2026 · pre-registered (`2026-11-preregistration.md`; Angel One in Addendum A) ·
reproduce with `scripts/mcx_uv.sh python -I research/strategy/run_verdict.py`

## The view

| CIO rule | Verdict | The numbers (out of sample, after costs) |
|---|---|---|
| **Price vs 45-day ADR line is fair value** | **Descriptive for exchanges, weak for Angel One** | R² is 0.87 (MCX) and 0.88 (BSE), but only 0.24 for Angel One, whose modelled revenue explains little of its price. MCX price does **not** revert to the line: it fails the cointegration test (ADF −2.1 vs −3.3 needed) and a gap takes about 96 trading days to halve. BSE is borderline (10% level, half-life 28 days). |
| **Trade at ±2 SD** | **Not proven** | About 1 trade per stock per year: only 8 trades across the three test periods, averaging +13.3% (t 2.4). After correcting for the 39 variants tried, the chance it's real is only 0.10 (MCX), 0.17 (BSE) and 0.22 (Angel One). |
| **Turning up:** 10d > 45d and price ≥1 SD below | **Most promising, not yet proven** | **54 episodes** across the three stocks: **+5.7% above the stock's average 20-day return (t 2.8)** and +12.9% over 60 days (t 3.5). Long-only test Sharpe is 1.5 (MCX), 2.4 (BSE) and 0.8 (Angel One); the chance it's real is 0.47, 0.80 and 0.32. |
| **Turning down / top of the band** | **Fails as a short; works as an exit flag** | In this rising market, shorts lost money on MCX. Relative to each stock's average, the signals did work: returns after the upper band were 3.8% below average (40 episodes, t 2.5), and after turning down 3.1% below (75 episodes, t 2.1). On Angel One, which fell in its train period, a turn down was right 67% of the time. |
| **"While 10d > 45d the stock doesn't fall"** | **False as stated** | With the 10-day above the 45-day, the stock still fell over the next 20 days 30% (MCX), 38% (BSE) and 46% (Angel One) of the time. Returns were higher in those windows for MCX (+7.0% vs +3.4%) and Angel One (+2.0% vs +1.3%), but not for BSE (+5.0% vs +6.3%). |

**Bottom line**

- The ADR line is a good *picture* of fair value for the exchanges, but not a
  reliable *trading band*. For a broker like Angel One it's a poor picture.
- Use the band's **lower half as a buy-watch flag**. Make **"turning up" the one
  live signal**, long only, with small size until it has more history.
- Use the **upper band and "turning down" to trim or exit longs**, not to short.
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
| Angel One (18 Nov) | ₹338 | ₹303 | +1.09 (+1.39) | ₹238 / ₹368 | 0.98 | **Turning down**: an exit flag for longs, not a short |

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

| Event | MCX n | MCX 20d (excess, t, hit) | BSE n | BSE 20d (excess, t, hit) | MCX + BSE n, excess |
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

### E2. Angel One (Addendum A) and the three-stock pool

**Data.** Angel One's daily revenue is the house model in the NSE-BSE project
(`sb_daily_revenue`, latest run): F&O, cash, commodity and net MTF interest.

- **Coverage:** 1 Oct 2024 → 18 Nov 2026; about 89% of FY26 operating revenue.
- **Windows:** evaluated from Jul 2025; train through Dec 2025, test 2026.
- **Look-ahead:** each quarter is scaled by take rates fitted to that quarter's
  reported results. The same rules on orders only (no fee scaling) are shown
  beside it.

| Angel One | Revenue | Orders only |
|---|---|---|
| R², price on ADR45 | 0.24 | 0.09 |
| Cointegration (ADF t, half-life) | −1.92, 31 days, not cointegrated | −1.51, 45 days, not cointegrated |
| Turning up, 20d excess (n, t, hit) | +7.9% (14, 1.9, 64%) | +11.6% (12, 2.2, 67%) |
| Turning up, 60d excess | +14.7% (t 2.6) | +24.7% (t 4.8) |
| Turning down, 20d excess (signed) | +5.3% (15, t 3.1, right 67%) | +3.5% (23, t 2.6) |
| z ≤ −2, 20d excess | +13.7% (6, t 2.5) | 1 episode |
| R1 band L/S test Sharpe (chance real) | 0.50 (0.22) | 0.53 (0.19) |
| R2 turning long-only test Sharpe | 0.82 | 0.11 |
| R4 trend long-only test Sharpe | 1.09 | 1.53 |
| Buy and hold test Sharpe | 1.33 | 1.33 |

Like BSE's, Angel One's revenue z-score rule is in the market nearly all the time
(weekly expiry spikes in F&O), so it adds nothing over holding the stock.

**Pooled events, MCX + BSE + Angel One (revenue).** Each event's return is
signed, minus that stock's average return over the same horizon. "Beat avg" is
the share of events that did better than the stock's average.

| Event | n | 20d excess (t, beat avg) | 60d excess (t, beat avg) |
|---|---|---|---|
| Turning up, long | 54 | **+5.7%** (2.8, 63%) | **+12.9%** (3.5, 61%) |
| z ≤ −2, long | 23 | +5.2% (1.7, 61%) | +24.6% (4.5, 91%) |
| z ≥ +2, short (below-average return) | 40 | +3.8% (2.5, 70%) | +8.9% (2.4, 72%) |
| Turning down, short (below-average return) | 75 | +3.1% (2.1, 63%) | +9.7% (3.9, 72%) |
| 10/45 cross up, long | 113 | +1.7% (1.1, 50%) | +4.5% (1.5, 47%) |
| 10/45 cross down, short | 113 | −0.3% (−0.2, 52%) | −2.1% (−0.8, 55%) |

**Pooled test-period trades** (three stocks):

| Rule | Trades | Average | t |
|---|---|---|---|
| R1 | 8 | +13.3% | 2.4 |
| R2 | 16 | +12.7% | 2.1 |
| R4 | 126 | +2.1% | 2.4 |

The t-stats overstate certainty:
- episodes in the same stock a few days apart are not independent;
- 60-day windows overlap heavily.

Read them as direction and rough size.

What Angel One adds:
- **Turning up** holds on a third stock, and on its activity-only version.
- **The band's upper side** works as a below-average-return flag on all three
  stocks.
- **The price-on-ADR line** is a poor fit for a broker, where revenue is a model
  and the share price also reflects client growth and MTF.

### F. Limits

- **One market regime.** MCX rose about 12× from 2023 and BSE about 6× from Jan 2024.
  Every long rule looks good and every short looks bad. "Excess over the average"
  is the fairer read. Angel One fell about 22% in its train period (Jul–Dec 2025), which is why its
  turn-down signals were right as shorts.
- **Angel One's revenue is a model** with quarterly look-ahead (see E2), and its
  sample is about 340 evaluation days.
- **Old MCX fees.** MCX revenue before Oct 2024 is restated at today's flat fee
  rate. The Nov 2024 onwards sample is shown separately.
- **BSE short trades** before BSE's stock futures listing are hypothetical; check
  the inclusion date before trusting the BSE short legs.
- **Few events.** The band rules give about 1 trade per stock per year. Adding
  the other ~17 stocks is what will make these tests conclusive.

### G. Next

1. **Done:** the tracker page (`/signals/bands`) shows MCX, BSE and Angel One.
2. **The 4-check blend:** test each check on its own, then compare 45/45/10 with
   weights fitted on past data.
3. **Revenue adjusted for day of week** (BSE, Angel One), so the z-score rule
   means something.
4. **Paper-trade "turning up"** from the tracker, to build forward-looking
   evidence.
5. **Options overlay** for downside views, instead of shorting futures off the band.
