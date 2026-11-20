# ADR-Trend Edge Stress Test: Results

Run 28 Oct 2026, following `Docs/ADR_Edge_StressTest_Preregistration.md` (3 deviations are logged there). Code: `research/adr_stress_test.py` (parts B, E, C) and `research/fetch_nse_fo_bhav.py` (NSE F&O bhavcopy: 56 monthly series, Dec 2021–Oct 2026, no missing days).

> **Corrected 20 Nov 2026: the verdict is now a fail on criterion 1.** The 28 Oct run used revenue for Jan 2022 – Aug 2024 that had been scaled to MCX's reported quarterly results, which were not public on those days. On a point-in-time series the pooled 20-day IC is 0.26 (t 1.80), below the pre-registered t ≥ 2. See [Correction, 20 Nov 2026](#correction-20-nov-2026-point-in-time-revenue). The original sections below are kept as run, for the record.

## Correction, 20 Nov 2026: point-in-time revenue

### What was wrong

`mcx_daily_revenue` rows from 3 Jan 2022 to 30 Aug 2024 (source `mcx_historical_backfill`, 654 days) are not MCX's daily figures. They were rebuilt from daily notional with one scale factor per calendar quarter, k_q, chosen so the quarter summed to MCX's **reported** revenue:

revenue = futures notional × k_q × 2 / 10⁷ + options notional × 0.016 · k_q × 2 / 10⁷ + ₹0.95 Cr

MCX publishes a quarter's results about 45 days after it ends, so on any day in that period the series already "knew" the quarter's reported revenue. k_q, read back from the rows, is constant within each quarter:

| Quarter | Jan–Mar 22 | Apr–Jun 22 | Jul–Sep 22 | Oct–Dec 22 | Jan–Mar 23 | Apr–Jun 23 | Jul–Sep 23 | Oct–Dec 23 | Jan–Mar 24 | Apr–Jun 24 | Jul–Aug 24 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| k_q | 359.81 | 423.53 | 512.53 | 389.31 | 407.38 | 445.35 | 559.40 | 616.53 | 652.38 | 603.44 | 1050.34 |

The series steps at quarter boundaries (k alone moves +21% into Jul 2022, −24% into Oct 2022, +26% into Jul 2023 and +74% into Jul 2024), and ADR-trend signals pick those steps up: a rising trend at the start of a strong quarter was partly the results, not the trading. Rows from 2 Sep 2024 are MCX's actual daily data. Primary-variant signals from early Dec 2024 (the slowest variant, v60/40, from late Jan 2025) no longer touch the backfill, so 2025 and 2026 barely change.

### The fix (study script only; the table is unchanged)

`research/adr_stress_test.py --rev pit` re-scales each backfill row with the k of the latest quarter whose results were out (quarter end + 45 days, used from the next day), and the first quarter's k before any result is out (Jan – mid-May 2022, warm-up only). k switches on 16 Aug 2022, 15 Nov 2022, 15 Feb 2023, 16 May 2023, 16 Aug 2023, 15 Nov 2023, 15 Feb 2024, 16 May 2024 and 16 Aug 2024; Jul–Aug 2024 uses 652.38 then 603.44 instead of 1050.34. As a check with no results input at all, `--rev flat` uses one k (Jan–Mar 2022) for every backfill row, so revenue there moves only with notional. `Bcmp` runs part B on all three series.

Monthly mean daily revenue, ₹ Cr (reported → point-in-time): Jul 2022 3.46 → 2.71, Oct 2023 3.97 → 3.13, Jul 2024 6.76 → 4.56, Aug 2024 7.29 → 4.74.

### Part B, before → after

Rerun 20 Nov 2026 with prices to date. "Reported, rerun" repeats the original method on today's data; it differs from 28 Oct by a few hundredths because of the extra weeks of data and price revisions.

| Pre-registered criterion | 28 Oct (published) | Reported, rerun | **Point-in-time** | Flat k | Pass on point-in-time? |
|---|---|---|---|---|---|
| 1. Pooled OOS 20-day IC > 0, t ≥ 2 (2023–2026) | 0.29, t 2.04 | 0.30, t 2.11 | **0.26, t 1.80** | 0.18, t 1.24 | ❌ |
| 2. Positive in ≥ 3 of 4 test years | 3/4 | 3/4 | **3/4** | 3/4 | ✅ |
| 3. ≥ 6 of 9 variants positive | 9/9 | 9/9 | **9/9** | 9/9 | ✅ |
| Pooled OOS 10-day IC (not a criterion) | 0.22, t 2.17 | 0.23, t 2.30 | 0.24, t 2.39 | 0.17, t 1.69 | |

Primary variant (45-day ADR, 20-day lookback), 20-day IC by year:

| Year | 28 Oct (published) | Reported, rerun | **Point-in-time** | Flat k |
|---|---|---|---|---|
| 2022 (warm-up) | −0.05 | −0.05 | 0.39 | 0.35 |
| 2023 | 0.56 | 0.56 | **0.48** | 0.41 |
| 2024 | 0.34 | 0.34 | **0.16** | 0.04 |
| 2025 | 0.38 | 0.41 | **0.41** | 0.41 |
| 2026 | −0.02 | −0.00 | **−0.00** | −0.00 |

Variants, pooled OOS 20-day IC (reported rerun → point-in-time): v20/10 0.13 → 0.12 · v20/20 0.15 → 0.11 · v20/40 0.26 → 0.24 · v45/10 0.28 → 0.25 · **v45/20 0.30 → 0.26** · v45/40 0.35 → 0.23 · v60/10 0.29 → 0.20 · v60/20 0.35 → 0.21 · v60/40 0.39 → 0.24. The slower variants lose the most: **"slower variants are stronger" was mostly the leak.** On the point-in-time series all nine sit between 0.11 and 0.25, none with t ≥ 2.

Walk-forward selection (variant chosen on data known before each year), pooled OOS 20-day IC: 0.36, t 2.62 → **0.15, t 1.06**. It now picks v45/20 for 2023, v20/40 for 2024, v20/10 for 2025 and v45/20 for 2026, scoring 0.48, 0.05, 0.15 and −0.00.

### Corrected verdict

**Fail on criterion 1** (was: conditional pass). The pre-registration says a failure on criteria 1–3 means the edge is not robust and the plan is rethought. What survives: the sign is positive in 2023–2025 and in all nine variants, and the 10-day IC still clears t 2 (not a pre-registered test). What does not: the 20-day IC at the pre-registered bar, the case for the slower variants, and 2024 (0.34 → 0.16). 2026 was already flat.

### Part C, before → after

Same 56 NSE bhavcopy series and costs (refetched 20 Nov, with `nse_days.json` taken from MCX.NS daily history, Dec 2021–Oct 2026; the reported-series rerun reproduces every 28 Oct Part C figure). The point-in-time series flips the signal's sign at entry on 12 of 46 Schedule A series and 10 of 47 Schedule B series, all between Oct 2022 and Sep 2024. Half-spread 2.5%, mean return per trade on capital at risk:

| | R2, 28 Oct → PIT | Always-bull benchmark | R2 bear trades, 28 Oct → PIT | Criterion 4 on PIT |
|---|---|---|---|---|
| A·S2 debit spread | +16.3% → +31.9% (t 0.96) | +2.0% | +4.7% → +57.7% (n 15, a few large winners) | ✅ by the letter |
| B·S3 credit spread | +12.1% → +13.2% (t 1.27) | +10.7% | +1.3% → +11.7% (n 10) | ✅ by the letter |
| B·S1 long option | +3.6% → −26.9% | +15.4% | −29.3% → −40.7% | ❌ |
| B·S2 debit spread | +4.8% → −11.8% | +11.0% | −13.2% → −21.5% | ❌ |

Criterion 4 still holds by the letter, with no significance (as before), but the verdict already fails on criterion 1.

The post-hoc bullish filter on Schedule B (signal > 0 → bullish trade, otherwise flat), which v1 paper trading uses, weakens:

| Structure | Signal > 0 (taken), 28 Oct → PIT | Signal ≤ 0 (skipped), 28 Oct → PIT | Always bullish |
|---|---|---|---|
| S1 long call | +40.0% → +32.8% (t 1.34), n 28 | −20.9% → −10.4% | +15.4% |
| S2 call debit spread | +32.0% (t 1.78) → +23.5% (t 1.27), 61% hit, n 28 | −20.0% → −7.5% | +11.0% |
| S3 put credit spread | +22.0% (t 2.32, 75% hit) → +14.6% (t 1.38, 68% hit), n 28 | −6.9% → **+4.6%** | +10.7% |

On the point-in-time series the put credit spreads the filter skipped made money on average, so "the signal keeps us out of fading-momentum trades" no longer holds for S3, and no taken-trade mean clears t 2.

### What this changes

- The 28 Oct plan rested on a pass. On point-in-time data the pre-registered test fails, so per §D the edge is not shown to be robust; the bullish filter is a hypothesis for forward paper trading to test, not a tested edge.
- "Track v60/40 as well" no longer has support from this data: slower variants are not stronger once the leak is removed.
- Not rerun: Part E's partial ICs (they control for the ADR trend on the same series; no feature passed before, and none of them is used).
- The Signals → Options page quotes the Schedule B backtest as its benchmark (put credit spread +22%, 75% winners; call debit spread +32%, 61% winners; 28 trades each). On point-in-time revenue those are +14.6%, 68% and +23.5%, 61%. That figure is in `OPTIONS_BACKTEST` (`api/models.py`) and is not changed here.

## Verdict (28 Oct 2026, superseded)

**Conditional pass.** The ADR trend is a real but modest edge on MCX Ltd's 2–4 week returns. Expressed through options, it works as a **bullish filter**: be long when the trend is rising, stand aside when it isn't. It does **not** work as a two-sided "buy calls / buy puts" machine, because bearish trades lost money in almost every structure. The option-level advantage over simply always being bullish is not statistically significant (t < 2), and 2026 is weak. Proceed to paper trading with bullish recommendations only. Bearish recommendations stay switched off until forward evidence supports them.

| Pre-registered criterion | Result | Pass? |
|---|---|---|
| 1. Pooled out-of-sample 20-day IC > 0, t ≥ 2 (2023–2026) | IC 0.29, t 2.04 (10-day: 0.22, t 2.17) | ✅ just |
| 2. Positive in ≥ 3 of 4 test years | 2023 0.56, 2024 0.34, 2025 0.38, **2026 −0.02** | ✅ 3/4 |
| 3. ≥ 6 of 9 variants positive | 9/9 (0.16 to 0.39; slower variants stronger) | ✅ |
| 4. An option structure at half-spread 2.5%: mean > 0, above the always-bull benchmark, bear trades ≥ −10% | A·S2·R2: +16.3% per trade vs benchmark +2.0%, bear +4.7%. B·S3·R2: +12.1% vs +10.7%, bear +1.3% | ✅ by the letter; t only 0.85 and 1.21 |

## Part B: walk-forward on the stock (28 Oct 2026, reported series)

- Per-year 20-day IC for the primary variant (45-day ADR, 20-day lookback): 2022 −0.05 (warm-up), 2023 0.56, 2024 0.34, 2025 0.38, 2026 −0.02.
- **Slower variants are stronger and held up in 2026.** Walk-forward selection, using only data known before each year, picked v20/10 for 2023 and then v60/40 for 2024–2026. It scored 0.31 in 2026, where the primary scored −0.02. Pooled out-of-sample for the selected variants: 20-day IC 0.36, t 2.6.
- Variant pooled out-of-sample 20-day IC: v20/10 0.16 · v20/20 0.18 · v20/40 0.26 · v45/10 0.28 · **v45/20 0.29** · v45/40 0.35 · v60/10 0.30 · v60/20 0.36 · v60/40 0.39 (t 2.8).

## Part C: priced through MCX Ltd options (bhavcopy closes, all costs)

Schedule A = enter the day after the previous expiry (about 18 trading days held). Schedule B = enter 10 trading days before expiry (about 8 held). Both exit 2 days before expiry. Figures are mean return per trade on capital at risk, at a 2.5% half-spread.

| | R1 (always in) | R2 (strong signal only) | Always-bull benchmark | R1 bull trades | R1 bear trades |
|---|---|---|---|---|---|
| A·S0 futures | +1.0% | +3.5% | +3.3% | +4.1% | −2.5% |
| A·S1 long option | +9.7% | +47.0% | +34.5% | +50.7% | −35.1% |
| A·S2 debit spread | −11.4% | **+16.3%** | +2.0% | +1.1% | −26.5% |
| A·S3 credit spread | −8.6% | +7.0% | +14.5% | +10.8% | −29.9% |
| B·S0 futures | +1.1% | +0.9% | +2.1% | +2.6% | −1.3% |
| B·S1 long option | +13.3% | +3.6% | +15.4% | +40.0% | −28.4% |
| B·S2 debit spread | +14.4% | +4.8% | +11.0% | +32.0% | −13.1% |
| B·S3 credit spread | +8.4% | **+12.1%** | +10.7% | +22.0% (t 2.32, 75% hit) | −11.6% |

Breakeven half-spread (the bid/ask at which mean return reaches zero): A·S2·R2 4.3%, B·S3·R2 8.3%, B·S2·R1 5.3%, B·S3·R1 5.9%. Plain long options (S1) are dominated by a few large winners (median trade −25% to −78%), which makes them a fragile product.

### The clearest pattern (post-hoc, **not** pre-registered)

Comparing "bullish only when the signal is positive, otherwise flat" with "always bullish" on Schedule B:

| Structure | Signal > 0 (taken) | Signal ≤ 0 (skipped) | Always bullish |
|---|---|---|---|
| S1 long call | +40.0% (n 28) | −20.9% (n 19) | +15.4% |
| S2 call debit spread | +32.0% (t 1.78) | −20.0% | +11.0% |
| S3 put credit spread | +22.0% (t 2.32, 75% hit) | −6.9% | +10.7% |

The signal earns its keep by **keeping us out of bullish trades when revenue momentum is fading**, not by calling declines. Because this was found after looking at the data, it must be confirmed in forward paper trading before it counts.

### Data-quality caveats

- Prices are the bhavcopy **close**, not executable bid/ask. Legs with no trades that day used NSE's settlement price: 24–29% of spread trades on Schedule A and 4–19% on Schedule B had at least one such leg. OTM strikes on MCX Ltd are thinly traded.
- Costs use current STT and exchange rates throughout (conservative for 2022–24). Brokerage is ₹20 per leg per order.
- Samples are small: 46–47 trades per cell, 12–16 per bull/bear subset under R2. None of the option-level edges over the benchmark is significant.
- The sample covers a strong MCX bull run (2022–2025). The always-bull benchmark is positive in every structure, so bearish results are measured against a headwind.

## Part E: other MCX data as predictors

No feature cleared the multiple-testing bar (|t| ≥ 2.7). Non-overlapping 20-day IC, raw / after controlling for the ADR trend:

| Feature | Raw | Partial | Note |
|---|---|---|---|
| E1 options turnover trend | −0.07 | −0.11 | |
| E2 options share of turnover, shift | **−0.24 (t −1.8)** | −0.21 | Best lead: a rising options share points to weaker returns. Worth tracking |
| E3 bullion share shift | −0.12 | −0.11 | |
| E4 iCOMDEX volatility trend | 0.08 | 0.05 | 2016+ |
| E5 valuation gap | −0.07 | −0.07 | |
| E6 ensemble score | 0.24 (t 1.8) | 0.13 | Mostly duplicates the ADR trend (correlation 0.46) |
| E7 margin trend | 0.50 | 0.52 | Only 8 non-overlapping observations; can't be judged |
| E8 prop participant share | −0.31 | −0.22 | Sep 2024+ only (n 23) |

Data gaps worth fixing: margins before Apr 2026 are sparse, participant OI only starts in Sep 2024, and commodity OI only starts in May 2026. More history on these is the cheapest way to find a second edge.

## What changes in the plan (28 Oct 2026, superseded by the correction above)

1. **Product v1 = a bullish filter.** When the ADR trend is rising, recommend a **put credit spread or call debit spread about 10 trading days before expiry** (Schedule B). Otherwise, no trade. No bearish recommendations in v1.
2. **Track both v45/20 and v60/40 forward.** The slower variant was stronger and held up in 2026, but choosing it now would be fitting to these results.
3. **Calibrate the spread costs with live Dhan bid/ask** before any real money. The edge needs a half-spread below about 4–8%, depending on structure.
4. **Paper trade at least 6 monthly series** before any real money, with bullish and bearish signals both logged (bearish ones as shadow trades only).
