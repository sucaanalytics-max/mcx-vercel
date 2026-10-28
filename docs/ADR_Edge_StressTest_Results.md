# ADR-Trend Edge Stress Test: Results

Run 28 Oct 2026, following `Docs/ADR_Edge_StressTest_Preregistration.md` (3 deviations are logged there). Code: `scripts/adr_stress_test.py` (parts B, E, C) and `scripts/fetch_nse_fo_bhav.py` (NSE F&O bhavcopy: 56 monthly series, Dec 2021–Oct 2026, no missing days).

## Verdict

**Conditional pass.** The ADR trend is a real but modest edge on MCX Ltd's 2–4 week returns. Expressed through options, it works as a **bullish filter**: be long when the trend is rising, stand aside when it isn't. It does **not** work as a two-sided "buy calls / buy puts" machine, because bearish trades lost money in almost every structure. The option-level advantage over simply always being bullish is not statistically significant (t < 2), and 2026 is weak. Proceed to paper trading with bullish recommendations only. Bearish recommendations stay switched off until forward evidence supports them.

| Pre-registered criterion | Result | Pass? |
|---|---|---|
| 1. Pooled out-of-sample 20-day IC > 0, t ≥ 2 (2023–2026) | IC 0.29, t 2.04 (10-day: 0.22, t 2.17) | ✅ just |
| 2. Positive in ≥ 3 of 4 test years | 2023 0.56, 2024 0.34, 2025 0.38, **2026 −0.02** | ✅ 3/4 |
| 3. ≥ 6 of 9 variants positive | 9/9 (0.16 to 0.39; slower variants stronger) | ✅ |
| 4. An option structure at half-spread 2.5%: mean > 0, above the always-bull benchmark, bear trades ≥ −10% | A·S2·R2: +16.3% per trade vs benchmark +2.0%, bear +4.7%. B·S3·R2: +12.1% vs +10.7%, bear +1.3% | ✅ by the letter; t only 0.85 and 1.21 |

## Part B: walk-forward on the stock

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

## What changes in the plan

1. **Product v1 = a bullish filter.** When the ADR trend is rising, recommend a **put credit spread or call debit spread about 10 trading days before expiry** (Schedule B). Otherwise, no trade. No bearish recommendations in v1.
2. **Track both v45/20 and v60/40 forward.** The slower variant was stronger and held up in 2026, but choosing it now would be fitting to these results.
3. **Calibrate the spread costs with live Dhan bid/ask** before any real money. The edge needs a half-spread below about 4–8%, depending on structure.
4. **Paper trade at least 6 monthly series** before any real money, with bullish and bearish signals both logged (bearish ones as shadow trades only).
