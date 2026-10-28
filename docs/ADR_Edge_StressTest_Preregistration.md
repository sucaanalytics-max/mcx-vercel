# ADR-Trend Edge Stress Test: Pre-registration

Written 28 Oct 2026, **before** any of the results below were computed. The rules here are fixed: results are reported for every variant listed, and nothing is added or dropped after seeing outcomes. Any later deviation is listed in a "Deviations" section with the reason.

**What has already been seen (disclosure).** The 28 Oct edge test (`scripts/options_edge_test.py`) reported the full-sample stock-level IC of the primary variant (45-day ADR, 20-day lookback) at 5/10/20-day horizons. The primary variant was therefore chosen *after* seeing its full-sample IC, so its full-sample stock result is not independent evidence. The walk-forward, the eight other variants, the option pricing and the feature screen have not been seen.

---

## A. Signal and variants

- Revenue series: `mcx_daily_revenue.total_rev_cr`, MCX trading days, Jan 2022 onward.
- ADR_w(t) = mean of total revenue over the last w MCX trading days ending t.
- Signal `adr_trend(w, L)` = ln(ADR_w(t) / ADR_w(t − L)).
- **Primary:** w = 45, L = 20.
- **Variant grid (all reported):** w ∈ {20, 45, 60} × L ∈ {10, 20, 40} = 9 variants.
- Timing: the signal for MCX date t is known after 23:30 IST on t, so it can first be traded at the **next NSE session**.

## B. Walk-forward on the stock (MCX.NS, adjusted daily closes)

1. **Per-year IC:** for each calendar year 2022–2026, Spearman IC of the signal against the return from the next NSE open to the close h trading days later, h ∈ {10, 20}. Non-overlapping samples: mean IC across the h offsets.
2. **Fixed primary, pooled out-of-sample:** IC for 2023-01-01 → 2026-10-27, with 2022 treated as warm-up.
3. **Walk-forward selection:** at the start of each test year Y ∈ {2023, 2024, 2025, 2026}, pick the variant with the highest 20-day IC using data up to 31 Dec of Y − 1 only. Apply it to Y and report the pooled out-of-sample IC of the selected variants.
4. **Robustness across neighbours:** the number of the 9 variants with a positive pooled out-of-sample 20-day IC.

## C. Option pricing (NSE F&O bhavcopy, MCX stock options)

**Data.** NSE F&O bhavcopy: the old format to 5 Jul 2024, UDiFF after. Only MCX `OPTSTK`/`FUTSTK` rows are kept. Prices are the day's **close**; if a leg had zero contracts traded that day, the settlement price is used and the trade is flagged "stale". Underlying reference = near-month MCX futures close from the same file (this handles the 1:5 split on 2 Jan 2026 automatically).

**Schedules** (one trade per monthly series):
- **A (full series):** enter at the close of the first NSE trading day after the previous expiry; exit at the close two NSE trading days before this series' expiry. (Stock options are physically settled, so trades are always closed before expiry.)
- **B (half series):** enter at the close 10 NSE trading days before expiry; same exit.
- Signal used: `adr_trend` from the last MCX trading day **before** the entry day.

**Structures** (strikes from the series expiring at the end of the trade; ATM = strike nearest the futures price; OTM5 = strike nearest futures × 1.05 for calls / × 0.95 for puts, which must differ from ATM):
- **S0:** futures, delta-1 baseline
- **S1:** long ATM call (bull) / long ATM put (bear)
- **S2:** debit vertical. Bull: long ATM call, short OTM5 call. Bear: long ATM put, short OTM5 put.
- **S3:** credit vertical. Bull: short ATM put, long OTM5 put. Bear: short ATM call, long OTM5 call.

**Return per trade** = P&L ÷ capital at risk: the debit paid (S1, S2), the spread width minus the credit (S3), or the entry futures price (S0).

**Rules:**
- **R1:** direction = sign of the signal, always in the market.
- **R2:** trade only if |z| ≥ 0.5, where z standardises the signal by its **expanding** mean and standard deviation using values before the entry date only (at least 120 prior values); otherwise flat.
- **Benchmark B:** always bullish, same structure and schedule. It controls for MCX's strong upward drift over 2022–2026.

**Costs** (per share; current rates applied to the whole history, which is conservative):
- STT 0.1% of premium on every sell leg
- Exchange transaction charge 0.03503% of premium per leg
- SEBI fee ₹10 per crore
- Stamp duty 0.003% of premium on buy legs
- Brokerage ₹20 per leg per order ÷ lot size (lot size from UDiFF when available, otherwise 225)
- GST 18% on brokerage + transaction charges + SEBI fee
- Futures (S0): 0.02% STT on the sell side, plus transaction charge 0.00173%
- **Bid/ask:** a half-spread h, as a % of each leg's price (minimum ₹0.05), paid on every leg at entry and at exit. Grid h ∈ {0, 1, 2.5, 5, 10}%. Also report the **breakeven h** at which mean return per trade reaches 0. Live bid/ask calibration will come from Dhan once the token is refreshed.

**Report for each schedule × structure × rule:** number of trades, stale-flag count, mean return per trade, median, hit rate, t-stat, sum of returns, worst trade, maximum drawdown of the cumulative sum (equal risk per trade), per-year means, bull vs bear split, and the difference vs Benchmark B.

## D. Pass / fail criteria (decided now)

The edge **passes** only if all of these hold:
1. Primary variant, fixed rule, pooled out-of-sample 2023–2026: 20-day IC > 0 with t ≥ 2.
2. Positive 20-day IC in at least 3 of the 4 test years.
3. At least 6 of the 9 variants have a positive pooled out-of-sample 20-day IC.
4. At least one option structure, under R1 or R2 and on Schedule A or B, at h = 2.5%: mean return per trade after costs > 0 **and** above Benchmark B. Its bear trades must not have a mean below −10% per trade.

If it passes: Phase 1 proceeds with that structure as the default product. If it fails on 1–3: the edge is not robust and the plan is rethought. If 1–3 pass but 4 fails: the edge is real on the stock but does not survive option costs, so consider futures or a stock overlay instead of options.

Small-sample warning: Schedule A gives about 55 trades over 2022–2026. Option-level t-stats will be weak whatever happens, so criterion 4 is about the sign and the comparison with the benchmark, not significance.

## E. Feature screen: other MCX data (exploratory, multiple-tested)

Each feature is evaluated with the same timing rule. Report the non-overlapping 20-day IC with the stock (full available history) **and the partial IC after controlling for primary `adr_trend`** (feature residualised on the signal). With 8 tests, only |t| ≥ 2.7 (≈ Bonferroni 5%) counts as a finding; anything weaker is noted, not acted on.

| # | Feature | Source | History |
|---|---|---|---|
| E1 | Options turnover trend: ln(20-day mean OPTFUT turnover ÷ same 20 days earlier) | `mcx_commodity_daily` | 2022+ |
| E2 | Options share shift: 20-day change in OPTFUT share of total turnover | `mcx_commodity_daily` | 2022+ |
| E3 | Bullion share shift: 20-day change in the BULLION share of total turnover | `mcx_commodity_daily` | 2022+ |
| E4 | Commodity volatility trend: ln(20-day realised vol of iCOMDEX Composite ÷ the same 20 days earlier) | `mcx_icomdex_daily` | 2016+ |
| E5 | Valuation gap: close ÷ `fair_value_base` − 1 (a negative IC is expected) | `mcx_valuation` | 2022+ |
| E6 | Ensemble score | `mcx_model_signals` | 2022+ |
| E7 | Margin trend: 20-day change in mean `total_margin_pct` for GOLD, SILVER, CRUDEOIL near futures | `mcx_margin_daily` | 2024+ |
| E8 | Prop participant share: 20-day change in prop (long + short) share of total OI, all commodities | `mcx_oi_participants` | Sep 2024+ (short) |

Not screened, because history is too short or the data isn't suitable: `mcx_commodity_prices` (Mar 2026+), commodity OI (Dhan, May 2026+ only), intraday snapshots (already tested on 28 Oct).

## Deviations

1. **E7 (margins), 28 Oct.** As registered, E7 used `total_margin_pct` for near-expiry contracts. That kept only data from May 2026: rows before then (source `mcxccl_daily`) have no expiry, and the two sources define "total" differently. Changed to: `initial_margin_pct` for FUTCOM, accepting rows with no expiry, and dropping 20-day changes whose window crosses a change of source. This fix was made after seeing that the registered E7 returned no result, but **before** seeing any E7 IC.
3. **Split-spanning trades, Part C.** §C claimed that using the same-day futures price handles the 2 Jan 2026 1:5 split. It does not when the entry is before the split and the exit after: bhavcopy prices change units mid-trade, which produced a spurious −79% futures trade. Trades with entry < 2026-01-02 ≤ exit are excluded (Schedule A's Jan 2026 series only). This was found after seeing Part C output and is a data-correctness fix, not a choice.
2. **Minimum sample, Part B.** Per-year non-overlapping subsamples at h = 20 have about 9–12 observations, so the IC routine's minimum n was set to 8 (it was 20 in the 28 Oct script). This is mechanical: without it, per-year results could not be computed at all.
