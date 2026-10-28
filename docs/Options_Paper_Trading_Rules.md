# MCX Ltd Options: Paper-Trading Rules (v1, bull-only)

Fixed 28 Oct 2026, before the first paper trade. Implements v1 from `Docs/ADR_Edge_StressTest_Results.md`. Code: `scripts/options_paper.py`. Ledger: `data/options_paper/`. **No real orders are placed.**

## Spread calibration (Dhan live bid/ask, 28 Oct 2026, 10:45–10:48 IST, 3 snapshots)

Median half-spread as a % of mid, out-of-the-money side:

| Expiry | ATM (<1.5%) | 1.5–3.5% | 3.5–6.5% (OTM5 legs) | 6.5–10% | 10–20% |
|---|---|---|---|---|---|
| 23 Nov 2026 (near) | 0.38 | 0.43 | 0.43 | 0.52 | 1.48 |
| 29 Dec 2026 (next) | 7.77 | 19.37 | 10.48 | 15.12 | 9.24 |

Crossing the bid/ask on a whole structure in the near month cost **0.8% of capital at risk** (bull put credit spread, 3150/3000) and **1.6%** (bull call debit spread, 3150/3300), per side. The backtest assumed 2.5% of each leg's price, and the edge broke even at half-spreads of 4–8%, so near-month execution leaves a wide margin. **Next-month options are not tradeable**, so the paper trades use the near month only. This is a single mid-morning sample; every run of the paper job also logs the near-month spread (`spreads.csv`), so this calibration keeps growing.

## Rules

1. **Series:** MCX Ltd stock options, nearest expiry (from Dhan `expiry_list`).
2. **Calendar:** entry = the 10th NSE trading day before expiry. Exit = the 2nd NSE trading day before expiry. Uses the NSE holiday list in the script. **2027 holidays must be added before the January 2027 series.** The script refuses to run past its holiday coverage.
3. **Fill time:** the job runs at 15:10 IST. Fills only count between 14:45 and 15:29. A missed entry is logged as `missed`, not filled late. A missed exit is filled at the next run before expiry and flagged `late_exit`.
4. **Signals:** computed for each variant from `mcx_daily_revenue`, using MCX days strictly before the entry day:
   - **v45/20** = ln(ADR45 ÷ ADR45 20 days earlier)
   - **v60/40** = ln(ADR60 ÷ ADR60 40 days earlier)

   If the latest revenue row is older than the previous NSE trading day, there is no trade (`signal_stale`).
5. **Books:** each variant runs its own books, 1 lot per trade.
   - **PCS** (primary): bull put credit spread. Sell the ATM put, buy the put nearest 0.95 × spot.
   - **CDS**: bull call debit spread. Buy the ATM call, sell the call nearest 1.05 × spot.
   - Opened only if that variant's signal is > 0. Each trade records the R2 flag: |z| ≥ 0.5, with z computed from the variant's expanding history.
   - **Shadow** (logged, never recommended): when a variant's signal is ≤ 0, the bear mirror trades (bear call credit spread, bear put debit spread) are logged with `shadow = true`.
6. **ATM** = the strike nearest the underlying's last price. The backtest used the near-month futures price; at 10 days to expiry the difference is about 0.15%, under a third of one 50-point strike step.
7. **Prices:** sell at the bid, buy at the ask, at entry and at exit. Mid prices are recorded too. A leg with no bid or no ask means no trade (`no_quote`).
8. **Costs:** the same model as the backtest (`lib/options_costs.py`): STT, exchange, SEBI, stamp duty, GST, and ₹20 brokerage per leg per order. The bid/ask is already paid through the fill prices.
9. **No stops, rolls or adjustments.** Every position is held from entry to exit.
10. **Return** = P&L after costs ÷ capital at risk, where capital at risk is the debit paid, or the spread width minus the credit received.

## Review gate

After **6 completed series** (about April 2027), compare each book's mean return per trade with the backtest:

| Book | Backtest reference |
|---|---|
| PCS, signal > 0, Schedule B | +22.0% per trade, 75% hit rate (2.5% spread haircut) |
| CDS, signal > 0, Schedule B | +32.0% per trade, 61% hit rate |

Real-money trading is considered only if PCS for at least one variant has a mean > 0, and the paper fills' realised spread cost stays within the calibration above. Six trades cannot prove anything statistically. The gate is there to catch a clear failure (consistent losses, or fills much worse than modelled), not to confirm success.
