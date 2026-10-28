# Options Recommendation Agent — Brainstorm & Plan

Draft, 28 Oct 2026. Status: **plan only, nothing built.**

## 0. Decisions taken (28 Oct 2026)

| Decision | Choice |
|---|---|
| Underlying | **MCX Ltd stock options only** (NSE F&O). Commodity and index options are out of scope. |
| Users / execution | **Tusk's own book; a person places every order.** No API order placement. |
| Risk structures | **Defined-risk only**: long calls/puts, debit and credit spreads, iron condors. No naked short options. |
| Horizon | **Positional, 2–4 weeks (current/next monthly expiry) is the product.** Intraday was chosen first, then dropped as the primary because the test below found no intraday edge. Intraday is kept as a *capability build*: data collection plus a research harness, retested later. |

### Edge test (28 Oct 2026): where is our edge?

Script: `scripts/options_edge_test.py`. Stock: MCX.NS (yfinance hourly and daily). Signals come from `mcx_snapshots` (23 Feb–28 Oct 2026) and `mcx_daily_revenue` (Jan 2022–Oct 2026). No look-ahead: an intraday snapshot must be captured at least 5 minutes before entry, and the daily signal enters at the **next** session's open. IC is the Spearman rank correlation. Multi-day horizons use non-overlapping samples (mean IC over the h offsets; t is approximate).

**Intraday: no edge.** Signal: revenue pace (cumulative revenue so far vs the trailing 20-day median at the same minute). Return: from the bar open at T to the 15:30 close.

| Decision time | n days | IC → rest of day | p | IC after removing the stock's own morning move |
|---|---|---|---|---|
| 10:15 | 105 | −0.08 | 0.42 | −0.05 |
| 11:15 | 114 | +0.01 | 0.88 | +0.04 |
| 12:15 | 116 | +0.02 | 0.82 | +0.04 |
| 13:15 | 127 | +0.05 | 0.58 | +0.05 |
| 14:15 | 126 | +0.03 | 0.71 | +0.03 |

Every reading is indistinguishable from zero. With about 8 months of data, an edge as large as IC 0.15 would probably have shown up. A small edge can't be ruled out, but it would not survive single-stock option spreads.

**Last night's revenue surprise → next day: no edge either.** Full history, n = 1,192: IC to the overnight gap 0.02, to next open→close 0.03. Over the 5/10/20-day horizons IC is 0.08/0.09/0.06 with t ≤ 1.2.

**The slow revenue trend → next 1–4 weeks: this is the edge.** Signal: `adr_trend` = log(45-day average daily revenue ÷ the same figure 20 days earlier). This is the same 45-day ADR the house valuation uses.

| Horizon (from next open) | IC | approx t | IC range across non-overlapping subsamples |
|---|---|---|---|
| 5 days | 0.15 | 2.2 | 0.13 to 0.16 |
| 10 days | 0.19 | 2.1 | 0.14 to 0.23 |
| 20 days | 0.27 | 2.0 | 0.18 to 0.33 |

The IC rises with horizon and is positive in every subsample. That matches a monthly option's life. Caveats:
- t ≈ 2 is borderline, and 10 signal/horizon combinations were tried, so treat this as "promising, not proven".
- Over the last 8 months alone (34 non-overlapping observations) the 5-day IC is about 0. That's too small a sample to judge, but it matches the recent decay of the ensemble signal.
- MCX turnover is public, so others can see this trend too. Our advantage is having it computed every day and tied to the valuation model.

### Stress test (28 Oct 2026): conditional pass

See `Docs/ADR_Edge_StressTest_Results.md`. The walk-forward passes (out-of-sample 20-day IC 0.29, t 2.04; positive in 3 of 4 test years; 9/9 variants positive). The 2026 IC is about 0 for the primary variant but holds for the slower v60/40. Through real option prices and costs, the signal works as a **bullish filter**: e.g. a put credit spread 10 days before expiry, taken only when the trend is rising, made +22%/trade with a 75% hit rate vs +10.7% for always being bullish. **Bearish trades lost money** in almost every structure. No other MCX data series added a significant edge. **v1 = bullish-only recommendations; bearish signals are logged as shadow trades.** This supersedes the two-sided matrix in §1 for now.

### What this means for the build

1. **The product is a positional agent:** monthly-expiry, defined-risk structures driven by the revenue trend, plus results-day trades (H3).
   - Bullish trend, IV cheap → call debit spread or long call.
   - Bullish trend, IV rich → put credit spread. The bearish side mirrors this.
   - Recommendations update daily after 23:30, and the agent re-prices them intraday during NSE hours for entry timing.
2. **Our existing ensemble/momentum signals are a second input, not a substitute.** Phase 2 tests whether they add anything beyond `adr_trend`.
3. **Intraday stays as a capability.** The 1-minute option-chain collector is needed anyway for entry pricing. Running it from week 1 builds the intraday history we lack. The intraday hypotheses (commodity-vol spillover H4; option-chain features such as IV and OI changes) are retested once about 6 months of chain data exists. The intraday test is rerun on the same schedule as more snapshots accumulate.
4. **A person placing every order is no constraint at this horizon.** Daily recommendations can be executed calmly during the session.

### Hypotheses for Phase 2 (replace H1–H3 in §7)

- **P1, revenue trend:** `adr_trend` (and variants, pre-registered before testing) → 10–20-day returns, expressed as debit or credit spreads with real option prices and costs.
- **P2, signal stack:** does the ensemble/momentum score add information beyond P1?
- **H3, results day:** our quarterly estimate vs consensus vs the option-implied move.
- **Vol filter for all of them:** IV vs a HAR-RV forecast decides buy-outright vs spread, and debit vs credit.
- **Parked, intraday:** H2i (revenue pace, failed 28 Oct), H4 (commodity-vol spillover, untested). Retest once the collector has history.

---

## 1. The question the agent has to answer

"Should I buy or sell calls or puts right now?" is really two separate forecasts:

1. **Direction**: will the underlying go up, down or stay put over the option's life?
2. **Volatility**: is the option *cheap or expensive* compared with the movement we expect?

An option buyer needs both to come out right: the move has to be in the right direction *and* big enough to cover the premium and theta. That is why most retail buyers lose. SEBI's own F&O studies found roughly 9 in 10 individual traders lost money. So the agent's job is to find the trades where **our** forecast differs from what the market has priced in, and to say "no trade" the rest of the time. Most of the time, no trade is the right answer.

### The decision matrix

| Our direction view | Implied vol vs our forecast | Recommendation |
|---|---|---|
| Bullish | Cheap | Buy call / call debit spread |
| Bullish | Rich | Sell put / put credit spread |
| Bearish | Cheap | Buy put / put debit spread |
| Bearish | Rich | Sell call / call credit spread |
| Neutral | Rich | Short strangle / iron condor (defined risk) |
| Neutral, event ahead | Cheap | Long straddle / strangle |
| No edge on either | — | **No trade** |

Each cell turns into a concrete strike and expiry only after an expected-value check (§4).

---

## 2. What we already have (and how strong it really is)

| Asset | Where | Use for options | Honest strength |
|---|---|---|---|
| Daily MCX revenue nowcast (live, known 23:30) | relay → Supabase, `mcx_daily_revenue` | Direction on **MCX Ltd stock** | Real information edge, but slow-moving |
| Quarterly P&L projection vs actuals | `api/quarterly.py` | **Earnings event trades** on MCX Ltd: our surprise estimate vs the option-implied move | Probably our strongest edge; only 4 events a year |
| Momentum / ensemble / ECM signals | `mcx_model_signals`, `mcx_momentum_signals` | Direction filter | **Weak.** After the 29 Sep look-ahead fix: full-history IC 0.16, Sharpe 0.60; latest 60-day IC 0.06, Sharpe −0.3 |
| Commodity daily data, signals, OI participants | `mcx_commodity_*`, `mcx_oi_participants` | Features for **commodity options** (crude, natgas, gold, silver) | Untested for options |
| Dhan API client | `lib/dhan_client.py` | Already authenticated; the natural realtime chain source | Used for futures OI only so far |
| Black-76 pricer, IV solver, PDE solver | OneDrive `mcx-vercel/trading/core/` | Pricing/Greeks for commodity options | Built April; needs a reconciliation test |
| Short straddle / strangle backtests + engine | OneDrive `trading/strategies`, `trading/backtest` | Volatility-selling baseline | **No recorded results**: rerun with costs and the lag rule |
| MCX bhavcopy history (223 MB DuckDB) | OneDrive `trading/data/mcx_bhavcopy.duckdb` | Historical options EOD for commodity options | Contents unaudited |

Conclusion: our edge on **direction** is modest and has decayed recently. Buying options on direction alone would likely lose to theta. Two places look more promising:
- **Volatility risk premium (VRP)**: implied vol tends to sit above the volatility that follows. Selling it with defined risk is the most widely documented options edge.
- **Event / information edge**: MCX Ltd results, where our revenue tracking gives a quarter estimate before the market has it.

---

## 3. Models worth using (evidence-based, not hype)

| Component | Model | Why |
|---|---|---|
| Option pricing | Black-76 (commodity futures options), Black-Scholes-Merton with dividends (stock options) | Standard; Black-76 is already built |
| IV surface | Per-expiry smile fit (SVI or a simple quadratic in log-moneyness) | Gives clean skew and term structure; avoids noise from stale quotes |
| Realised-vol forecast | **HAR-RV** (Corsi 2009) as the baseline, GARCH(1,1) as a cross-check | HAR is simple, robust and hard to beat; the core input to "IV rich or cheap?" |
| VRP signal | IV − forecast RV, expressed as a z-score vs its own history | Documented across markets (Coval-Shumway, Bakshi-Kapadia, Carr-Wu) |
| Event move | Implied earnings move (straddle price / spot) vs historical realised result-day moves, vs our surprise estimate | Turns our quarterly model into an options view |
| Direction | Existing ensemble + revenue nowcast, used as a *filter*, not a trigger | Too weak to stand alone (see §2) |
| Positioning | OI build-up, PCR, participant OI | Weak evidence in the literature: use as features, keep only if they survive the backtest |
| Sizing | Fractional Kelly, capped by max loss per trade and per month | Stops one tail event from ending the book |

Machine learning (gradient boosting on chain features) comes **later**, and only if the simple models leave measurable edge on the table. With monthly expiries the sample sizes are small, and ML would overfit.

---

## 4. The expected-value gate (every recommendation must pass)

For each candidate trade the engine computes, deterministically:
- Fill at **bid/ask, not mid**, plus slippage
- All costs: brokerage, STT/CTT, exchange and SEBI fees, GST, stamp duty (verify current rates at build time)
- Probability of profit and expected P&L under **our** vol and direction forecast, not the market's
- Max loss, margin required, return on margin
- Greeks (delta, gamma, vega, theta) and the portfolio Greeks after adding the trade
- Liquidity check: minimum OI and volume, maximum spread as % of premium

It recommends a trade only if expected P&L after costs > 0 with a margin of safety **and** all risk limits pass. Otherwise it outputs "no trade" and the reason.

**The LLM never computes a number.** Claude explains, cross-checks, flags anomalies and writes the note. Every figure comes from tested Python.

---

## 5. Architecture

```
Dhan market feed (websocket) + option chain API
        │  every 1–5 min during session
        ▼
Collector daemon (local launchd, like mcx_relay — or a Mumbai VPS)
        │  snapshots: chain, IV, Greeks, futures, spot
        ▼
Supabase (live tables) + DuckDB (research history)
        │
        ├── Our signals: revenue nowcast, quarterly projection, momentum, commodity signals
        ├── Vol engine: IV surface, HAR-RV forecast, VRP z-score, event implied move
        ▼
Strategy selector (matrix §1) → EV gate (§4) → risk limits
        ▼
Recommendation log (immutable: timestamp, snapshot, inputs, expected P&L)
        ▼
Claude agent (Agent SDK / MCP tools) → explanation + sanity check
        ▼
Delivery: dashboard tab + push alert (Telegram / Teams / email)
        ▼
Grader: marks every recommendation to market daily and at exit → calibration report
```

Notes:
- **Realtime feeds need a long-lived process.** Vercel functions are request-based and the Hobby plan has a 12-function cap, so the collector runs locally (like the relay) or on a small VPS. Vercel only serves the dashboard.
- MCX's commodity session runs to 23:30 (23:55 in winter), so a Mac-hosted collector must stay awake late. A VPS is more reliable.
- The **recommendation log is the learning loop**. Every call is graded against what actually happened, and a strategy is switched off automatically if its live hit rate or P&L falls outside its backtest band.

---

## 6. Validation rules (inherited from what we learned)

- **Entry lag:** a signal that uses MCX revenue (known 23:30) can first be traded at the **next** session. No same-day entry in any backtest.
- Rolling windows use only data known by their end date.
- Walk-forward with out-of-sample periods; report confidence intervals, not just point Sharpe.
- Fill at the bid/ask; include all costs.
- Sample-size honesty: about 12 monthly expiries and 4 results events a year. Use daily P&L of delta-hedged positions to get more observations, and say plainly when a result is not significant.
- Paper-trade at least 2–3 expiry cycles before any real money.

---

## 7. Phased plan

| Phase | What | Exit criterion |
|---|---|---|
| **0. Decide & audit** (≈1 wk) | Answer §10; confirm the Dhan data plan covers the MCX Ltd option chain and websocket; check how far back relay snapshots (`mcx_snapshots`) go, since they are the intraday nowcast history | We know which intraday history exists |
| **1. Data foundation** (≈2 wks) | **Start the collector in week 1** (entry pricing now, intraday research history later): MCX Ltd stock and the full option chain every 1 min, 09:15–15:30 → DuckDB/Supabase. Fetch NSE F&O bhavcopy (EOD) for MCX Ltd options history. Our own IV/Greeks (BSM with dividends) reconciled against Dhan's | 2 weeks of clean snapshots; IV agrees with the vendor within tolerance |
| **2. Research** (≈3–4 wks) | Test **P1, P2, H3** (§0) separately on daily stock history since 2022, then price the actual spreads with NSE F&O bhavcopy option prices and costs to check the edge survives | Each hypothesis passes or fails the gate in §6. Only survivors go forward |
| **3. Engine + agent** (≈2 wks) | Strategy selector, EV gate, risk limits, recommendation log, Claude explanation layer, dashboard tab, alerts | End-to-end recommendation in paper mode |
| **4. Paper trading** (2–3 expiry cycles) | Live recommendations, no money; daily grading | Live results inside the backtest band |
| **5. Live, small, human-in-the-loop** | Human places every order; size scales only with evidence | Monthly review; automatic kill switch |

Automated order placement is **out of scope** until Phase 5 is proven. SEBI's retail algo framework adds registration and static-IP requirements for API orders; recommend-then-human-executes avoids that entirely.

---

## 8. Resources & connectors needed

| Need | Option | Status |
|---|---|---|
| Realtime option chain + ticks (NSE + MCX) | **Dhan Data API** (we already use Dhan) | Confirm the subscription covers options data and websocket |
| Backup / alternative feed | Zerodha Kite Connect, TrueData, Global Datafeeds | Only if Dhan is insufficient |
| Historical intraday options | TrueData / Global Datafeeds (paid) | Needed **only** for intraday strategies; EOD history is free |
| Historical EOD options | NSE F&O bhavcopy (free), MCX bhavcopy (have) | Have MCX; NSE to fetch |
| Consensus estimates for MCX Ltd results | Broker notes in the Tusk knowledge base; Daloopa/FactSet connectors | Daloopa/FactSet plugins need authorisation via `/mcp` |
| Event calendar | EIA inventories, FOMC, US CPI, MCX results date | Free sources; small scraper |
| Always-on compute | Mac launchd (free, fragile) vs Mumbai VPS (≈₹1–2k/month) | Decision needed |
| Alerts | Telegram bot (simplest), Teams, email | Decision needed |
| Execution (later) | Dhan order API (needs static IP under SEBI rules) | Out of scope for now |

The IBKR connector in this environment covers US and global markets (CME/COMEX options). It matters only if we want to compare against, or hedge with, offshore gold, silver or crude options.

---

## 9. Risks

- **Selling volatility has fat left tails.** One gap move can wipe out months of premium. The Motilal Oswal desk example (Vikas Motwani meeting, 6 Aug 2026: a gap open of 800–1,200 points, IVs that used to sit at 60–80; transcript figures, unverified) is exactly this risk. Use defined-risk structures only until proven otherwise.
- **Liquidity:** MCX Ltd stock options and the smaller commodity options (gold, silver, metals) can have wide spreads. Crude and natgas options are the deepest MCX contracts.
- **Overfitting:** small samples. Keep the model simple and pre-register the hypotheses (§7 Phase 2).
- **Regulatory:** if recommendations go to anyone outside Tusk's own book, SEBI Research Analyst / PMS rules may apply. Confirm with compliance.

---

## 10. Still open

1. **Data:** is the Dhan Data API (option chain + websocket) active on our account? Paid historical intraday options data is **no longer needed** for the positional product; EOD bhavcopy is enough. Buy it only if we want to speed up the intraday retest.
2. **Sizing:** capital allocated to this book, max loss per trade, max loss per day/month.
3. **Success metric:** what counts as alpha (target Sharpe after costs, max drawdown, benchmark such as holding MCX Ltd stock)?
4. **Alerts:** Telegram, Teams or email? How many alerts a day is too many?
5. **Existing position:** does Tusk hold MCX Ltd stock? If so, the agent should also suggest hedges and overlays (collars, covered calls), which may be a better first product than standalone trades.
6. **Consensus source for results-day trades (H3):** broker notes in the knowledge base, or authorise Daloopa/FactSet?

Note: §2–§3 and §8 still describe commodity options and the April `trading/` module. Those are now reference only. Black-76 is replaced by Black-Scholes-Merton with dividends for stock options; the IV solver and backtest engine can be reused.
