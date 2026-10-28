# MCX Ltd Results-Day Options Study (H3): Pre-registration

Written 28 Oct 2026, 12:00 IST, **before** computing our Q2 FY27 projection, any historical projection error, or any implied-vs-realised move. Known at this point: Yahoo lists the next results date as **3 Nov 2026** (not confirmed by an MCX filing) and a consensus EPS of **₹18.70** (analyst count unknown). Code: `scripts/results_event_study.py`.

## Inputs

- **Our projection:** `api/quarterly.py` logic applied to the full quarter of daily revenue. Revenue is the sum of daily F&O revenue; expenses come from the OLS expense model; PAT = (revenue − expenses) × (1 − tax/depreciation rate). EPS = PAT ÷ 25.451 Cr diluted shares. For historical quarters the expense model and tax rate are fitted on **earlier quarters only** (walk-forward, at least 3 prior).
- **Consensus and actuals:** Yahoo `get_earnings_dates` EPS estimate and reported EPS (split-adjusted, consolidated basis; spot-checked against our PAT for Q4 FY26 and Q1 FY27).
- **Event timing:** MCX announces after market hours or on non-trading days. Pre-day = the last NSE trading day on or before the announcement date. Reaction day = the next NSE trading day after it.
- **Realised event move:** |close(reaction) ÷ close(pre) − 1| on MCX.NS (split-adjusted).
- **Implied event move:** NSE F&O bhavcopy, near-month MCX options, Black-76 on the near-month futures close (r = 6.5%). Use ATM IV (the call/put average at the strike nearest the futures price) on the pre-day and the reaction day, same expiry, provided the expiry falls after the reaction day. Event variance is σ_e² = IV_pre²·T_pre − IV_post²·T_post (T in years, trading-day basis /252). The implied absolute move is √(2/π)·σ_e. If σ_e² ≤ 0, the event is recorded as "no event premium".

## Historical tests (reported in full, whatever they show)

1. **Projection accuracy:** mean absolute % error of our EPS vs reported, compared with consensus vs reported. Quarters from Q1 FY25 on (the first with at least 3 prior actuals for the walk-forward fit).
2. **Direction:** hit rate of sign(our EPS − consensus) against sign(reaction-day return), over the same quarters.
3. **Volatility:** for every results event since 2022 with usable option data, the ratio of realised to implied event move. Report the median ratio, the share of events where realised > implied, and the average P&L of a long ATM straddle held pre-day → reaction day (bid/ask at a 2.5% half-spread plus costs).

## Decision rules for Q2 FY27 (fixed now)

Let gap = (our EPS − consensus EPS) ÷ consensus EPS.

- **Direction view:**
  - **bullish** if gap > +5% *and* test 2's hit rate is at least 60%.
  - **bearish** if gap < −5% with the same hit-rate condition. Bearish views are logged as shadow trades only, in line with the bull-only v1 rule.
  - **neutral** otherwise.
- **Volatility view:** compare the live implied event move (from the Dhan chain, using the same σ_e method, with the post-event IV taken as the historical median ratio IV_post ÷ IV_pre × the current IV) with the historical median realised event move.
  - **cheap** if implied < 0.8 × the median realised move, and test 3's median realised/implied ratio is > 1.
  - **rich** if implied > 1.2 × the median realised move, and test 3's median ratio is < 1.
  - **fair** otherwise.
- **Trade map** (defined-risk only, near-month, entered at about 15:10 on the pre-day, exited at about 15:10 on the reaction day):

| Direction \ Volatility | Cheap | Fair | Rich |
|---|---|---|---|
| Bullish | Long ATM call | Call debit spread (ATM / +5%) | Put credit spread (ATM / −5%) |
| Neutral | Long ATM straddle | No trade | Iron butterfly (short ATM straddle, wings ±7%) |
| Bearish | Shadow only | Shadow only | Shadow only |

## Deviations

1. **Test 1/2 sample.** "Quarters from Q1 FY25 on" was an arithmetic slip. `QUARTERLY_ACTUALS` starts at Q4 FY24, so the first quarter with 3 earlier actuals is **Q3 FY25**: 7 quarters (Q3 FY25 to Q1 FY27). Corrected before running anything.
2. **Intraday announcements.** If Yahoo's announcement time converted to IST falls before 15:30 on an NSE trading day, the announcement day itself is treated as the reaction day and the previous trading day as the pre-day. This is a refinement of the timing rule, added before seeing any event data.

3. **Reported EPS source (found after the run).** Yahoo's reported EPS for Q3 FY26 is 12.04, but our verified consolidated PAT of ₹401 Cr gives 15.76. Test 1 therefore uses `QUARTERLY_ACTUALS` PAT ÷ 25.451 Cr (multi-source verified) as the reported figure. Tests 2 and 3 don't use reported EPS and are unaffected.
4. **"Historical median realised event move" was ambiguous** (all events vs since 2024). The volatility view uses all events (n = 19), the same sample as test 3. The since-2024 median is reported alongside it.

Every recommendation is paper only. With about 10–18 historical events, none of these tests can be statistically significant. They decide whether there is *any* basis for a view, and otherwise the default is no trade.
