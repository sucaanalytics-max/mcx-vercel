# MCX Ltd Q2 FY27 Results: Options Study (H3)

Run 28 Oct 2026, ~12:15 IST, following `Docs/Results_Event_Study_Preregistration.md` (4 deviations logged there). Code: `scripts/results_event_study.py`.

## Verdict

**Direction: neutral.** We have no edge on the earnings number. Our model under-projects every quarter, and consensus has been about twice as accurate. **Volatility: rich.** The options are pricing a results-day move of about 5–5.5%, against a typical realised move of 3.9%. The pre-registered rule therefore maps to an **iron butterfly**, but the historical case for selling results volatility is thin. Treat it as a **paper trade only**, and re-measure on the day before results.

| Input | Value |
|---|---|
| Results date | **3 Nov 2026 per Yahoo, not confirmed by any MCX filing.** Last year's Q2 was 6 Nov. |
| Consensus EPS (Yahoo, analyst count unknown) | ₹18.70 (≈ ₹476 Cr PAT) |
| Our model EPS | ₹17.77 (PAT ₹452 Cr; daily F&O fee sum ₹741 Cr over 66 days) → gap −5.0% |
| Our model after its recent bias (last 3 quarters −8%) | ≈ ₹19.3 (post-hoc; not part of the rule) |
| Implied reported revenue (fee sum ÷ 0.915) | ≈ ₹810 Cr (Q1 FY27: ₹702 Cr) |
| Live implied results-day move (Dhan, ATM IV 38.2%, Nov expiry) | **5.5%** (4.9–5.5% depending on the post-event IV assumption) |
| Median realised results-day move | 3.9% over all 19 events since 2022; 4.5% since 2024 |

## Test 1: projection accuracy (Q3 FY25 to Q1 FY27, n = 7)

| Quarter | Ours | Consensus | Actual | Our error | Consensus error |
|---|---|---|---|---|---|
| Q3 FY25 | 5.42 | 6.28 | 6.29 | −13.8% | −0.2% |
| Q4 FY25 | 4.58 | 6.50 | 5.30 | −13.6% | +22.6% |
| Q1 FY26 | 6.88 | 7.74 | 7.98 | −13.8% | −3.0% |
| Q2 FY26 | 6.99 | 7.86 | 7.74 | −9.7% | +1.6% |
| Q3 FY26 | 13.67 | 15.40 | 15.76 | −13.3% | −2.3% |
| Q4 FY26 | 19.56 | 21.40 | 20.82 | −6.1% | +2.8% |
| Q1 FY27 | 15.48 | 16.40 | 16.23 | −4.6% | +1.0% |

Mean absolute error: **ours 10.7%, consensus 4.8%.** Our error is a consistent bias (−10.7% on average), not noise. Cause: `api/quarterly.py` feeds the **daily F&O fee sum** into an expense model fitted on **reported revenue**. Fee sums are only 87–92% of reported revenue (the gap is other operating income), so revenue, and therefore PAT, are understated every quarter. The bias has narrowed (−6%, −5% in the last two quarters) as fees have grown. **This affects the Quarterly tab on the dashboard, not just this study.**

## Test 2: direction (n = 7)

Our gap vs consensus was negative in all 7 quarters, which is the bias again. Its sign matched the results-day return in **3 of 7 (43%)**. That is below the pre-registered 60% bar, so the direction view is **neutral**.

## Test 3: implied vs realised results-day moves (19 events since 2022; 17 with usable options)

- Median realised move 3.9%; median implied move 3.9%. Realised/implied: **median 0.87, mean 1.07**. Realised beat implied in 47% of events.
- So the market prices MCX results about right on average, with occasional large moves well above implied (Jan 2025 8.7%, Feb 2024 8.9%, Jul 2022 5.3%).
- **A long ATM straddle held from the day before results to the day after lost 3.8% per event on average** (n = 18, 32% hit rate), after a 2.5% half-spread on each leg plus costs. At live near-month spreads (about 0.4%) it is roughly breakeven. Neither side has shown a clear edge.
- Implied volatility falls after results to a median of 0.865 × its pre-results level (the post-results IV collapse).

## Q2 FY27 reading under the pre-registered rules

- Direction: gap −5.0% (not below −5%) and a 43% hit rate → **neutral**.
- Volatility: live implied 5.5% > 1.2 × 3.9% = 4.7%, and test 3's median ratio is 0.87 < 1 → **rich**. This holds for every post-event IV assumption tested (4.9–5.5%). Against the since-2024 median (4.5%) it is only borderline.
- **Map → neutral + rich = iron butterfly:** sell the ATM straddle, buy wings at ±7%, near-month, enter about 15:10 on the day before results, exit about 15:10 on the day after.

## Caveats

- **The date is unconfirmed.** If MCX files a different date, the trade dates move with it.
- **Today's 5.5% is measured 4 trading days early.** The rule calls for measuring on the day before results, and the reading can change.
- **The Dhan token expires 29 Oct, 09:44.** A fresh token is needed before the pre-day measurement.
- **Selling results volatility has a fat tail.** In 3 of 17 events the realised move was more than double the implied move. The iron butterfly caps the loss, but the historical edge for the seller is not established (mean realised/implied 1.07).
- **Small sample:** 7 quarters for direction, 17 events for volatility. Nothing here is statistically significant.
- Historical implied moves use bhavcopy closing prices: one event had a stale leg, and one (Oct 2024) had no usable chain.

## Follow-ups

1. **Fix the Quarterly tab's revenue bias.** Scale the fee sum up to reported revenue (rolling fee-sum/reported ratio, about 0.92 now), or model other operating income separately. Then rerun test 1.
2. **Confirm the results date** from MCX's board-meeting notice, refresh the Dhan token, and re-measure the implied move on the day before results. Paper-trade the iron butterfly only if the rule still says neutral + rich.
