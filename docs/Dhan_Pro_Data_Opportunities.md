# Dhan Pro Traders data — what it unlocks for the MCX models

29 Oct 2026. The plan was upgraded to "Data APIs for Pro Traders" (₹799/month). This note maps the data to our models, records what a live probe found, and ranks what to build. Probe scripts are throwaway (session scratchpad); the results below are what matter.

## 1. Probe results (29 Oct, live token)

| What | Result |
|---|---|
| Account | `dataPlan: Active`, valid to **30 Oct 2027**. Segments E, D, C, M. Token valid 24 h (to 30 Oct 10:51) |
| **Pro APIs**: Technical Data, Market Movers, CAS Live Feed, News Headlines | **Not found.** Not in the public docs (search index of `dhanhq.co/docs/v2`), the release notes (to v2.5) or the latest SDK (`dhanhq` 2.3.0). Need endpoint docs from Dhan (apihelp@dhan.co) |
| **200-level depth** | **Works** for MCX Ltd stock (NSE_EQ 31181) and MCX Ltd options (NSE_FNO): 200 levels per side. NSE only per the docs, so no MCX commodity depth. Option packets can arrive with bid and ask joined in one message, so the parser must loop |
| **Historical OI, MCX futures** (`/charts/historical`, `oi: true`) | **Works back to 2020-01-01** for every commodity tested. See §2 |
| **MCX commodity option chain** | **Works** for GOLD, SILVER, CRUDEOIL, NATURALGAS: ~95–200 strikes, OI, previous-day OI and volume, bid/ask, Greeks. **Dhan's IVs are unreliable** (gold ATM call 12.7 vs put 53.8; a silver ATM at 198), so compute IV ourselves (Black-76) from mid prices |
| **Expired options** (`/charts/rollingoption`) | **Broken for us.** DH-905 on every request, including Dhan's own documented NIFTY example. Dhan's FAQ also says MCX commodity contracts aren't covered. Needs a Dhan support ticket |
| **Minute history, live option contracts** (`/charts/intraday`) | **Works.** MCX Ltd 23 Nov 3050 PE: 1-min bars with OI from first trade (16 Oct). Only contracts that are still live |

## 2. Commodity OI backfill — validated

- **`expiryCode` is ignored for MCX.** Near, next and far all return the same series.
- **Each contract ID returns a history stitched by expiry position.** The current front-month ID gives whichever contract was front on each past date; the second ID gives the then-second contract, and so on.
- **So the sum over all live contract IDs is a total-OI history back to 2020**, and it has no roll sawtooth: an expiring contract's OI moves into the next position.
- **Check against our only independent data**, the `dhan_live` snapshot of 27 May 2026:

  | Commodity | Live (stored) | Backfill | Diff |
  |---|---|---|---|
  | SILVER | 10,866 | 10,866 | 0.0% |
  | ZINC | 2,894 | 2,894 | 0.0% |
  | GOLD | 9,904 | 9,902 | 0.0% |
  | NATURALGAS | 20,348 | 20,335 | −0.1% |
  | CRUDEOIL | 15,265 | 15,244 | −0.1% |
  | SILVERM | 27,602 | 27,573 | −0.1% |
  | GOLDM | 67,461 | 65,465 | −3.0% |
  | COPPER | 21,960 | 21,082 | −4.0% |

  The 28 May "live" row equals 27 May exactly, which looks like a stale snapshot; compare against 27 May only.
- **Needs cleaning before use.**
  - There are one-day glitches, e.g. 9 Mar 2026 in every commodity (SILVER 7,030 → 1,702 → 6,936).
  - Monthly contracts double count on some roll days (CRUDEOIL 16,889 → 27,488 → 20,727 around 15 Oct).
  - Days with a move over 40% since 2020: SILVER 13, GOLD 32, CRUDEOIL 103 of ~1,666.
  - Fix with a revert filter: drop a day that moves more than 40% and reverses the next day.
  - The GOLDM and COPPER gaps (3–4%) are probably far contracts that have since expired and dropped out of the scrip master.

**Our current commodity OI factor is effectively empty.** `refresh_dhan_oi.py` wrote OI on only 27–28 May; it was never scheduled. Every row since has `open_interest = 0`, so the OI z-score in the commodity signals has had no input for five months.

## 3. Ideas, ranked (updated)

### P0 — Automate token renewal — **DONE 29 Oct**
`lib/dhan_client.py` now uses the Demat project's shared token file, `~/.dhan/credentials.json` (written by `~/Documents/Demat/dhan_auth.py`, renewed by its 09:10/15:35 launchd job). If the token has less than 1 h left, or Dhan returns 401, it mints a new one from PIN + TOTP and saves it atomically (0600). The `.env` token is only a fallback when the file is missing.
- **Minting a token revokes earlier PIN+TOTP tokens within minutes.** On 29 Oct, Demat's 08:31 token was still accepted right after two test mints, then started failing with `400 DH-906 Invalid Token`. A token made on Dhan Web survived. So all minting must go through the one shared file. A revoked token gives DH-906 (HTTP 400), not 401, so the check treats both as rejected.
- **Dhan allows one mint every 2 minutes.** If both projects renew at once, the loser re-reads the file and uses the token the other just wrote.
- **Demat's own `dhan_auth.py` only re-mints on 401**, so a revoked (DH-906) token leaves it stuck until its 23 h expiry note runs out. It needs the same one-line fix.
- Tested: TOTP matches `pyotp` at 50 time steps; the normal path doesn't touch the file; forced renewal on a scratch copy minted a token Dhan accepts; a corrupted token is detected (401) and replaced; the race fallback picks up the other project's token; `options_paper.py status` runs through it.

### P1 — Commodity OI history + a nightly OI job — **built 29 Oct; OI kept out of the composite**
`scripts/backfill_commodity_oi.py` (2022 → today, `--nightly` for the trailing 30 days) writes only `open_interest` / `oi_value_cr` onto existing FUTCOM rows. Dry run: 23,107 rows, 29 commodities, no fetch failures. Dhan has no futures history for COTTONCNDY, CPO and RUBBER, and about 23 days per commodity have no Dhan bar (e.g. 16–20 Nov 2023). Glitch days and those gaps are stored as 0, which the signals cron now treats as missing.

**Signal impact (measured before writing):** putting OI into the composite would flip 35% of commodity-day signals across history and 39% in the last 30 days, and 7 of 10 on 28 Oct (base metals NEUTRAL → SELL in their expiry week; crude and natural gas SELL → NEUTRAL). Part of that is an expiry-cycle effect. Decision (29 Oct): store and show the OI z-score, keep it out of the composite (`OI_IN_COMPOSITE = False` in `lib/cron_commodity_signals.py`) until tested. Composite and signals were verified identical with the data present. **Open test:** does OI z add information once the expiry cycle is removed, and should it use ₹ value rather than lots summed across contract sizes?

Original plan:
Backfill `mcx_commodity_daily.open_interest` from 2020 using the summed stitched series, with the revert filter. Replace the never-scheduled live-quote job with the same historical call run nightly, so past and future values come from one method. Then recompute the OI z-score and check whether it adds anything to the commodity signals; it has never been tested with real history.

### P1 — MCX Ltd options intraday history (research)
- **Blocked:** expired-options data (DH-905). File a ticket with the exact payload.
- **Workaround now:** every night, pull 1-min bars and OI for every live MCX Ltd option contract and store them. Each contract's history runs from listing to expiry, so history builds up from today without the expired endpoint.
- **Use:** price the results-day straddle and the bull put spreads at 15:10, not on bhavcopy closes, and add an IV-rank condition to the spread rule.

### P1 — Record depth at every paper entry (execution, ~½ day)
First reading, MCX 23 Nov 3050 PE (~5% OTM, near month), 12:40 IST:

| Size (lot 225) | Buy slippage vs mid | Sell slippage vs mid |
|---|---|---|
| 1 lot | 0.60% | 0.60% |
| 5 lots | 0.72% | 1.08% |
| 10 lots | 1.02% | 1.41% |
| 20 lots | 1.60% | 2.12% |

This is one snapshot, not a model. Add a 200-level snapshot of each leg at 15:10 to `options_paper.py` so the ledger records real fill cost by size. Against the 4–8% breakeven, near-month costs leave room up to ~10 lots; check that over several entries before sizing.

### P2 — Dhan as a second revenue relay (model and resilience, 3–5 days)
MCX option chains and quotes work. Pull every live MCX contract per snapshot for per-commodity futures turnover and options premium turnover, which closes the same-day `mcx_commodity_daily` gap and gives a fallback when MCX blocks the scraper. **Gate:** must reconcile with MCX's reported total within 1% on ≥10 days before it feeds any revenue number. Commodity PCR and OI change by strike come with it at no extra cost.

### P2 — Commodity volatility as a leading indicator of MCX revenue (research, 1–2 days)
Turnover follows volatility. Test now, without Dhan, using free CBOE GVZ/OVX against our ADR history. If it holds, collect our own Black-76 IV from the MCX option chain daily and add it to the projection drift and the options trend filter.

### P3 — Pro-only APIs: waiting on Dhan docs
| API | Possible use | Expected value |
|---|---|---|
| News Headlines | MCX Ltd event feed: results date, SEBI circulars, fee changes, outages | Medium, if it carries corporate announcements |
| Market Movers | OI build-up and volume-spike screens | Low |
| Technical Data | Vendor indicators; we compute our own | Low (skip) |
| CAS Live Feed | Consolidated holdings; matters only if Tusk holds MCX Ltd (Options plan §10.5) | Low unless that answer is yes |
| Company Fundamentals (coming soon) | Could feed `QUARTERLY_ACTUALS` if it carries quarterly P&L | Watch |

### What not to build
An intraday trading system on depth or tick data. The 28 Oct test found no intraday edge (revenue-pace IC ≈ 0). More detailed data doesn't change that without a new, pre-registered hypothesis.

## 4. Dhan support ticket (to send)
1. `/v2/charts/rollingoption` returns DH-905 for every payload, including the docs example (`NSE_FNO`, `securityId` 13, `OPTIDX`, `MONTH`, `expiryCode` 1, `ATM`, `CALL`). Is it enabled on our plan? Does it cover `OPTSTK` for MCX (underlying 31181)?
2. Endpoint documentation for the Pro Traders APIs: Technical Data, Market Movers, CAS Live Feed, News Headlines.

Sources: [DhanHQ v2 releases](https://dhanhq.co/docs/v2/releases/), [Full Market Depth](https://dhanhq.co/docs/v2/full-market-depth/), [Expired Options Data](https://dhanhq.co/docs/v2/expired-options-data/), [Dhan FAQ: expired options for MCX](https://dhan.co/support/platforms/dhanhq-api/is-expired-options-data-available-for-mcx-contracts-through-dhanhq-apis/), [madefortrade: OPTSTK 400 on rollingoption](https://dev.madefortrade.in/t/issue-with-fetching-historical-stock-options-data-via-api-http-400-error/57030).
