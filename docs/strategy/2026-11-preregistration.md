# Pre-registration: ADR-band strategy test (MCX, BSE)

Written 20 Nov 2026, before any result below was computed. Every change made after
this point goes in **Deviations** at the bottom, with the date and the reason.

## Thesis under test (CIO, VT Capital notes 13 and 17 Nov 2026)

- **H1 (band).** Regress the share price on the 45-day moving average of daily
  revenue (ADR):
  - when the price is about 2 SD below the line, it rises;
  - when it is about 2 SD above, it falls.
- **H2 (turning up).** If the 10-day ADR average is above the 45-day **and** the
  price is about 1 SD or more below the line, the stock turns up.
- **H3 (turning down).** If the 10-day ADR average is below the 45-day **and** the
  price is about 1 SD or more above the line, the stock turns down.
- **H4 (rule of thumb).** While the 10-day (or 15-day) average stays above the
  45-day, the stock doesn't fall.

## Data

- **MCX:**
  - `mcx_daily_revenue.total_rev_cr` (Muhurat sessions and zero days dropped),
    Jan 2022 →;
  - MCX.NS daily OHLC from yfinance (`auto_adjust=True`, split-adjusted).
- **BSE:**
  - `codex_bse_daily.total_rev` from the NSE-BSE project (snapshot 20 Nov 2026,
    `research/strategy/data/`), Apr 2023 →;
  - BSE.NS daily OHLC from yfinance.
  - Sample starts **1 Jan 2024**, because BSE's F&O revenue was near zero before
    then. The 45-day average and the fit warm up on 2023 data.
- **Revenue alignment.** Revenue for day d is known after that day's close. Every
  signal dated d is traded at the **next NSE open** after d.

## Signal construction (point in time)

- `ADR_w`: rolling w-day mean of daily revenue.
- **Regression:**
  - at each date t, fit OLS `close = a + b · ADR_45` on all pairs **up to and
    including t−1**;
  - expanding window, minimum 120 pairs (60 for BSE as a variant);
  - residual SD `s_t` comes from the same fit;
  - `z_t = (close_t − (a + b·ADR_45,t)) / s_t`.
- **Variant fits:**
  - rolling 250-day window instead of expanding;
  - log-log fit (`ln close` on `ln ADR`), because BSE revenue grew about 100× in
    the sample and a level fit is dominated by the trend.
- `ratio_t = ADR_10 / ADR_45`. The H4 variant uses ADR_15.

## Primary rules (the CIO's, as stated)

| Rule | Long when | Short when | Exit |
|---|---|---|---|
| R1 band | z ≤ −2 | z ≥ +2 | z crosses 0, or 60 trading days |
| R2 turning | ratio > 1 and z ≤ −1 | ratio < 1 and z ≥ +1 | the opposite condition, or 60 days |
| R4 trend | ratio > 1 | ratio < 1 | always in (flip) |

## Grid (secondary, reported in full, deflated Sharpe applied)

- ADR window: 30 / **45** / 60.
- Band: 1.5 / **2** / 2.5 SD.
- Turning band: **1** SD.
- Fit: **expanding** / rolling 250 / log-log.

That's 3 × 3 × 3 = 27 R1 variants, 9 R2 variants and 3 R4 variants. Primary
settings are in bold.

## Tests

1. **Cointegration.**
   - Engle-Granger: ADF on the full-sample residual of price on ADR_45, per
     stock, using MacKinnon critical values.
   - OU half-life: `−ln 2 / ln(1+β)` from `Δe_t = α + β e_{t−1}`.
   - **Pass:** ADF p < 0.05 and half-life < 60 trading days.
2. **Event study.** Forward returns at 5/10/20/60 trading days from the next open,
   after:
   - z ≤ −2 / ≥ +2;
   - z ≤ −1 / ≥ +1;
   - the H2/H3 states;
   - 10/45 crossovers.

   Only the first day of each episode counts (non-overlapping). Report n, mean,
   hit rate and t-stat against the unconditional mean.
3. **Trade backtest**, as stock futures, both directions:
   - **Cost per round trip:** 0.10% (STT 0.02% on the sell side, exchange and
     broker fees, 5 bp slippage each side). Add 0.05% per monthly roll while a
     position is held.
   - **Metrics:** CAGR, annualised Sharpe on daily P&L, max drawdown, hit rate,
     profit factor, number of trades, time in market.
   - **Benchmarks:** buy-and-hold; R4; and the BSE revenue z-score rule from the
     March 2026 brief (20-day z > 0.5 → long for 5 days).
4. **Split:**
   - **MCX:** train through 31 Mar 2025, test 1 Apr 2025 →.
   - **BSE:** train 2024, test 2025 →.
   - Results are reported on both halves and in full.
5. **Pooled panel:** MCX and BSE R1/R2 events pooled, with a t-stat on the pooled
   mean.
6. **Multiple testing:** deflated Sharpe (Bailey & López de Prado) over the 39
   variants.

## Pass criteria (per rule, decided now)

A rule **passes** if, on the test period and after costs, all of these hold:

- Sharpe > 0.5;
- mean trade return > 0 with t > 1.5 on the pooled panel;
- deflated-Sharpe probability > 0.8;
- the train and test halves have the same sign.

If it does none of these, it **fails**. Anything in between is **inconclusive**:
not traded, kept in the tracker as information.

## Known limits, stated up front

- **MCX revenue before Oct 2024** is computed at today's flat SEBI rate, not the
  fees MCX actually charged then. Every MCX result is also reported on a
  **Nov 2024 → sample** (the start of `REGRESSION_START` in `lib/house_model.py`).
- **Small samples:** 2-SD events are rare (expect fewer than 10 independent
  episodes per stock). The event study is the honest unit; the Sharpe on so few
  trades has wide error bars.
- **BSE stock futures** exist only from BSE's F&O inclusion date. Before it,
  short results are hypothetical.

## Deviations

1. **20 Nov 2026, re-entry after a time stop.** I wrote this after the first run
   and before reading any result.
   - In the first run, a position closed by the 60-day stop re-opened the same day
     whenever z was still beyond the band, so the stop never took effect. One MCX
     short was held 180 days and lost 99.8%.
   - **Clarified rule:** after an exit, a side re-arms only once z has come back
     inside its band (R1), or once the turning state has reset to 0 (R2).
   - An opposite state still flips R2 immediately.
2. **20 Nov 2026, evaluation starts.**
   - MCX trades are evaluated from **1 Jul 2022** (the 120-pair fit needs about
     six months of 2022 data).
   - The post-fee-change MCX sample fits from 1 Nov 2024 and evaluates from
     1 May 2025, with train through Dec 2025 and test 2026.
3. **20 Nov 2026, BSE minimum fit.** The BSE minimum-60-pair variant was not run.
   The 120-pair fit already starts before the Jan 2024 evaluation date.
