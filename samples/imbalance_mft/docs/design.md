# MA crossover on pooled BTC/ETH flow → long/short basket of other coins — design

**Date:** 2026-10-12. **Status:** approved by we in chat (design presented 2026-10-12, "Okay, looks right. Proceed with implementation.").
**Builds on:** the trade and order flow dataset `tfl4w2-20261009` (`docs/superpowers/specs/2026-10-09-crypto-trade-order-flow-design.md`) and the cross-coin
flow model (`2026-10-10-cross-coin-flow-model-design.md`, whose 298 target coins this study reuses).

## Question

Does a simple moving-average crossover rule on BTC/ETH order flow, pooled across every venue and every quote, make money when it trades a basket of other
coins? Our specific curiosity: do imbalance MAs on information (dollar) bars beat the classic price MA crossover, and does the clock (dollar bars vs
time bars) matter?

## Decisions (2026-10-12)

| Topic | Decision |
|---|---|
| Signal source | BTC perp `ALL` and ETH perp `ALL` series of the dataset (every pair of every venue pooled = multiple venues and multiple quotes). |
| What is averaged | **Rule:** trade imbalance and, separately, book imbalance. **Benchmark:** price (vwap). |
| Clocks | $100k dollar bars (information bars, the dataset's `mas/usd100000`) and 1 s time bars (`mas/sec1`) — a 2 × 2 of {imbalance, price} × {dollar, time}. |
| Windows | The dataset's 14 MA windows 3, 5, 10, 20, 30, 40, 50, 75, 100, 200, 500, 1000, 5000, 10000 bars; every fast < slow pair = 91 pairs. |
| Rule | State = sign(fast MA − slow MA) at each bar close; a tie or a null keeps the previous state. Fast crossing above slow → long; below → short. |
| BTC vs ETH | Basket direction = the common sign when BTC and ETH agree; **hold the previous direction while they disagree**. Always invested after the first agreement. BTC-alone and ETH-alone kept as reference variants. |
| Decision clock | 1 s grid: the state at t is the last bar closed at or before t (no look-ahead). |
| Basket | The flow model's 298 target coins (no BTC/ETH, no tokenised equities), perp if present else spot, pooled `ALL` 1 s bars, **equal weight**; the basket return each second is the equal-weight mean of the coins' 1 s log mid returns (equal weight rebalanced continuously — accepted this over rebalance-on-flip). |
| Execution | Marked at mid. Lag 0 s (upper bound) and lag 1 s (executable, the convention of the earlier latency studies). |
| Costs | None — every number gross. Turnover (flips per day) and the break-even cost per flip (bp) are reported so costs can be judged. |
| Period | Sept 1–28 2026 (28 days), one pass, no fitting. Split-half honesty check: best rule per arm chosen on Sept 1–14, scored on Sept 15–28. |
| Null | Always-long basket; a random-flip null per rule (position series with the same number of flips at uniformly random times, 200 draws → p-value of the total return). |

## Rules

Arms `imb_usd`, `imb_sec` (imbalance types `timb`, `bimb`), `px_usd`, `px_sec` (type `px`). Rule id `{arm}:{type}:{fast}:{slow}`; 2·91 + 2·91 + 91 + 91 =
**546 rules**, × 3 combinations (both, btc, eth) × 2 lags.

Signal matrix per (source coin, date): int8 state of every rule on the 1 s grid τ_j = 00:00:00 + j s, j = 0..86399 (state as of τ_j). Price MAs are simple
moving averages of `vwap` over the previous N bars (polars rolling mean, min_samples = N) with the previous day's bars as warm-up, mirroring the dataset's
imbalance MAs; the imbalance MAs are read from the dataset.

Basket matrix per date: r_j = 1e4·ln(mid(τ_{j+1}) / mid(τ_j)) per coin, where mid(τ) = the `mid` of the 1 s bar whose bucket is τ − 1 s (the book mid at bar
close), forward-filled across seconds without a bar; null when either side has no mid yet. Basket return = mean over coins with a value; the first second of
each day is skipped (its previous mid is in the previous day's file).

PnL over interval j: pos_j · r_j with pos_j = direction(τ_{j − lag}); the direction carries across midnight (days processed in order).

## Metrics (per rule × combination × lag)

total return (bp), daily mean and sd (bp), Sharpe (daily mean / sd, and × √365), share of positive days, max drawdown (bp, on the cumulative sum), flips per
day, median holding time (s), long share of time, hit rate per holding segment, break-even cost per flip (total bp / flips), null p-value (two-sided, 200
draws) and the null's 95th percentile of |total|. Per arm × type: distribution over the 91 pairs (median / best Sharpe, share > 0, share with p < 0.05).
Per-coin PnL contribution for each arm's best rule. Daily PnL table for the best rule per arm. Always-long basket as the baseline row.

## Architecture (everything after `plan` runs on Batch; the sample's CPU image, dispatch word `macx`)

- `scripts/ma_cross_core.py` — pure functions: `rule_columns`, `price_mas`, `states`, `sample_states`, `combine`, `shift_positions`, `coin_returns`,
  `basket`, `segments`/`metrics`, `random_flip_null`, `drawdown`.
- `scripts/ma_cross.py` — controller (`plan`, `run`, `sig`, `bask`, `pnl`, `status`), markers in S3 under
  `s3://<data-bucket>/<dataset-prefix>/<run>/macx/`:
  - `sig` element `[coin, date]` (2 × 28 = 56): reads the coin-day (and previous day) `mas/usd100000`, `mas/sec1`, `bars/100000`, `tbars/1` `ALL` rows →
    `sig/{coin}/{date}.parquet` (t + 546 int8), `sig_done/{coin}_{date}.json`. Tier 8 vCPU / 32 GB.
  - `bask` element `[date]` (28): reads the 298 coins' `tbars/1` `ALL` mids → `basket/{date}.parquet` (t, r_bp, n_coins) and `basket_coins/{date}.parquet`
    (t + one float32 column per coin), `bask_done/{date}.json`. Tier 8 vCPU / 32 GB, 16 reader threads.
  - `pnl` (1 job, 16 vCPU / 64 GB): loads everything in date order, builds directions, PnL, metrics, null, split-half, per-coin contributions →
    `summary/{rules,daily,arms,split,coins,baseline}.parquet`, `summary/_done.json`.
  - Controller `run` loops sig → bask → pnl with the usual rounds / memory doubling; `plan --coins --dates` narrows (smoke); `status --watch`.
- `scripts/ma_cross_report.py` (laptop, reads `summary/` from S3): PDF + sortable HTML twin, clocks ET: 1 setup + reading, 2 arm summary, 3 top rules,
  4 fast × slow Sharpe tables per arm × type, 5 split-half, 6 per-coin contributions, 7 daily PnL of the arm winners, 8 BTC-alone / ETH-alone / both, 9 reproduction.
- Tests on Batch via `scripts/batch_test.py` (`tests/test_ma_cross_core.py`, `tests/test_ma_cross.py`, `tests/test_ma_cross_report.py`).

Run ids: `macxA-20260903` (smoke: Sept 1–3, basket SOL/XRP/DOGE), `macx4w-20261012` (full).

## Verification

1. Unit tests: states never use a bar with t_close > t; ties keep the previous state; combine holds on disagreement and carries across days; lag shifts by
   whole seconds; coin returns use the bar before the interval; flips / holding / hit-rate arithmetic on a hand-built series; null has mean ≈ 0 and the right
   flip count; price MAs equal a hand rolling mean with warm-up.
2. Smoke run: markers complete, rules × lags rows = 546 × 3 × 2, always-long total equals the sum of the basket series, a rule's total equals Σ pos·r recomputed in the test job.
3. Full run, report, review with we before any trading interpretation; all numbers gross.

## Non-goals

Costs, position sizing, volatility targeting, other MA types (EMA), windows outside the dataset's 14, per-coin rules, live trading.

## Smoke record (macxA-20260903: Sept 1–3, basket SOL / XRP / DOGE; 2026-10-12)

1. **Pipeline:** sig 6/6 (BTC $100k ALL day ≈ 500k bars, 1 s ≈ 173k; every arm decided by day end), bask 3/3, pnl in 60–106 s. A rule's reported total equals an
   independent recomputation (Σ pos·r, lag 1) to 4 decimals; 3 of 259,200 basket seconds null (the first second of each day).
2. **Bug fixed:** polars inferred the rules table's schema from its first 100 rows (all lag-0, p_value null) and rejected the later floats → every table is built
   with `infer_schema_length=None` (`_df`), regression tests added.
3. **Finding — the pooled ALL mid fakes a lead-lag.** On the pooled mid the rules looked absurdly good (imb_usd:timb:50:200: +2,514 bp in 3 days, 5,173 flips,
   p = 0.0006; 95 % of pairs positive). The lag profile decays smoothly (lag 0 → 5,132, 1 s → 2,514, 3 s → 1,281, 5 s → 232, ≥ 10 s ≈ 0): a 1–3 s lead-lag, not
   a look-ahead. Marking SOL at each venue's OWN mid showed the cause: Binance futures (median quote age 23 ms, 90 % of seconds with a return) keeps only a third
   of the lag-1 PnL (834 vs 2,891 bp) and none by lag 3; venues with sparse quotes (Hyperliquid, Kraken, Lighter: ages 2.6 s, returns in 6–13 % of seconds)
   show PnL that does NOT decay with the lag — stale quotes catching up to BTC. The pooled ALL mid is a USD-weighted average over pairs whose quotes may be up
   to 10 s old (`BOOK_STALE_MS`), median oldest age 2.7 s on SOL, so it inherits the artefact.
   **Ruling:** keep the approved pooled mark AND add a second mark, each coin's **primary venue** (largest traded notional in the day's 1 s bars) marked at its
   own mid; every table carries `mark`; the report shows both, pooled first (approved) and primary as the fair mark. Added the basket's mean quoted spread so
   the break-even cost per flip has a yardstick (half-spread ≈ 0.53 bp on the primary mark, 0.66 pooled, for these three coins).
   On the primary mark the same rule falls to +828 bp (p = 0.12), break-even 0.16 bp per flip — below the half-spread.
4. Sharpe ratios on 3 days are selection noise (best per arm 200–5,800); meaningful only on the 28-day run.

## Full run record (macx4w-20261012: Sept 1–28, 298 coins; 2026-10-12)

1. sig 56/56 (≈ 13 s each), bask 28/28 (≈ 14 s each, 298/298 coins every day; primary venue = Binance futures for 274 coins, OKX 9, Bybit 5, Binance spot 4,
   others 6), pnl 1,578 s for two marks (13 min per mark: 3,276 metric passes + 1,638 FFT nulls over 2.42 M seconds).
2. **Finding — the primary-venue mark is still stale on thin coins.** Pooled mark: median annualised Sharpe 8–13 per imbalance arm; primary mark: 38–43, 99 %
   of pairs positive and p < 0.05, every day positive for the top rules, break-even 0.2–0.7 bp per flip against a 1.8 bp half-spread. Diagnosis: 1 s bars
   exist only in seconds with a trade; 220 of 298 coins trade in under 30 % of seconds (median 15 %), so their forward-filled mid is stale for seconds at a
   time and catches up to BTC later. The best rule's per-coin contribution falls monotonically with the coin's share of fresh seconds (107 bp per coin in
   the sparsest decile → 6–22 bp in the freshest; corr −0.42). Same artefact as the sparse venues in the smoke, one level down.
   **Ruling:** third mark `fresh` = primary venue, a coin's return counts only between two consecutive seconds that BOTH have a bar (no forward fill); the
   report presents all three marks and names `fresh` the fair one. Added a total-PnL lag profile (0, 1, 2, 3, 5, 10, 30, 60, 300 s) per rule so the decay is
   visible. The pooled quoted spread averages 18 bp vs 3.9 bp on the primary venues — another symptom of stale cross-venue quotes. Basket and pnl stages rerun
   (signals unchanged); results below once complete.
3. **Rerun with three marks (finished 10:45 ET).** Fresh mark: always-long +20,839 bp over 28 days (744 bp a day, Sharpe 21) against +1,690 bp forward-filled
   on the same mids — returns counted only between consecutive trade-seconds of thin coins carry an upward drift inside trade bursts and drop the offsetting
   gaps, so the fresh mark is biased long and its rule results are discarded. On ZEC (liquid) the fresh sum matches the full path, so the bias is a thin-coin
   effect. Pooled mark: basket return sd 22.7 bp/s (0.47 on the primary venues), 19,653 seconds beyond 50 bp, lag-1 autocorrelation −0.09 — crossed / stale
   cross-venue quotes snapping back; unusable at 1 s. **The primary-venue mark is the only readable one**, with the caveat that forward-filled thin-coin
   mids still overstate levels. User review 2026-10-12 (chat): always-long vs rules, long / short legs split (short leg Sharpe 29–41 vs always-short −4 on the
   primary mark: the edge is timing, not beta), random-half and day-shifted benchmarks, price vs imbalance (imbalance median Sharpe 38–43 vs price 23–34,
   book > trade; information bars help the price MA, not the imbalance MA; price MA decays slower with lag and earns 1.3 bp per flip vs 0.6–0.8).
4. **Viewer (request: viewers that work directly with S3 data):** scripts/ma_cross_viewer.py, local server :8781, Polars from the summary parquet on
   request, pivot of any metric (incl. hit ratio = share of holding segments with PnL > 0) over any two of fast / slow / arm / type / combo / lag / mark.
5. **Ruling (request 2026-10-12): BTC and ETH themselves as traded targets** — marks `btc` / `eth` (the source coin on its primary venue), so the
   combos give own-flow (BTC flow → BTC) and cross-flow (ETH flow → BTC) rules next to the alt basket. Basket + pnl stages rerun (five marks).
6. **Five-mark rerun done 14:33 ET (pnl 4,036 s; 16,380 rule rows).** BTC itself (Binance futures, half-spread 0.03 bp): always-long +595 bp (Sharpe 1.8);
   BTC flow → BTC imbalance arms median Sharpe 42–59 (best 86, imb_sec:bimb:10:20: 36k bp, 6,756 flips/day, hold 12 s, break-even 0.19 bp); ETH flow → BTC
   39–53; agreement 34–42 (the hold rule costs). ETH itself (half-spread 0.05 bp): always-long +854 bp (Sharpe 2.5); ETH flow → ETH 24–41 (best 104,
   imb_sec:bimb:3:75: 71k bp, 9,652 flips/day, hold 5 s, break-even 0.26 bp); BTC flow → ETH 22–26. Price MAs on BTC / ETH are dead: time-bar price MA median
   Sharpe −1 to −2 (47 % / 38 % positive, negative at every lag), dollar-bar price MA 3–11. Hit ratio 0.46–0.51 for imbalance, 0.34–0.40 for price. Decay:
   own-flow edge halves by 1 s, gone by 10 s, negative beyond 30 s. The BTC / ETH targets have no stale-mid artefact (a bar every second on Binance), so these are
   clean measurements of book-imbalance → next-second mid predictability; break-even 0.1–0.3 bp per flip clears the half-spread but not a taker fee.
   Report re-rendered (ma_crossover_basket_2026-09.pdf), viewer restarted on :8781 with the five marks.
7. **Study viewer + coin x exchange cube (request 2026-10-12, modelled on the :8777 /study10y viewer).** bask writes `venue_coins/{date}.parquet` (1,523
   coin|venue series, each coin at every venue it trades on, forward-filled mid); pnl adds `hourly` (equity curves), `hist` (per-trade PnL bins), `venues`
   (full metrics of every rule x combo at lag 1 on each venue's own equal-weight basket; 12 venues) and `cube_index` + `cube/{combo}/{rule}.parquet` (daily PnL
   per coin|venue series). Marks and venues evaluated in threads (forked processes deadlocked on the Polars / BLAS pools; a 50-minute hang on the first
   attempt); 2,080 s on 32 vCPU. Viewer `/` = study page (cell selectors, equity / drawdown / per-trade histogram / lag profile, neighbour tables per
   dimension, exchanges table, coin x exchange table with a coverage filter); `/pivot` = the pivot page. **Reading caveat:** per-venue and per-coin-per-venue
   PnL uses each venue's forward-filled mid, so a venue that "works" better than Binance is usually a venue whose quotes are stale (ZEC on aster_futures:
   +73k bp / Sharpe 41 vs +2.3k bp / Sharpe 2.4 on binance_futures for the same rule); the coverage filter and the comparison against the coin's primary venue are
   the guard. Deploy lesson: pin the image tag from the build output, not HEAD (another session's commit landed between build and deploy and the controller
   was submitted against a non-existent tag).
8. **Ruling (2026-10-12): long windows 25,000 / 50,000 / 100,000 / 250,000 / 500,000 bars added** → 19 windows, 171 pairs, 1,026 rules. The dataset's MA
   files stop at 10,000, so the sig stage computes the missing imbalance MAs itself (`ma_cross_core.imb_mas`, min_samples = N) over a 7-day warm-up read
   (`WARMUP_DAYS`); the price MAs already were. 500,000 bars ≈ 1 day of BTC $100k bars but 5.8 days of 1 s bars, and the dataset starts Sept 1, so the longest
   time-bar rules are undecided (no position) for the first days. Baskets reused; signals + pnl rerun.
9. **Long-window rerun done 19:52 ET (pnl 3,490 s; 30,780 rule rows).** A null imbalance inside a window voided the whole rolling mean (18 of 342 imbalance
   pairs undecided on Sept 15 = every pair with slow = 500,000) → nulls count as neutral 0 (`imb_mas`), all pairs decided. Primary mark, agreement, lag 1:
   median Sharpe by slow window falls monotonically past 40 bars — 42 (30–40) → 23 (10,000) → 18 (25k) → 12.5 (50k) → 7 (100k) → 3.2 (250k) → 2.2 (500k); flips/day
   4,800 → 8, median hold 12 s → 36 min, hit ratio 0.51 → 0.44; break-even per flip peaks at ~1.1–1.25 bp (5k–100k slow) and never clears the 1.8 bp half-spread at
   the median. Pairs with BOTH windows long (fast ≥ 25k) are flat to negative on every arm (median Sharpe −3 to +1.6, 1–5 flips/day). The best long-slow rules are
   fast 3–20 bars against slow 25k–50k on 1 s bars (Sharpe 43–46, i.e. a fast imbalance pulse against a slow baseline), still 1,600–5,700 flips/day. Price MAs with
   long windows are dead (median Sharpe −1 to +0.3, hit 0.28). BTC itself with long windows: imbalance 9–24, price negative. The edge is a seconds-scale effect;
   lengthening the windows only dilutes it.
10. **Per-trade and hour-of-day statistics (request 2026-10-12; pnl rerun done 21:50 ET).** Metrics gain `mean_trade_bp` / `median_trade_bp`; new table
    `hod` (per rule x combo x mark, lag 1, US Eastern hour via zoneinfo — EDT in September): hourly PnL mean / share positive, and trades, hit ratio, mean and
    median PnL per trade by the hour a trade opened. Findings (primary mark, agreement, lag 1): the median trade is ~0 (−0.1 to +0.01 bp for imbalance rules,
    −4 to −5 bp for price rules with hit 0.33–0.35): the PnL is a thin positive mean on a symmetric distribution, i.e. a few large winners. Hour of day: the edge
    is largest 09:00–12:00 ET (median rule 67–82 bp per clock hour, 82–86 % of hours positive, 45 trades/hour) — the US morning when BTC flow is heaviest —
    and smallest 07:00 and 19:00 ET (14–17 bp); it is positive in every hour, so it is not a session effect. BTC-own best rule: 38–65 bp every hour, flat
    across the clock. Viewer: hour-of-day section (two charts + table); report §9.
