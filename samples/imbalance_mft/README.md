# imbalance_mft — moving-average crossover on pooled BTC/ETH order-flow imbalance, traded on a basket of other coins

A mid-frequency (seconds to minutes) trading study on crypto order flow, built to run on AWS Batch with the data read straight from S3. The question:
does a fast/slow moving-average crossover on **BTC and ETH order-flow imbalance**, pooled across every exchange and quote currency and measured on
**information bars** ($100k dollar bars) as well as on 1 s time bars, make money when it trades an equal-weight **basket of ~300 other coins** long or
short, against the classic crossover on **price** as the benchmark? Everything is gross and mid-to-mid, with the quoted half-spread reported next to every
rule as the cost yardstick. The design, each decision and each finding are in `docs/design.md`.

## What is here

| File | Role |
|---|---|
| `ma_cross_core.py` | Pure functions: 19 MA windows (3 … 500,000 bars) → 171 fast < slow pairs → 1,026 rules (4 arms x imbalance type); price MAs; crossover states (ties hold); as-of sampling on the 1 s grid (no look-ahead); coin / basket 1 s mid returns (forward-filled or fresh-to-fresh); BTC–ETH agreement with hold; execution lags; metrics (Sharpe, hit ratio, per-trade mean / median, drawdown, break-even per flip); circular-shift null; hour-of-day statistics |
| `ma_cross.py` | Batch controller: `plan` → `run` (controller job: stages `sig` → `bask` → `pnl`) → `status`; `sig` / `bask` / `pnl` are the Batch elements |
| `ma_cross_report.py` | PDF + HTML twin of one run (sortable tables), rendered on the laptop from the run's `summary/` parquet |
| `ma_cross_viewer.py` | Local server reading the summary parquet from S3: a study page (one cell: equity, drawdown, per-trade histogram, lag profile, hour of day, neighbour tables per dimension, exchanges, coin x exchange) and a pivot page over any two parameters |
| `common.py`, `report_common.py`, `settings.py`, `config.json` | S3 / Batch helpers, report helpers, and the single config file every account value comes from |
| `tests/` | Unit tests on synthetic frames (no S3, fake config via `tests/conftest.py`) |

## Data

The study reads a trade-and-order-flow dataset under `s3://<data_bucket>/<dataset_prefix>/<dataset_run>/flow/`:

- `mas/usd100000/{coin}/perp/{date}.parquet`, `mas/sec1/...`: per venue and pooled (`venue == "ALL"`) bars with `t_close`, `trade_imb`, `book_imb` and
  their simple moving averages `timb_ma{N}` / `bimb_ma{N}` for N in 3 … 10000 (longer windows are computed here);
- `bars/100000/{coin}/perp/{date}.parquet`, `tbars/1/{coin}/{kind}/{date}.parquet`: dollar and 1 s bars with `venue`, `t_bucket`, `t_close`, `vwap`, `close`,
  `mid`, `spread_bp`, `usd`;
- `ref/universe.parquet` (coin, kind, venue, coin_adv_usd, in_universe) and `plan.json` (dates).

Trade imbalance = (buyer-initiated − seller-initiated dollar volume) / total over a bar; book imbalance = (bid − ask dollars at the touch) / total at the
bar close. The basket is the universe minus BTC, ETH and tokenised equities (`common.target_coins`).

## Running

```
pip install -r requirements.txt
cp config.json my.json    # or edit config.json in place: replace every <placeholder>
python ma_cross.py plan --run-id macx-2026-09                 # plan.json + ONE controller job; everything after plan runs on Batch
python ma_cross.py status --run-id macx-2026-09 --watch
python ma_cross_report.py --run-id macx-2026-09               # PDF + HTML twin into paths.viewer_out
python ma_cross_viewer.py --run-id macx-2026-09 --port 8781   # then open http://127.0.0.1:8781/
python -m pytest tests -q
```

`plan --coins SOL,XRP,DOGE --dates 2026-09-01 2026-09-02 2026-09-03` narrows a smoke run. The Batch image must carry this package and dispatch the command
word `macx` to `ma_cross.main` (the controller submits `["macx", "sig" | "bask" | "pnl" | "run", ...]`); the queues and job definition are in `config.json`.
Stage sizing: `sig` and `bask` are array jobs of 8 vCPU / 32–64 GB, `pnl` is one job that holds every coin x venue return series in memory (~15 GB float32
for 1,500 series x 2.4 M seconds) and evaluates the five target marks in threads.

## Terminology

**Target / mark**: what is traded and how it is priced — `pooled` (alt basket at the dataset's `ALL` mid, which averages every venue's quote up to 10 s old;
unusable at 1 s), `primary` (alt basket, each coin at its top venue by notional, forward-filled mid; the readable mark, still stale on thinly traded coins),
`fresh` (primary venue, returns only between consecutive seconds that both have a bar; biased long, discard), `btc` / `eth` (the source coin itself).
**Signal**: `btc`, `eth`, or `both` (direction when BTC and ETH agree, held otherwise; never flat). **Arm**: `imb_usd` / `imb_sec` (imbalance on dollar / time
bars), `px_usd` / `px_sec` (price). **Type**: `timb`, `bimb`, `px`. **Rule**: `arm:type:fast:slow`. **Lag**: execution delay in seconds (0 = upper bound,
1 = executable). **Hit ratio**: share of holding segments (flip to flip) with positive PnL regardless of size. **Break-even per flip**: total bp / flips,
read against the basket's mean half-spread.

## Findings (September 2026, 28 days, gross)

- A real 1–3 s lead-lag from BTC/ETH flow into the alts: imbalance crossovers are positive on 95–99 % of window pairs, every day positive for the best, and
  significant against the circular-shift null. Book imbalance beats trade imbalance beats price; information bars help the price MA, not the imbalance MA.
- The edge lives at seconds: median annualised Sharpe falls monotonically once the slow window passes 40 bars (42 at 30–40 bars → 2 at 500,000); pairs
  with both windows long are flat to negative.
- The mark decides the level: median Sharpe 8–13 on the pooled mid vs 38–43 on the primary mid for the same rules; both the long and the short leg carry it,
  so it is not market beta. Hit ratio 0.46–0.51 for imbalance rules, 0.33–0.40 for price rules; the median trade is ~0 bp — the PnL is in the right tail.
- Nothing survives the touch: break-even 0.2–0.8 bp per flip against a 1.8 bp half-spread for the short windows; the price MA on dollar bars earns 1.3 bp per
  flip. BTC and ETH traded on their own flow show the same picture (the microprice effect) with price MAs dead.
- Measurement lessons worth keeping: forward-filled mids of thinly traded coins fake a lead-lag, and a venue that "works" better than the most liquid one is
  usually a venue whose quotes are stale.
