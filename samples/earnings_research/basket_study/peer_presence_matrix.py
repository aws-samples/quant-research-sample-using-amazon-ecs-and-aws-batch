"""peer_presence_matrix — chronological peer presence table for chosen reporters.

One row per event (date order), one FIXED column per peer name, grouped by
universe (all / correlated / functional / pure play), a mark where the name
was in that universe for that event. A name dropping out or returning shows
as a gap in its column. Peers = theoretical universe per event (equal-weight
row). Columns sorted by how often the name appears. Tier in red = LARGE.

    AWS_PROFILE=<profile> python peer_presence_matrix.py BLMN WEN GPRO \
        [--out-key earnings-basket-study/results-10y/diagnostics/peer_stability/presence_matrix_BLMN_WEN_GPRO.html]

Writes ONE self-contained HTML page to S3 (view via the results browser
/view?key=...). Default out-key = .../presence_matrix_<SYM1>_<SYM2>....html
"""
import argparse
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
import aggregate  # noqa: E402
import settings
from s3io import S3IO  # noqa: E402

UNIS = ["all", "correlated", "functional", "pure play"]
COL = {"all": "#2a78d6", "correlated": "#eb6834", "functional": "#1baf7a", "pure play": "#9b2fae"}
CSS = """body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:1.2rem 1.6rem;color:#0b0b0b;background:#f9f9f7;font-size:.85rem}
h1{font-size:1.2rem;margin:0 0 .3rem} p.sub{color:#52514e;margin:0 0 1rem} h2{font-size:1rem;margin:1.4rem 0 .4rem} h2 span{color:#52514e;font-weight:400;font-size:.8rem;margin-left:.6rem}
.scroll{overflow:auto;max-height:90vh;background:#fcfcfb;border:1px solid rgba(11,11,11,.1);border-radius:10px}
table{border-collapse:separate;border-spacing:0;font-variant-numeric:tabular-nums}
th,td{padding:.15rem .4rem;text-align:center;border-bottom:1px solid #e1e0d9;white-space:nowrap}
th{position:sticky;background:#fcfcfb;font-weight:500;font-size:.75rem;z-index:2} tr:first-child th{top:0} tr:nth-child(2) th{top:1.55rem}
th.l,td.l{text-align:left;position:sticky;left:0;background:#fcfcfb;z-index:3} tr:first-child th.l{z-index:4}
.grp{border-left:2px solid #c3c2b7} td.off{color:#c3c2b7} td.lg{color:#b93535;font-weight:600}
tbody tr:hover td{filter:brightness(.96)}"""


def matrix(df, sym):
    g = (df.filter((pl.col("event_symbol") == sym) & (pl.col("construction") == "equal_weight_dollar_neutral")
                   & pl.col("peers_theo").is_not_null())
         .select("event_date", "liquidity_tier", "universe", "peers_theo")
         .unique(["event_date", "universe"]).sort("event_date").to_pandas())
    dates = sorted(g.event_date.unique())
    tier = g.drop_duplicates("event_date").set_index("event_date")["liquidity_tier"]
    sets = {(r.event_date, r.universe): set(r.peers_theo.split()) for r in g.itertuples()}
    groups = {}
    for u in UNIS:
        names = set().union(*[s for (d, uu), s in sets.items() if uu == u]) if any(uu == u for _, uu in sets) else set()
        groups[u] = sorted(names, key=lambda n: (-sum(n in sets.get((d, u), set()) for d in dates), n))
    return dates, tier, sets, groups


def table(df, sym):
    dates, tier, sets, groups = matrix(df, sym)
    if not dates:
        return f"<h2>{sym} <span>no events with peers</span></h2>"
    h1 = ("<tr><th rowspan=2 class=l>event</th><th rowspan=2>tier</th>"
          + "".join(f"<th colspan={len(groups[u])} class=grp style='color:{COL[u]}'>{u} ({len(groups[u])} names)</th>"
                    for u in UNIS) + "</tr>")
    h2 = "<tr>" + "".join("".join(f"<th class='{'grp' if i == 0 else ''}' style='color:{COL[u]}'>{n}</th>"
                                  for i, n in enumerate(groups[u])) for u in UNIS) + "</tr>"
    body = []
    for d in dates:
        cells = ""
        for u in UNIS:
            s = sets.get((d, u))
            present = [n in s for n in groups[u]] if s is not None else [None] * len(groups[u])
            for i, p in enumerate(present):
                cls = ("grp " if i == 0 else "") + ("on" if p else "off" if p is False else "na")
                style = f"background:{COL[u]}22;color:{COL[u]}" if p else ""
                cells += f"<td class='{cls}' style='{style}'>{'●' if p else ('·' if p is False else '')}</td>"
        body.append(f"<tr><td class=l>{d}</td><td class='{'lg' if tier[d] == 'large' else ''}'>{tier[d]}</td>{cells}</tr>")
    return (f"<h2>{sym} <span>{len(dates)} events {dates[0]} → {dates[-1]}</span></h2>"
            f"<div class=scroll><table>{h1}{h2}{''.join(body)}</table></div>")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("reporters", nargs="+")
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--out-key", default=None)
    ap.add_argument("--profile", default=None)
    args = ap.parse_args()
    s3io = S3IO(profile=args.profile)
    df = pl.read_parquet(f"s3://{settings.get("s3", "data_bucket")}/{args.s3_prefix}/shards_consolidated/shards.parquet",
                         storage_options=s3io.pl_opts)
    syms = [s.upper() for s in args.reporters]
    page = (f"<!doctype html><html><head><meta charset='utf-8'><title>Peer presence — {', '.join(syms)}</title>"
            f"<style>{CSS}</style></head><body>"
            "<h1>Peer presence — one column per peer name, one row per event (chronological)</h1>"
            "<p class=sub>Theoretical peers per event (equal-weight rows). ● = in the universe for that event; "
            "· = universe existed for that event but this name was absent; blank = no peer list for that event. "
            "Columns are fixed per name and sorted by how often the name appears, so a name dropping out or "
            "returning shows as a gap in its column. Tier in red = the event fell in the LARGE tier that day.</p>"
            + "".join(table(df, s) for s in syms) + "</body></html>")
    key = args.out_key or f"{args.s3_prefix}/diagnostics/peer_stability/presence_matrix_{'_'.join(syms)}.html"
    s3io.write_bytes(page.encode(), f"s3://{settings.get("s3", "data_bucket")}/{key}")
    print(f"wrote s3://{settings.get("s3", "data_bucket")}/{key}\nview: http://127.0.0.1:8777/view?key={key}")


if __name__ == "__main__":
    main()
