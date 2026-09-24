"""S3 results browser — zero-dependency web app (stdlib http.server + boto3).

Browse the basket-study / sentiment result prefixes directly in S3 and view
charts (PNG), tables (CSV/.k), JSON, and parquet summaries in the browser.
Read-only: the server never writes to S3.

Usage:
    scripts/.venv/bin/python tools/results_browser/server.py \
        [--port 8777] [--profile <profile>] [--bucket <bucket>] [--open]

Then browse http://127.0.0.1:8777/
"""

import argparse
import html
import io
import json
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3

import study10y  # same directory; /study10y with-vs-without-S&P viewer

import settings  # noqa: E402
# Landing-page shortcuts: the result trees people actually look at.
SHORTCUTS = [
    ("10-year basket study — interactive viewer: with vs. without S&P (SPY) leg", "/study10y"),
    ("10-year basket study — finalize charts (strategy only, w=0)", "earnings-basket-study/results-10y/aggregate/"),
    ("10-year basket study — +1x SPY overlay charts (w=1)", "earnings-basket-study/results-10y/aggregate_spyw1/"),
    ("10-year basket study — +1x SPY, 2x strategy charts (w=1, L=2)", "earnings-basket-study/results-10y/aggregate_spyw1_lev2/"),
    ("Sentiment analysis — null band + model placement", "earnings-basket-study/results-sentiment-analysis/aggregate/"),
    ("Sentiment analysis — LARGE tier (null band + model placement)", "earnings-basket-study/results-sentiment-analysis-large/aggregate/"),
    ("XLE sentiment study — comparison charts", "earnings-basket-study/results-xle-sentiment/aggregate-sentiment/"),
    ("XLE sentiment study — per-view aggregates (baseline + model×band)", "earnings-basket-study/results-xle-sentiment/views/"),
    ("XLE full-history fade baseline", "earnings-basket-study/results-xle-full/aggregate/"),
    ("2-year study", "earnings-basket-study/results-2y/aggregate/"),
    ("Main results", "earnings-basket-study/results/aggregate/"),
]

_s3 = None
_bucket = None

CSS = """
body{font-family:-apple-system,Segoe UI,sans-serif;margin:2rem;color:#222}
a{color:#0b5394;text-decoration:none} a:hover{text-decoration:underline}
h1{font-size:1.3rem} h2{font-size:1.05rem;margin-top:1.5rem}
table{border-collapse:collapse;font-size:.85rem}
td,th{border:1px solid #ddd;padding:.25rem .6rem;text-align:right}
th{background:#f5f5f5} td:first-child,th:first-child{text-align:left}
.crumb{color:#666;font-size:.9rem;margin-bottom:1rem}
.dir{font-weight:600} .size{color:#999;font-size:.8rem;margin-left:.6rem}
img{max-width:100%;border:1px solid #ddd;margin-top:.5rem}
pre{background:#f8f8f8;padding:1rem;overflow-x:auto;font-size:.8rem}
.grid{display:flex;flex-wrap:wrap;gap:1rem}
.grid img{max-width:640px}
"""


def page(title: str, body: str) -> bytes:
    return (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{html.escape(title)}</title><style>{CSS}</style></head>"
            f"<body>{body}</body></html>").encode()


def crumbs(prefix: str) -> str:
    out = ["<a href='/'>root</a>"]
    acc = ""
    for part in [p for p in prefix.split("/") if p]:
        acc += part + "/"
        out.append(f"<a href='/browse?prefix={urllib.parse.quote(acc)}'>{html.escape(part)}</a>")
    return "<div class='crumb'>" + " / ".join(out) + "</div>"


_list_cache = {}
_LIST_TTL_S = 60


def list_prefix(prefix: str):
    """S3 listing with a short TTL cache — newly uploaded results appear
    within a minute (an unbounded lru_cache made fresh charts invisible)."""
    hit = _list_cache.get(prefix)
    if hit and time.time() - hit[0] < _LIST_TTL_S:
        return hit[1]
    dirs, files = [], []
    paginator = _s3.get_paginator("list_objects_v2")
    for pg in paginator.paginate(Bucket=_bucket, Prefix=prefix, Delimiter="/"):
        dirs += [c["Prefix"] for c in pg.get("CommonPrefixes", [])]
        files += [(o["Key"], o["Size"]) for o in pg.get("Contents", []) if o["Key"] != prefix]
    _list_cache[prefix] = (time.time(), (dirs, files))
    return dirs, files


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def render_browse(prefix: str) -> bytes:
    dirs, files = list_prefix(prefix)
    rows = []
    for d in dirs:
        name = d[len(prefix):].rstrip("/")
        rows.append(f"<div class='dir'>&#128193; <a href='/browse?prefix={urllib.parse.quote(d)}'>{html.escape(name)}/</a></div>")
    pngs = [k for k, _ in files if k.endswith(".png")]
    for k, size in files:
        name = k[len(prefix):]
        grid = (f" <a href='/table?key={urllib.parse.quote(k)}' title='open in grid'>&#128202;</a>"
                if k.endswith(".parquet") else "")
        rows.append(f"<div>&#128196; <a href='/view?key={urllib.parse.quote(k)}'>{html.escape(name)}</a>"
                    f"{grid}<span class='size'>{fmt_size(size)}</span></div>")
    gallery = ""
    if pngs:
        gallery = ("<h2>Charts</h2><div class='grid'>"
                   + "".join(f"<a href='/view?key={urllib.parse.quote(k)}'>"
                             f"<img loading='lazy' src='/raw?key={urllib.parse.quote(k)}'"
                             f" title='{html.escape(k.rsplit('/',1)[-1])}'></a>" for k in pngs)
                   + "</div>")
    body = (f"<h1>s3://{_bucket}/{html.escape(prefix)}</h1>" + crumbs(prefix)
            + ("".join(rows) or "<p>(empty)</p>") + gallery)
    return page(prefix or "root", body)


def get_object(key: str) -> bytes:
    return _s3.get_object(Bucket=_bucket, Key=key)["Body"].read()


# --------------------------------------------------------------- data grid
# Excel-like parquet viewer: FINOS Perspective (CDN, no build step) fed by
# Arrow IPC. Sort/filter/pivot/drag-rearrange come from the component; numeric
# columns get a diverging heatmap by default. Layout persists per-file in
# localStorage.

PERSPECTIVE_VER = "3.1.3"

def render_arrow(key: str):
    """Parquet from S3 -> Arrow IPC stream bytes for Perspective."""
    import pyarrow as pa
    import pyarrow.ipc as ipc
    import pyarrow.parquet as pq
    table = pq.read_table(io.BytesIO(get_object(key)))
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as w:
        w.write_table(table)
    return sink.getvalue(), "application/vnd.apache.arrow.stream"


def render_table(key: str) -> bytes:
    name = key.rsplit("/", 1)[-1]
    q = urllib.parse.quote(key)
    v = PERSPECTIVE_VER
    return f"""<!doctype html><html><head><meta charset='utf-8'>
<title>{html.escape(name)} — grid</title>
<script type="module" src="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@{v}/dist/cdn/perspective-viewer.js"></script>
<script type="module" src="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@{v}/dist/cdn/perspective-viewer-datagrid.js"></script>
<script type="module" src="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@{v}/dist/cdn/perspective-viewer-d3fc.js"></script>
<link rel="stylesheet" crossorigin="anonymous"
      href="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@{v}/dist/css/themes.css"/>
<style>
  html,body{{margin:0;height:100%;overflow:hidden;font-family:-apple-system,Segoe UI,sans-serif}}
  #bar{{padding:.4rem .8rem;font-size:.85rem;background:#f5f5f5;border-bottom:1px solid #ddd}}
  #bar a{{color:#0b5394;text-decoration:none;margin-right:1rem}}
  perspective-viewer{{height:calc(100% - 2rem);width:100%}}
</style></head><body>
<div id="bar"><a href="/view?key={q}">&larr; plain view</a>
<b>{html.escape(name)}</b> <span style="color:#888">drag headers to rearrange
&middot; click to sort &middot; right side panel: pivot/filter/heatmap</span></div>
<perspective-viewer id="v" theme="Pro Light"></perspective-viewer>
<script type="module">
import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective@{v}/dist/cdn/perspective.js";
const viewer = document.getElementById("v");
const resp = await fetch("/arrow?key={q}");
const worker = await perspective.worker();
const table = await worker.table(await resp.arrayBuffer());
await viewer.load(table);
const LS = "results_browser_layout::{q}";
const saved = localStorage.getItem(LS);
if (saved) {{
  try {{ await viewer.restore(JSON.parse(saved)); }} catch (e) {{ console.warn(e); }}
}} else {{
  // default: datagrid with a diverging heatmap on every numeric column
  const schema = await table.schema();
  const numeric = Object.entries(schema)
    .filter(([_, t]) => t === "float" || t === "integer").map(([c]) => c);
  const styles = Object.fromEntries(numeric.map(c => [c,
    {{"number_bg_mode": "gradient", "bg_gradient": null}}]));
  await viewer.restore({{plugin: "Datagrid",
                         plugin_config: {{columns: styles}}}});
}}
viewer.addEventListener("perspective-config-update", async () => {{
  localStorage.setItem(LS, JSON.stringify(await viewer.save()));
}});
</script></body></html>""".encode()


def render_view(key: str) -> bytes:
    name = key.rsplit("/", 1)[-1]
    parent = key.rsplit("/", 1)[0] + "/"
    head = f"<h1>{html.escape(name)}</h1>" + crumbs(parent)
    q = urllib.parse.quote(key)
    if key.endswith(".png"):
        return page(name, head + f"<img src='/raw?key={q}'>")
    if key.endswith((".html", ".htm")):
        return get_object(key)          # self-contained reports render as-is
    if key.endswith((".csv", ".k", ".txt", ".log")):
        text = get_object(key).decode("utf-8", "replace")
        if key.endswith(".csv"):
            lines = text.splitlines()
            rows = ["<tr>" + "".join(f"<th>{html.escape(c)}</th>" for c in lines[0].split(",")) + "</tr>"]
            rows += ["<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in ln.split(",")) + "</tr>"
                     for ln in lines[1:2001]]
            note = f"<p class='crumb'>showing {min(len(lines)-1,2000)} of {len(lines)-1} rows</p>"
            return page(name, head + note + "<table>" + "".join(rows) + "</table>")
        return page(name, head + f"<pre>{html.escape(text[:400_000])}</pre>")
    if key.endswith(".json"):
        text = json.dumps(json.loads(get_object(key)), indent=2)
        return page(name, head + f"<pre>{html.escape(text[:400_000])}</pre>")
    if key.endswith(".parquet"):
        import pandas as pd
        df = pd.read_parquet(io.BytesIO(get_object(key)))
        info = (f"<p class='crumb'>{len(df)} rows x {len(df.columns)} cols — first 200 rows"
                f" &middot; <a href='/table?key={q}'>&#128202; open in grid"
                f" (sort/heatmap/pivot, all rows)</a></p>")
        return page(name, head + info + df.head(200).to_html(index=False, border=0))
    return page(name, head + f"<p>No viewer for this type — <a href='/raw?key={q}'>download</a></p>")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quiet
        pass

    def _send(self, body: bytes, ctype="text/html; charset=utf-8", code=200):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            url = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(url.query)
            if url.path == "/":
                items = "".join(
                    f"<li><a href='{p if p.startswith('/') else '/browse?prefix=' + urllib.parse.quote(p)}'>"
                    f"{html.escape(t)}</a><div class='crumb'>{html.escape(p)}</div></li>" for t, p in SHORTCUTS)
                self._send(page("results browser",
                                f"<h1>S3 results browser — {_bucket}</h1><ul>{items}</ul>"
                                "<p class='crumb'>or browse any prefix: /browse?prefix=...</p>"))
            elif url.path == "/browse":
                self._send(render_browse(qs.get("prefix", [""])[0]))
            elif url.path == "/study10y":
                self._send(study10y.render_page())
            elif url.path == "/study10y/data":
                self._send(study10y.data_json(qs), ctype="application/json")
            elif url.path == "/view":
                self._send(render_view(qs["key"][0]))
            elif url.path == "/table":
                self._send(render_table(qs["key"][0]))
            elif url.path == "/arrow":
                body, ctype = render_arrow(qs["key"][0])
                self._send(body, ctype=ctype)
            elif url.path == "/raw":
                key = qs["key"][0]
                ctype = "image/png" if key.endswith(".png") else "application/octet-stream"
                self._send(get_object(key), ctype=ctype)
            elif url.path.startswith("/f/"):
                # clean path-style URLs (no query string — survives terminal
                # link detection): /f/<key> views, /f/<key>?raw=1 streams
                key = urllib.parse.unquote(url.path[3:])
                if key.endswith("/"):
                    self._send(render_browse(key))
                elif "raw" in qs:
                    ctype = "image/png" if key.endswith(".png") else "application/octet-stream"
                    self._send(get_object(key), ctype=ctype)
                else:
                    self._send(render_view(key))
            else:
                self._send(page("404", "<h1>404</h1>"), code=404)
        except Exception as e:  # render errors in-browser, keep server alive
            self._send(page("error", f"<h1>error</h1><pre>{html.escape(repr(e))}</pre>"), code=500)


def main():
    global _s3, _bucket
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=8777)
    ap.add_argument("--profile", default=None, help="AWS profile (default: settings aws.profile)")
    ap.add_argument("--bucket", default=None, help="results bucket (default: settings s3.data_bucket)")
    ap.add_argument("--open", action="store_true", help="open the browser")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")
    _bucket = args.bucket or settings.get("s3", "data_bucket")
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    _s3 = session.client("s3")
    study10y.init(get_object)
    srv =ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}/"
    print(f"results browser: {url} (bucket {_bucket}, profile {args.profile})", flush=True)
    if args.open:
        import webbrowser
        webbrowser.open(url)
    srv.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
