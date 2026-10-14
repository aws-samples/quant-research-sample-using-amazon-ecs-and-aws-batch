"""MA crossover basket viewer: metrics of every rule x combination x lag x basket mark, pivoted over any two parameters, straight from the run's
summary parquet on S3 (Polars on request, cached in memory, no local copies). A small local server (stdlib http); the heavy work (positions, PnL, nulls)
was done on Batch by scripts/ma_cross.py. Spec: docs/superpowers/specs/2026-10-12-ma-crossover-basket-design.md.

  python ma_cross_viewer.py [--run-id <id>] [--port 8781]      (default run: runs.default of config.json)
  then open http://127.0.0.1:8781/

Routes: /  study page (one cell = target x signal x lag x rule: equity, drawdown, per-trade histogram, lag profile, and one neighbours table per dimension —
pairs, arms, signals, targets — plus exchanges and coin x exchange for the cell; click a row to switch, like the 10-year basket viewer);  /pivot  the pivot page;
/cell?run&mark&combo&lag&rule  everything the study page shows for one cell;  /meta?run  plan, baselines per mark, dimensions;  /rules?run  every rule x combo x lag x mark with its metrics (+ daily median bp,
average bp per trade);  /daily?run&rule&combo&lag&mark  the rule's daily PnL next to the always-long basket;  /lagprofile?run&rule&mark  total PnL by
execution lag;  /coins?run&rule&mark  per-coin contributions (arm winners only). Clock labels in ET (user preference)."""
import argparse, functools, json, math, os, sys, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import numpy as np, polars as pl
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as tv
import ma_cross_core as mc

import settings
DEFAULT_RUN = None                                 # default = settings runs.default
DIMS = ["fast", "slow", "arm", "typ", "combo", "lag_s", "mark"]
METRICS = [("sharpe_ann", "Sharpe (ann.)", 2), ("daily_mean_bp", "mean daily bp", 1), ("daily_median_bp", "median daily bp", 1), ("total_bp", "total bp", 0), ("hit_rate", "hit ratio (per trade)", 3),
           ("mean_trade_bp", "mean bp per trade", 3), ("median_trade_bp", "median bp per trade", 3), ("avg_bp_per_trade", "total / trades (bp)", 3), ("trades_per_day", "trades / day", 0), ("median_hold_s", "median hold s", 0), ("breakeven_bp_per_flip", "break-even bp / flip", 2), ("pos_day_share", "days > 0", 2),
           ("max_drawdown_bp", "max drawdown bp", 0), ("long_share", "long share", 2), ("p_value", "p (shift null)", 3), ("daily_sd_bp", "daily sd bp", 1)]


# ----------------------------------------------------------------------------------------------- data (Polars from S3, cached per run)
def _root(run_id): return f"{tv.out_root()}/{run_id}/macx"


@functools.lru_cache(maxsize=16)
def _summary(run_id, name):
    f = tv.fs(); k = tv._key(f"{_root(run_id)}/summary/{name}.parquet")
    return pl.read_parquet(f"s3://{k}", storage_options=tv.so()) if f.exists(k) else None


@functools.lru_cache(maxsize=4)
def _plan(run_id): return tv.read_json(f"{_root(run_id)}/plan.json")


@functools.lru_cache(maxsize=4)
def _done(run_id):
    f = tv.fs(); k = tv._key(f"{_root(run_id)}/summary/_done.json"); return tv.read_json(f"s3://{k}") if f.exists(k) else {}


def rules_doc(R, D):
    """Rules table + the daily median PnL (from the daily table), trades per day (flips + 1 per day), average bp per trade. Lists of plain values per column."""
    if R is None: return {"columns": [], "rows": []}
    X = R
    if D is not None and D.height:
        M = D.filter(pl.col("rule") != "always_long").group_by("rule", "combo", "lag_s", "mark").agg(pl.col("pnl_bp").median().alias("daily_median_bp"))
        X = X.join(M, on=["rule", "combo", "lag_s", "mark"], how="left")
    else: X = X.with_columns(pl.lit(None, dtype=pl.Float64).alias("daily_median_bp"))
    X = X.with_columns(((pl.col("flips") + 1) / pl.col("n_days")).alias("trades_per_day"), (pl.col("total_bp") / (pl.col("flips") + 1)).alias("avg_bp_per_trade"))
    cols = [c for c in X.columns if c not in ("n_secs", "sharpe_daily", "null_p95_bp")]
    return {"columns": cols, "rows": [[_clean(v) for v in row] for row in X.select(cols).iter_rows()]}


def meta_doc(plan, baseline, done):
    B = baseline.to_dicts() if baseline is not None else []
    return {"run_id": plan.get("run_id"), "dates": plan.get("dates", []), "coins": len(plan.get("targets", [])), "windows": plan.get("windows", mc.WINDOWS), "arms": plan.get("arms", {k: list(v) for k, v in mc.ARMS.items()}),
            "combos": plan.get("combos", mc.COMBOS), "lags_s": plan.get("lags_s", mc.LAGS_S), "marks": sorted({b["mark"] for b in B}) or ["pooled"], "baseline": [{k: _clean(v) for k, v in b.items()} for b in B],
            "metrics": [{"key": k, "label": l, "dec": d} for k, l, d in METRICS], "dims": DIMS, "done": {k: _clean(v) for k, v in done.items() if k != "marks"}, "half_spread_bp": {m: _clean(v.get("half_spread_bp")) for m, v in done.get("marks", {}).items()}}


def daily_doc(D, rule, combo, lag_s, mark):
    if D is None: return {"dates": [], "pnl_bp": [], "always_long_bp": []}
    X = D.filter((pl.col("rule") == rule) & (pl.col("combo") == combo) & (pl.col("lag_s") == int(lag_s)) & (pl.col("mark") == mark)).sort("date")
    A = D.filter((pl.col("rule") == "always_long") & (pl.col("mark") == mark)).sort("date"); al = dict(zip(A["date"], A["pnl_bp"]))
    return {"dates": X["date"].to_list(), "pnl_bp": [_clean(v) for v in X["pnl_bp"]], "always_long_bp": [_clean(al.get(d)) for d in X["date"]]}


def lagprofile_doc(L, rule, mark):
    if L is None: return {"lag_s": [], "total_bp": []}
    X = L.filter((pl.col("rule") == rule) & (pl.col("mark") == mark)).sort("lag_s"); return {"lag_s": X["lag_s"].to_list(), "total_bp": [_clean(v) for v in X["total_bp"]]}


def coins_doc(C, rule, mark):
    if C is None: return {"rows": []}
    X = C.filter((pl.col("rule") == rule) & (pl.col("mark") == mark)).sort("contrib_bp", descending=True)
    return {"rows": [{k: _clean(v) for k, v in r.items()} for r in X.select("coin", "contrib_bp", "always_long_bp", "secs_with_value").to_dicts()]}


def cell_doc(T, meta, mark, combo, lag_s, rule):
    """Everything the study page shows for one cell. T = dict of summary frames (rules, daily, hourly, hist, lagprofile, venues, cube_index, baseline)."""
    lag_s = int(lag_s); R = T.get("rules"); sel = lambda F, **kw: F.filter(pl.all_horizontal([pl.col(k) == v for k, v in kw.items()])) if F is not None else None
    row = sel(R, rule=rule, combo=combo, lag_s=lag_s, mark=mark); row = row.to_dicts()[0] if row is not None and row.height else None
    if row is None: return {"error": f"no such cell: {mark} {combo} {lag_s} {rule}"}
    arm, typ, fast, slow = row["arm"], row["typ"], row["fast"], row["slow"]; out = {"cell": {k: _clean(v) for k, v in row.items()}}
    D = T.get("daily")
    if D is not None:
        X = sel(D, rule=rule, combo=combo, lag_s=lag_s, mark=mark).sort("date"); A = sel(D, rule="always_long", mark=mark).sort("date")
        out["daily"] = {"dates": X["date"].to_list(), "pnl_bp": [_clean(v) for v in X["pnl_bp"]], "always_long_bp": [_clean(v) for v in A["pnl_bp"]]}; out["cell"]["daily_median_bp"] = _clean(X["pnl_bp"].median()) if X.height else None
    H = T.get("hourly")
    if H is not None:
        X = sel(H, rule=rule, combo=combo, lag_s=lag_s, mark=mark).sort("hour"); A = sel(H, rule="always_long", mark=mark).sort("hour")
        out["hourly"] = {"hour": X["hour"].to_list(), "pnl_bp": [_clean(v) for v in X["pnl_bp"]], "always_long_bp": [_clean(v) for v in A["pnl_bp"]]}
    Hs = T.get("hist")
    if Hs is not None:
        X = sel(Hs, rule=rule, combo=combo, mark=mark); out["hist"] = [{"lo": _clean(a), "hi": _clean(b), "count": int(c)} for a, b, c in zip(X["bin_lo"], X["bin_hi"], X["count"])] if X.height else []
    L = T.get("lagprofile"); out["lagprofile"] = lagprofile_doc(L, rule, mark)
    Hd = T.get("hod")
    out["hod"] = [{k: _clean(v) for k, v in r.items()} for r in sel(Hd, rule=rule, combo=combo, mark=mark).sort("hod_et").select([c for c in ["hod_et", "n_hours", "mean_hour_bp", "share_hours_pos", "n_trades", "hit_rate", "trades_bp", "mean_trade_bp", "median_trade_bp"] if c in Hd.columns]).to_dicts()] if Hd is not None else []
    keep = ["rule", "arm", "typ", "fast", "slow", "combo", "lag_s", "mark", "sharpe_ann", "total_bp", "daily_mean_bp", "hit_rate", "mean_trade_bp", "median_trade_bp", "flips_per_day", "median_hold_s", "breakeven_bp_per_flip", "pos_day_share", "max_drawdown_bp", "long_share", "p_value"]
    rows = lambda F: [{k: _clean(v) for k, v in r.items() if k in keep} for r in F.select([c for c in keep if c in F.columns]).to_dicts()] if F is not None else []
    out["pairs"] = rows(sel(R, arm=arm, typ=typ, combo=combo, lag_s=lag_s, mark=mark)); out["arms"] = rows(sel(R, fast=fast, slow=slow, combo=combo, lag_s=lag_s, mark=mark))
    out["signals"] = rows(sel(R, rule=rule, mark=mark)); out["targets"] = rows(sel(R, rule=rule, combo=combo, lag_s=lag_s))
    V = T.get("venues"); out["venues"] = [{k: _clean(v) for k, v in r.items()} for r in sel(V, rule=rule, combo=combo).select([c for c in ["venue", "n_coins", "sharpe_ann", "total_bp", "daily_mean_bp", "hit_rate", "flips_per_day", "median_hold_s", "breakeven_bp_per_flip", "pos_day_share", "max_drawdown_bp"] if c in V.columns]).to_dicts()] if V is not None else []
    CI = T.get("cube_index"); out["coins"] = [{k: _clean(v) for k, v in r.items()} for r in sel(CI, rule=rule, combo=combo).select("coin", "venue", "total_bp", "sharpe_ann", "days_up", "secs_with_value").to_dicts()] if CI is not None else []
    B = T.get("baseline"); b = sel(B, mark=mark, name="always_long") if B is not None else None; out["baseline"] = {k: _clean(v) for k, v in b.to_dicts()[0].items()} if b is not None and b.height else {}
    return out


def _clean(v):
    if isinstance(v, (np.integer,)): return int(v)
    if isinstance(v, (np.floating, float)): return None if (v is None or math.isnan(float(v)) or math.isinf(float(v))) else float(v)
    return v


# ----------------------------------------------------------------------------------------------- page
PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>MA crossover basket — parameter viewer</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
<style>
:root{--surface-1:#fcfcfb;--ink-1:#1f1f1e;--ink-2:#5b5b58;--ink-3:#8a8a86;--hairline:#e6e5e1;--pos:#2a78d6;--neg:#eb6834}
body{margin:0;background:var(--surface-1);color:var(--ink-1);font:14px/1.4 -apple-system,Segoe UI,Helvetica,Arial,sans-serif}
header{padding:14px 24px 4px;display:flex;gap:18px;align-items:baseline;flex-wrap:wrap}h1{font-size:18px;margin:0;font-weight:600}
.sub{color:var(--ink-2);font-size:13px;padding:0 24px 6px;max-width:1500px}.controls{display:flex;gap:12px;align-items:center;padding:5px 24px;color:var(--ink-2);font-size:13px;flex-wrap:wrap}
.controls select,.controls input{border:1px solid var(--hairline);background:#fff;border-radius:6px;padding:4px 10px;color:var(--ink-1);font-size:13px}.controls label{white-space:nowrap}
.tiles{display:flex;gap:10px;padding:6px 24px;flex-wrap:wrap}.tile{border:1px solid var(--hairline);border-radius:8px;padding:7px 12px;min-width:104px}
.tile .v{font-size:16px;font-weight:600;font-variant-numeric:tabular-nums}.tile .l{color:var(--ink-2);font-size:11px}.note{color:var(--ink-3);font-size:12px;padding:0 24px 10px;max-width:1500px}
h2{font-size:14px;font-weight:600;margin:14px 24px 6px}
table{border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums;margin:0 24px}th{text-align:right;padding:4px 7px;color:var(--ink-2);font-weight:500;border-bottom:1px solid var(--hairline);cursor:pointer;white-space:nowrap;position:sticky;top:0;background:var(--surface-1);user-select:none}
td{padding:3px 7px;text-align:right;border-bottom:1px solid #f0efeb;white-space:nowrap}td.l,th.l{text-align:left}tbody tr:hover td{background:#f4f3ef;cursor:pointer}th.on{color:var(--ink-1);font-weight:600}th.over{box-shadow:inset 3px 0 0 var(--ink-2)}th:active{cursor:grabbing}
tr.hl td{background:#ff0 !important}tr.sel td{outline:2px solid var(--ink-1)}
#pivot td.c{min-width:52px;cursor:pointer}#pivot td.c.sel{outline:2px solid var(--ink-1)}#pivot th.y{text-align:left}
.wrap{max-height:520px;overflow:auto;margin:0 0 16px}.row{display:flex;gap:18px;padding:0 24px;flex-wrap:wrap;align-items:flex-start}.chart{height:300px;flex:1 1 480px;min-width:420px}
</style></head><body>
<header><h1>MA crossover on pooled BTC/ETH flow → long/short basket: parameter viewer</h1><span id="status" style="color:var(--ink-3);font-size:12px"></span></header>
<div class="sub" id="sub"></div>
<div class="controls"><label>run <input id="run" size="16"></label><label>metric <select id="metric"></select></label><label>aggregate <select id="agg"><option>median</option><option>mean</option><option>max</option><option>min</option><option>count</option></select></label>
<label>rows <select id="ydim"></select></label><label>columns <select id="xdim"></select></label></div>
<div class="controls" id="filters"></div>
<div class="tiles" id="tiles"></div>
<div class="note">Cells: the chosen aggregate of the metric over every rule that falls in the cell after the filters. Blue = above the metric's neutral value (zero for bp and for Sharpe; one half for shares and for the hit ratio), orange = below; colour depth is rank-free, scaled to the largest |value| on the page. Click a cell to list its rules; click a rule for its daily PnL, lag profile and (for arm winners) per-coin contributions. All numbers gross, mid-to-mid. Hit ratio = share of holding segments (one position from flip to flip) with positive PnL, regardless of size; trades / day = segments per day.</div>
<div class="wrap"><table id="pivot"><thead></thead><tbody></tbody></table></div>
<h2 id="rules_h">Rules in the selected cell</h2>
<div class="wrap"><table id="rules"><thead></thead><tbody></tbody></table></div>
<h2 id="detail_h">Rule detail</h2>
<div class="tiles" id="dtiles"></div>
<div class="row"><div id="daily" class="chart"></div><div id="lag" class="chart"></div><div id="coins" class="chart"></div></div>
<script>
const $=id=>document.getElementById(id);let META=null,RULES=[],sel={cell:null,rule:null};
const fmt={f0:v=>v==null?'–':v.toFixed(0),f1:v=>v==null?'–':v.toFixed(1),f2:v=>v==null?'–':v.toFixed(2),f3:v=>v==null?'–':v.toFixed(3),i:v=>v==null?'–':Math.round(v).toLocaleString(),s:v=>v==null?'–':v,n:v=>v==null?'–':v};
const NEUTRAL={hit_rate:0.5,pos_day_share:0.5,long_share:0.5,p_value:0.05};
const TS={};
function table(id,cols,data,o){
 const st=TS[id]||(TS[id]={order:cols.map(c=>c[0]),key:o.key,desc:o.desc!==false});const left=o.left||[];
 const C=st.order.map(k=>cols.find(c=>c[0]===k)).filter(Boolean);
 const R=data.slice().sort((a,b)=>{const x=a[st.key],y=b[st.key];if(x==null&&y==null)return 0;if(x==null)return 1;if(y==null)return -1;return(typeof x==='string'||typeof y==='string'?String(x).localeCompare(String(y)):x-y)*(st.desc?-1:1)});
 const t=$(id);
 t.querySelector('thead').innerHTML='<tr>'+C.map(([k,l])=>`<th draggable="true" data-k="${k}" class="${left.includes(k)?'l ':''}${k===st.key?'on':''}">${l}${k===st.key?(st.desc?' ▾':' ▴'):''}</th>`).join('')+'</tr>';
 t.querySelector('tbody').innerHTML=R.length?R.map(e=>`<tr ${o.rowAttr?o.rowAttr(e):''} class="${o.rowClass?o.rowClass(e):''}">`+C.map(([k,l,f])=>`<td class="${left.includes(k)?'l':''}">${fmt[f](e[k])}</td>`).join('')+'</tr>').join(''):`<tr><td colspan="${C.length}" class="l">${o.empty||'–'}</td></tr>`;
 t.querySelectorAll('th').forEach(th=>{
  th.onclick=()=>{const k=th.dataset.k;if(k===st.key)st.desc=!st.desc;else{st.key=k;st.desc=!left.includes(k)}table(id,cols,data,o)};
  th.ondragstart=ev=>{ev.dataTransfer.setData('text/plain',th.dataset.k);ev.dataTransfer.effectAllowed='move'};
  th.ondragover=ev=>{ev.preventDefault();th.classList.add('over')};th.ondragleave=()=>th.classList.remove('over');
  th.ondrop=ev=>{ev.preventDefault();const from=ev.dataTransfer.getData('text/plain'),to=th.dataset.k;if(!from||from===to)return;const i=st.order.indexOf(from),j=st.order.indexOf(to);const ord=st.order.filter(k=>k!==from);ord.splice(ord.indexOf(to)+(i<j?1:0),0,from);st.order=ord;table(id,cols,data,o)}});
 t.querySelectorAll('tbody tr').forEach(r=>r.onclick=ev=>{if(ev.shiftKey){r.classList.toggle('hl');return}if(o.onRow)o.onRow(r)});
 return st;}
async function get(u){const r=await fetch(u);if(!r.ok)throw new Error(u+' '+r.status);return r.json()}
async function load(){const run=$('run').value;$('status').textContent='loading '+run+' …';
 try{META=await get('/meta?run='+encodeURIComponent(run));const D=await get('/rules?run='+encodeURIComponent(run));RULES=D.rows.map(r=>Object.fromEntries(D.columns.map((c,i)=>[c,r[i]])));}
 catch(e){$('status').textContent='failed: '+e.message;return}
 $('status').textContent=`${RULES.length.toLocaleString()} rows`;
 $('sub').innerHTML=`Run <b>${META.run_id}</b>, ${META.dates[0]} → ${META.dates[META.dates.length-1]} (${META.dates.length} days), basket of ${META.coins} coins, ${META.windows.length} MA windows [${META.windows.join(', ')}] → ${META.windows.length*(META.windows.length-1)/2} fast &lt; slow pairs per arm x type. Marks: <b>pooled</b> = the ALL mid across venues (quotes up to 10 s old, unusable at 1 s), <b>primary</b> = each coin's top venue by notional, forward-filled mid (the one to read; still stale on thin coins), <b>fresh</b> = primary venue, returns only between two consecutive seconds with a bar (biased long, discard); <b>btc</b> / <b>eth</b> = the source coin itself traded on its primary venue (the combos then give own-flow and cross-flow). Combos: <b>both</b> = BTC and ETH agree, hold otherwise; <b>btc</b> / <b>eth</b> alone.`;
 $('metric').innerHTML=META.metrics.map(m=>`<option value="${m.key}">${m.label}</option>`).join('');
 const dl={fast:'fast window',slow:'slow window',arm:'arm',typ:'type',combo:'combination',lag_s:'lag s',mark:'mark'};
 $('ydim').innerHTML=META.dims.map(d=>`<option value="${d}">${dl[d]}</option>`).join('');$('xdim').innerHTML=$('ydim').innerHTML;$('ydim').value='fast';$('xdim').value='slow';
 const H=new URLSearchParams(location.hash.slice(1));for(const k of ['metric','agg','ydim','xdim'])if(H.get(k))$(k).value=H.get(k);
 buildFilters(H);render();}
function vals(d){return [...new Set(RULES.map(r=>r[d]))].sort((a,b)=>typeof a==='number'?a-b:String(a).localeCompare(String(b)))}
function buildFilters(H){const def={mark:['primary'],combo:['both'],lag_s:[1]};
 $('filters').innerHTML=META.dims.map(d=>{const vs=vals(d);const on=H&&H.get('f_'+d)?H.get('f_'+d).split(','):(def[d]?def[d].map(String):vs.map(String));
  return `<span><b>${d}</b> ${vs.map(v=>`<label><input type="checkbox" data-d="${d}" value="${v}" ${on.includes(String(v))?'checked':''}> ${v}</label>`).join(' ')}</span>`}).join('<span style="color:var(--hairline)">|</span>');
 $('filters').querySelectorAll('input').forEach(i=>i.onchange=render);}
function active(d){return new Set([...$('filters').querySelectorAll(`input[data-d="${d}"]:checked`)].map(i=>i.value))}
function filtered(){const A=Object.fromEntries(META.dims.map(d=>[d,active(d)]));return RULES.filter(r=>META.dims.every(d=>A[d].has(String(r[d]))))}
function agg(xs,how){const v=xs.filter(x=>x!=null);if(how==='count')return xs.length;if(!v.length)return null;if(how==='mean')return v.reduce((a,b)=>a+b,0)/v.length;if(how==='max')return Math.max(...v);if(how==='min')return Math.min(...v);v.sort((a,b)=>a-b);const m=v.length>>1;return v.length%2?v[m]:(v[m-1]+v[m])/2}
function colour(v,neutral,scale){if(v==null)return '';const d=(v-neutral)/scale;const a=Math.min(1,Math.abs(d))*0.85;return d>=0?`background:rgba(42,120,214,${a.toFixed(3)});color:${a>0.55?'#fff':'inherit'}`:`background:rgba(235,104,52,${a.toFixed(3)});color:${a>0.55?'#fff':'inherit'}`}
function hash(){const p=new URLSearchParams({metric:$('metric').value,agg:$('agg').value,ydim:$('ydim').value,xdim:$('xdim').value});for(const d of META.dims)p.set('f_'+d,[...active(d)].join(','));history.replaceState(null,'','#'+p.toString())}
function render(){hash();const m=$('metric').value,how=$('agg').value,yd=$('ydim').value,xd=$('xdim').value,dec=META.metrics.find(x=>x.key===m).dec;const F=filtered();
 const ys=vals(yd).filter(v=>F.some(r=>r[yd]===v)),xs=vals(xd).filter(v=>F.some(r=>r[xd]===v));const cell={};
 for(const r of F){const k=r[yd]+'|'+r[xd];(cell[k]=cell[k]||[]).push(r)}
 const val={};let mx=0;const neutral=how==='count'?0:(NEUTRAL[m]||0);
 for(const k in cell){val[k]=agg(cell[k].map(r=>r[m]),how);if(val[k]!=null)mx=Math.max(mx,Math.abs(val[k]-neutral))}
 const f=how==='count'?fmt.i:(v=>v==null?'–':v.toFixed(dec));
 const t=$('pivot');t.querySelector('thead').innerHTML=`<tr><th class="y">${yd} \\ ${xd}</th>${xs.map(x=>`<th>${x}</th>`).join('')}<th>row ${how}</th></tr>`;
 t.querySelector('tbody').innerHTML=ys.map(y=>{const rowv=agg(F.filter(r=>r[yd]===y).map(r=>r[m]),how);return `<tr><th class="y">${y}</th>${xs.map(x=>{const k=y+'|'+x,v=val[k];return `<td class="c ${sel.cell===k?'sel':''}" data-k="${k}" style="${colour(v,neutral,mx||1)}" title="${(cell[k]||[]).length} rules">${v==null?'':f(v)}</td>`}).join('')}<td style="${colour(rowv,neutral,mx||1)}"><b>${f(rowv)}</b></td></tr>`}).join('')
  +`<tr><th class="y">column ${how}</th>${xs.map(x=>{const v=agg(F.filter(r=>r[xd]===x).map(r=>r[m]),how);return `<td style="${colour(v,neutral,mx||1)}"><b>${f(v)}</b></td>`}).join('')}<td><b>${f(agg(F.map(r=>r[m]),how))}</b></td></tr>`;
 t.querySelectorAll('td.c').forEach(td=>td.onclick=()=>{sel.cell=td.dataset.k;render()});
 const B=META.baseline.filter(b=>active('mark').has(b.mark));
 $('tiles').innerHTML=[[F.length.toLocaleString(),'rules after filters'],[f(agg(F.map(r=>r[m]),how)),`${how} ${META.metrics.find(x=>x.key===m).label}`]].concat(B.filter(b=>b.name==='always_long').map(b=>[`${fmt.f1(b.sharpe_ann)} / ${fmt.f0(b.total_bp)} bp`,`always-long ${b.mark}: Sharpe / total`])).concat(B.filter(b=>b.name==='always_long').map(b=>[fmt.f2(b.half_spread_bp)+' bp',`${b.mark} basket half-spread`]))
  .map(([v,l])=>`<div class="tile"><div class="v">${v}</div><div class="l">${l}</div></div>`).join('');
 const sub=sel.cell?(cell[sel.cell]||[]):F;$('rules_h').textContent=sel.cell?`Rules in cell ${yd} = ${sel.cell.split('|')[0]}, ${xd} = ${sel.cell.split('|')[1]} (${sub.length})`:`All ${sub.length} rules after filters (click a cell to narrow)`;
 const cols=[['rule','rule','s'],['mark','mark','s'],['combo','combo','s'],['lag_s','lag s','n'],['sharpe_ann','Sharpe','f2'],['daily_mean_bp','mean daily bp','f1'],['daily_median_bp','median daily bp','f1'],['total_bp','total bp','f0'],['hit_rate','hit ratio','f3'],['mean_trade_bp','mean bp/trade','f3'],['median_trade_bp','median bp/trade','f3'],['trades_per_day','trades/day','f0'],['median_hold_s','hold s','f0'],['breakeven_bp_per_flip','break-even bp/flip','f2'],['pos_day_share','days > 0','f2'],['max_drawdown_bp','max DD bp','f0'],['long_share','long share','f2'],['p_value','p','f3'],['daily_sd_bp','daily sd bp','f1']];
 table('rules',cols,sub,{key:m==='p_value'?'sharpe_ann':m,left:['rule','mark','combo'],rowAttr:e=>`data-id="${e.rule}|${e.combo}|${e.lag_s}|${e.mark}"`,rowClass:e=>sel.rule===`${e.rule}|${e.combo}|${e.lag_s}|${e.mark}`?'sel':'',onRow:r=>{sel.rule=r.dataset.id;detail();render()}});}
async function detail(){if(!sel.rule)return;const [rule,combo,lag,mark]=sel.rule.split('|');const r=RULES.find(e=>e.rule===rule&&e.combo===combo&&String(e.lag_s)===lag&&e.mark===mark);if(!r)return;
 $('detail_h').textContent=`Rule ${rule} — ${combo}, lag ${lag} s, ${mark} mark`;
 $('dtiles').innerHTML=[[fmt.f2(r.sharpe_ann),'Sharpe (ann.)'],[fmt.f0(r.total_bp)+' bp','total'],[fmt.f1(r.daily_mean_bp)+' / '+fmt.f1(r.daily_median_bp),'mean / median daily bp'],[fmt.f3(r.hit_rate),'hit ratio'],[fmt.f3(r.mean_trade_bp)+' / '+fmt.f3(r.median_trade_bp)+' bp','mean / median per trade'],[fmt.f0(r.trades_per_day),'trades / day'],[fmt.f0(r.median_hold_s)+' s','median hold'],[fmt.f2(r.breakeven_bp_per_flip)+' bp','break-even / flip'],[fmt.f2(r.pos_day_share),'days > 0'],[fmt.f0(r.max_drawdown_bp)+' bp','max drawdown'],[fmt.f2(r.long_share),'long share'],[fmt.f3(r.p_value),'p (shift null)']]
  .map(([v,l])=>`<div class="tile"><div class="v">${v}</div><div class="l">${l}</div></div>`).join('');
 const run=encodeURIComponent($('run').value),q=`run=${run}&rule=${encodeURIComponent(rule)}&mark=${mark}`;
 const [D,L,C]=await Promise.all([get(`/daily?${q}&combo=${combo}&lag=${lag}`),get(`/lagprofile?${q}`),get(`/coins?${q}`)]);
 const lay=(t)=>({title:{text:t,font:{size:13}},margin:{l:55,r:15,t:34,b:45},paper_bgcolor:'#fcfcfb',plot_bgcolor:'#fff',font:{size:11},legend:{orientation:'h',y:-0.22},hovermode:'x unified'});
 Plotly.newPlot('daily',[{x:D.dates,y:D.pnl_bp,type:'bar',name:'rule',marker:{color:'#2a78d6'}},{x:D.dates,y:D.always_long_bp,type:'bar',name:'always-long basket',marker:{color:'#c9c7c1'}}],Object.assign(lay('daily PnL, bp'),{barmode:'group'}),{displaylogo:false,responsive:true});
 Plotly.newPlot('lag',[{x:L.lag_s,y:L.total_bp,mode:'lines+markers',name:'total bp',line:{color:'#2a78d6',width:2},marker:{size:8}}],Object.assign(lay('total PnL by execution lag (agreement rule)'),{xaxis:{type:'log',title:'lag s (log)'},yaxis:{title:'bp'}}),{displaylogo:false,responsive:true});
 if(C.rows.length){const top=C.rows.slice(0,15),bot=C.rows.slice(-15);const R=[...top,...bot.filter(b=>!top.includes(b))];Plotly.newPlot('coins',[{x:R.map(e=>e.coin),y:R.map(e=>e.contrib_bp),type:'bar',name:'rule contribution',marker:{color:R.map(e=>e.contrib_bp>=0?'#2a78d6':'#eb6834')}},{x:R.map(e=>e.coin),y:R.map(e=>e.always_long_bp),type:'bar',name:'always-long',marker:{color:'#c9c7c1'}}],Object.assign(lay('per-coin contribution to basket PnL, bp (top / bottom 15)'),{barmode:'group'}),{displaylogo:false,responsive:true})}
 else Plotly.purge('coins'),$('coins').innerHTML='<div class="note" style="padding:12px">per-coin contributions exist for the arm winners only</div>';}
$('run').value=new URLSearchParams(location.hash.slice(1)).get('run')||'__RUN__';$('run').onchange=load;for(const k of ['metric','agg','ydim','xdim'])$(k).onchange=render;load();
</script></body></html>"""


STUDY = r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>MA crossover basket — study viewer</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
<style>
:root{--surface-1:#fcfcfb;--ink-1:#1f1f1e;--ink-2:#5b5b58;--ink-3:#8a8a86;--hairline:#e6e5e1;--pos:#2a78d6;--neg:#eb6834}
body{margin:0;background:var(--surface-1);color:var(--ink-1);font:14px/1.4 -apple-system,Segoe UI,Helvetica,Arial,sans-serif}
header{padding:14px 24px 4px;display:flex;gap:18px;align-items:baseline;flex-wrap:wrap}h1{font-size:18px;margin:0;font-weight:600}header a{color:var(--pos);font-size:13px;margin-left:auto}
.sub{color:var(--ink-2);font-size:13px;padding:0 24px 6px;max-width:1500px}.controls{display:flex;gap:12px;align-items:center;padding:5px 24px;color:var(--ink-2);font-size:13px;flex-wrap:wrap}
.controls select{border:1px solid var(--hairline);background:#fff;border-radius:6px;padding:4px 10px;color:var(--ink-1);font-size:13px}.controls label{white-space:nowrap}
.tiles{display:flex;gap:10px;padding:6px 24px;flex-wrap:wrap}.tile{border:1px solid var(--hairline);border-radius:8px;padding:7px 12px;min-width:104px}
.tile .v{font-size:16px;font-weight:600;font-variant-numeric:tabular-nums}.tile .l{color:var(--ink-2);font-size:11px}.note{color:var(--ink-3);font-size:12px;padding:0 24px 8px;max-width:1500px}
h2{font-size:14px;font-weight:600;margin:16px 24px 6px}h2 span{font-weight:400;color:var(--ink-2);font-size:12px;margin-left:8px}
table{border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums;margin:0 24px}th{text-align:right;padding:4px 7px;color:var(--ink-2);font-weight:500;border-bottom:1px solid var(--hairline);cursor:pointer;white-space:nowrap;position:sticky;top:0;background:var(--surface-1);user-select:none}
td{padding:3px 7px;text-align:right;border-bottom:1px solid #f0efeb;white-space:nowrap}td.l,th.l{text-align:left}tbody tr:hover td{background:#f4f3ef;cursor:pointer}th.on{color:var(--ink-1);font-weight:600}th.over{box-shadow:inset 3px 0 0 var(--ink-2)}
tr.hl td{background:#ff0 !important}tr.sel td{font-weight:600;box-shadow:inset 3px 0 0 var(--ink-1)}
.wrap{max-height:460px;overflow:auto;margin:0 0 10px}.row{display:flex;gap:18px;padding:0 24px;flex-wrap:wrap;align-items:flex-start}.chart{height:300px;flex:1 1 440px;min-width:400px}.row .chart:nth-child(3){flex-basis:100%}
#grid td.c{min-width:48px;cursor:pointer}#grid td.c.sel{outline:2px solid var(--ink-1)}#grid th.y{text-align:left}
</style></head><body>
<header><h1>MA crossover on pooled BTC/ETH flow → long/short basket: study viewer</h1><span id="status" style="color:var(--ink-3);font-size:12px"></span><a href="/pivot">pivot viewer →</a></header>
<div class="sub" id="sub"></div>
<div class="controls"><label>target <select id="mark"></select></label><label>signal <select id="combo"></select></label><label>lag <select id="lag"></select></label><label>arm <select id="arm"></select></label><label>type <select id="typ"></select></label><label>fast <select id="fast"></select></label><label>slow <select id="slow"></select></label></div>
<div class="tiles" id="tiles"></div>
<div class="row"><div id="eq" class="chart"></div><div id="dd" class="chart"></div></div>
<div class="row"><div id="hist" class="chart"></div><div id="lag_chart" class="chart"></div></div>
<h2>Hour of day — US Eastern time<span id="hodnote">lag 1 s; a trade belongs to the hour it opened; hourly PnL = the sum of the rule's PnL in that clock hour, averaged over the days</span></h2>
<div class="row"><div id="hod_chart" class="chart"></div><div id="hod_chart3" class="chart"></div><div id="hod_chart2" class="chart"></div></div>
<div class="wrap"><table id="hod"><thead></thead><tbody></tbody></table></div>
<h2>MA pairs — fast (rows) × slow (columns), this arm, type, target, signal and lag<span>cell = annualised Sharpe; click to switch</span></h2>
<div class="wrap"><table id="grid"><thead></thead><tbody></tbody></table></div>
<h2>Arms × types — same windows, target, signal and lag<span>click a row to switch</span></h2><div class="wrap"><table id="arms"><thead></thead><tbody></tbody></table></div>
<h2>Signal source × lag — same rule and target<span>click a row to switch</span></h2><div class="wrap"><table id="signals"><thead></thead><tbody></tbody></table></div>
<h2>Targets — what is traded, same rule, signal and lag<span>click a row to switch</span></h2><div class="wrap"><table id="targets"><thead></thead><tbody></tbody></table></div>
<h2>Exchanges — this rule traded on each venue's own equal-weight basket of the alt coins<span id="vnote">lag 1 s, coins marked at that venue's mid; click a venue to filter the pairs below</span></h2><div class="wrap"><table id="venues"><thead></thead><tbody></tbody></table></div>
<h2>Pairs × exchanges — this rule per coin per venue<span id="cnote"></span></h2>
<div class="controls"><label>venue <select id="vfilter"><option value="">all</option></select></label><label>min coverage <select id="cov"><option value="0">any</option><option value="0.1">10 % of seconds</option><option value="0.3" selected>30 % of seconds</option><option value="0.6">60 % of seconds</option><option value="0.9">90 % of seconds</option></select></label><label>show <select id="topn"><option>100</option><option selected>300</option><option>1000</option><option value="100000">all</option></select></label></div>
<div class="wrap" style="max-height:620px"><table id="coins"><thead></thead><tbody></tbody></table></div>
<div class="note">All numbers gross, mid-to-mid, 1 s bars. Target marks: pooled = ALL mid across venues (unusable at 1 s), primary = each coin's top venue by notional, forward-filled mid (readable; stale on thin coins), fresh = fresh seconds only (biased long), btc / eth = the source coin itself. Signal: both = BTC and ETH agree, hold otherwise; btc / eth alone. Hit ratio = share of holding segments (flip to flip) with PnL &gt; 0 regardless of size. Per coin-per-venue rows use each venue's own forward-filled mid, so sparse series (low coverage) overstate; the coverage filter hides them. Equity and drawdown are hourly sums of the second-by-second PnL.</div>
<script>
const $=id=>document.getElementById(id);let META=null,CELL=null;const DIMS=['mark','combo','lag','arm','typ','fast','slow'];
const fmt={f0:v=>v==null?'–':v.toFixed(0),f1:v=>v==null?'–':v.toFixed(1),f2:v=>v==null?'–':v.toFixed(2),f3:v=>v==null?'–':v.toFixed(3),i:v=>v==null?'–':Math.round(v).toLocaleString(),s:v=>v==null?'–':v,n:v=>v==null?'–':v,pct:v=>v==null?'–':(100*v).toFixed(0)+' %'};
const TS={};
function table(id,cols,data,o){
 const st=TS[id]||(TS[id]={order:cols.map(c=>c[0]),key:o.key,desc:o.desc!==false});const left=o.left||[];
 const C=st.order.map(k=>cols.find(c=>c[0]===k)).filter(Boolean);
 const R=data.slice().sort((a,b)=>{const x=a[st.key],y=b[st.key];if(x==null&&y==null)return 0;if(x==null)return 1;if(y==null)return -1;return(typeof x==='string'||typeof y==='string'?String(x).localeCompare(String(y)):x-y)*(st.desc?-1:1)});
 const t=$(id);
 t.querySelector('thead').innerHTML='<tr>'+C.map(([k,l])=>`<th draggable="true" data-k="${k}" class="${left.includes(k)?'l ':''}${k===st.key?'on':''}">${l}${k===st.key?(st.desc?' ▾':' ▴'):''}</th>`).join('')+'</tr>';
 t.querySelector('tbody').innerHTML=R.length?R.map(e=>`<tr ${o.rowAttr?o.rowAttr(e):''} class="${o.rowClass?o.rowClass(e):''}">`+C.map(([k,l,f])=>`<td class="${left.includes(k)?'l':''}">${fmt[f](e[k])}</td>`).join('')+'</tr>').join(''):`<tr><td colspan="${C.length}" class="l">${o.empty||'–'}</td></tr>`;
 t.querySelectorAll('th').forEach(th=>{
  th.onclick=()=>{const k=th.dataset.k;if(k===st.key)st.desc=!st.desc;else{st.key=k;st.desc=!left.includes(k)}table(id,cols,data,o)};
  th.ondragstart=ev=>{ev.dataTransfer.setData('text/plain',th.dataset.k);ev.dataTransfer.effectAllowed='move'};
  th.ondragover=ev=>{ev.preventDefault();th.classList.add('over')};th.ondragleave=()=>th.classList.remove('over');
  th.ondrop=ev=>{ev.preventDefault();const from=ev.dataTransfer.getData('text/plain'),to=th.dataset.k;if(!from||from===to)return;const i=st.order.indexOf(from),j=st.order.indexOf(to);const ord=st.order.filter(k=>k!==from);ord.splice(ord.indexOf(to)+(i<j?1:0),0,from);st.order=ord;table(id,cols,data,o)}});
 t.querySelectorAll('tbody tr').forEach(r=>r.onclick=ev=>{if(ev.shiftKey){r.classList.toggle('hl');return}if(o.onRow)o.onRow(r)});}
async function get(u){const r=await fetch(u);if(!r.ok)throw new Error(u+' '+r.status);return r.json()}
const state=()=>Object.fromEntries(DIMS.map(d=>[d,$(d).value]));const rule=()=>`${$('arm').value}:${$('typ').value}:${$('fast').value}:${$('slow').value}`;
function hash(){const st=state();st.run=$('status').dataset.run;history.replaceState(null,'','#'+new URLSearchParams(st).toString())}
function fill(id,vals,cur){$(id).innerHTML=vals.map(v=>`<option value="${v}">${v}</option>`).join('');if(cur!=null&&vals.map(String).includes(String(cur)))$(id).value=cur}
async function init(){const H=new URLSearchParams(location.hash.slice(1));const run=H.get('run')||'__RUN__';$('status').dataset.run=run;$('status').textContent='loading …';
 META=await get('/meta?run='+encodeURIComponent(run));
 $('sub').innerHTML=`Run <b>${META.run_id}</b>, ${META.dates[0]} → ${META.dates[META.dates.length-1]} (${META.dates.length} days), basket of ${META.coins} coins; ${META.windows.length} MA windows (${META.windows[0]} … ${META.windows[META.windows.length-1].toLocaleString()} bars), ${META.windows.length*(META.windows.length-1)/2} fast &lt; slow pairs per arm × type; the state is sign(fast − slow) sampled every second, BTC and ETH pooled across every venue and quote.`;
 fill('mark',['primary','pooled','fresh','btc','eth'].filter(m=>META.marks.includes(m)),H.get('mark')||'primary');fill('combo',META.combos,H.get('combo')||'both');fill('lag',META.lags_s,H.get('lag')||'1');
 fill('arm',Object.keys(META.arms),H.get('arm')||'imb_usd');fillTyp(H.get('typ'));fill('fast',META.windows.slice(0,-1),H.get('fast')||'30');fillSlow(H.get('slow')||'40');
 DIMS.forEach(d=>$(d).onchange=()=>{if(d==='arm')fillTyp();if(d==='fast')fillSlow();load()});load();}
function fillTyp(cur){const ts=META.arms[$('arm').value];fill('typ',ts,cur||(ts.includes($('typ').value)?$('typ').value:ts[0]))}
function fillSlow(cur){const f=+$('fast').value;const ss=META.windows.filter(w=>w>f);fill('slow',ss,cur||(ss.includes(+$('slow').value)?$('slow').value:ss[0]))}
function setSel(o){for(const k in o)if($(k)&&String($(k).value)!==String(o[k])){$(k).value=o[k];if(k==='arm')fillTyp(o.typ);if(k==='fast')fillSlow(o.slow)}}
async function load(){hash();const st=state();$('status').textContent='loading cell …';
 try{CELL=await get(`/cell?run=${encodeURIComponent($('status').dataset.run)}&mark=${st.mark}&combo=${st.combo}&lag=${st.lag}&rule=${encodeURIComponent(rule())}`)}catch(e){$('status').textContent='failed: '+e.message;return}
 if(CELL.error){$('status').textContent=CELL.error;return}$('status').textContent='';render()}
function cum(a){let s=0;return a.map(v=>s+=(v||0))}
function render(){const c=CELL.cell,b=CELL.baseline||{};
 $('tiles').innerHTML=[[fmt.f2(c.sharpe_ann),'Sharpe (ann.)'],[fmt.f0(c.total_bp)+' bp','total, 28 days'],[fmt.f1(c.daily_mean_bp)+' / '+fmt.f1(c.daily_median_bp),'mean / median daily bp'],[fmt.f3(c.hit_rate),'hit ratio (per trade)'],[fmt.f3(c.mean_trade_bp)+' / '+fmt.f3(c.median_trade_bp)+' bp','mean / median PnL per trade'],[fmt.f0(c.flips_per_day+1/ (c.n_days||28)),'trades / day'],[fmt.f0(c.median_hold_s)+' s','median hold'],[fmt.f2(c.breakeven_bp_per_flip)+' vs '+fmt.f2(b.half_spread_bp)+' bp','break-even / flip vs half-spread'],[fmt.pct(c.pos_day_share),'days > 0'],[fmt.f0(c.max_drawdown_bp)+' bp','max drawdown'],[fmt.pct(c.long_share),'time long'],[fmt.f3(c.p_value),'p (shift null)'],[fmt.f1(b.sharpe_ann)+' / '+fmt.f0(b.total_bp)+' bp','always-long: Sharpe / total']]
  .map(([v,l])=>`<div class="tile"><div class="v">${v}</div><div class="l">${l}</div></div>`).join('');
 const lay=(t,x,y)=>({title:{text:t,font:{size:13}},margin:{l:55,r:15,t:34,b:45},paper_bgcolor:'#fcfcfb',plot_bgcolor:'#fff',font:{size:11},legend:{orientation:'h',y:-0.25},hovermode:'x unified',xaxis:{title:x},yaxis:{title:y}});
 if(CELL.hourly&&CELL.hourly.hour.length){const h=CELL.hourly.hour.map(v=>v/24),e=cum(CELL.hourly.pnl_bp),a=cum(CELL.hourly.always_long_bp);
  Plotly.newPlot('eq',[{x:h,y:e,mode:'lines',name:'rule',line:{color:'#2a78d6',width:2}},{x:h,y:a,mode:'lines',name:'always-long',line:{color:'#8a8a86',width:1.5}}],lay('cumulative PnL, bp (hourly)','day','bp'),{displaylogo:false,responsive:true});
  const dd=x=>{let pk=-1e18;return x.map(v=>{pk=Math.max(pk,v);return v-pk})};
  Plotly.newPlot('dd',[{x:h,y:dd(e),mode:'lines',name:'rule',fill:'tozeroy',line:{color:'#eb6834',width:1.5}},{x:h,y:dd(a),mode:'lines',name:'always-long',line:{color:'#8a8a86',width:1}}],lay('drawdown from peak, bp','day','bp'),{displaylogo:false,responsive:true});}
 if(CELL.hist&&CELL.hist.length){const lab=CELL.hist.map(b=>b.lo==null?`< ${b.hi}`:b.hi==null?`≥ ${b.lo}`:`${b.lo}…${b.hi}`);const n=CELL.hist.reduce((s,b)=>s+b.count,0);
  Plotly.newPlot('hist',[{x:lab,y:CELL.hist.map(b=>100*b.count/n),type:'bar',marker:{color:CELL.hist.map(b=>(b.hi!=null&&b.hi<=0)?'#eb6834':'#2a78d6')}}],Object.assign(lay(`per-trade PnL distribution (${n.toLocaleString()} trades), % of trades`,'bp per trade (bins, log-spaced)','% of trades'),{xaxis:{tickangle:-45}}),{displaylogo:false,responsive:true});}
 if(CELL.lagprofile&&CELL.lagprofile.lag_s.length)Plotly.newPlot('lag_chart',[{x:CELL.lagprofile.lag_s,y:CELL.lagprofile.total_bp,mode:'lines+markers',line:{color:'#2a78d6',width:2},marker:{size:8}}],Object.assign(lay('total PnL by execution lag (agreement signal)','lag s (log)','bp'),{xaxis:{type:'log',title:'lag s (log)'}}),{displaylogo:false,responsive:true});
 const HD=CELL.hod||[];$('hodnote').textContent=HD.length?'lag 1 s, US Eastern clock (daylight time in September); a trade belongs to the hour it opened; mean hourly PnL = the rule\'s PnL in that clock hour averaged over the days':'no hour-of-day table in this run yet';
 if(HD.length){const hl=HD.map(r=>`${String(r.hod_et).padStart(2,'0')}:00`);const lay2=(t,y)=>({title:{text:t,font:{size:13}},margin:{l:55,r:15,t:34,b:45},paper_bgcolor:'#fcfcfb',plot_bgcolor:'#fff',font:{size:11},showlegend:false,xaxis:{title:'hour of day, ET',tickangle:-45},yaxis:{title:y}});
  Plotly.newPlot('hod_chart',[{x:hl,y:HD.map(r=>r.mean_hour_bp),type:'bar',marker:{color:HD.map(r=>r.mean_hour_bp>=0?'#2a78d6':'#eb6834')},hovertemplate:'%{y:.1f} bp'}],lay2('mean PnL per clock hour, bp (averaged over days)','bp per hour'),{displaylogo:false,responsive:true});
  Plotly.newPlot('hod_chart3',[{x:hl,y:HD.map(r=>r.mean_trade_bp),type:'bar',name:'mean bp / trade',marker:{color:HD.map(r=>r.mean_trade_bp>=0?'#2a78d6':'#eb6834')},hovertemplate:'%{y:.3f} bp'},{x:hl,y:HD.map(r=>r.median_trade_bp),type:'scatter',mode:'lines+markers',name:'median bp / trade',line:{color:'#1f1f1e',width:2},marker:{size:6},hovertemplate:'%{y:.3f} bp'}],Object.assign(lay2('per-trade return by hour: mean (bars) and median (line), bp','bp per trade'),{showlegend:true,legend:{orientation:'h',y:-0.3}}),{displaylogo:false,responsive:true});
  Plotly.newPlot('hod_chart2',[{x:hl,y:HD.map(r=>r.hit_rate),type:'bar',marker:{color:'#2a78d6'},name:'hit ratio',hovertemplate:'%{y:.3f}'},{x:hl,y:HD.map(r=>r.median_trade_bp),type:'scatter',mode:'lines+markers',yaxis:'y2',name:'median bp / trade',line:{color:'#eb6834',width:2},hovertemplate:'%{y:.3f} bp'}],Object.assign(lay2('hit ratio (bars) and median PnL per trade (line) by hour','hit ratio'),{yaxis2:{title:'median bp / trade',overlaying:'y',side:'right'},showlegend:true,legend:{orientation:'h',y:-0.3}}),{displaylogo:false,responsive:true});}
 else{Plotly.purge('hod_chart');Plotly.purge('hod_chart2');Plotly.purge('hod_chart3')}
 table('hod',[['hod_et','hour (ET)','n'],['n_hours','hours','n'],['mean_hour_bp','mean bp / hour','f1'],['share_hours_pos','hours > 0','f2'],['n_trades','trades','i'],['hit_rate','hit ratio','f3'],['trades_bp','trades PnL bp','f0'],['mean_trade_bp','mean bp / trade','f3'],['median_trade_bp','median bp / trade','f3']],HD,{key:'hod_et',desc:false,empty:'no hour-of-day table in this run yet'});
 // pairs grid
 const P=CELL.pairs,fs=[...new Set(P.map(r=>r.fast))].sort((a,b)=>a-b),ss=[...new Set(P.map(r=>r.slow))].sort((a,b)=>a-b);const by={};P.forEach(r=>by[r.fast+'|'+r.slow]=r);let mx=0;P.forEach(r=>{if(r.sharpe_ann!=null)mx=Math.max(mx,Math.abs(r.sharpe_ann))});
 const col=v=>{if(v==null)return'';const a=Math.min(1,Math.abs(v)/(mx||1))*0.85;return v>=0?`background:rgba(42,120,214,${a.toFixed(3)});color:${a>0.55?'#fff':'inherit'}`:`background:rgba(235,104,52,${a.toFixed(3)});color:${a>0.55?'#fff':'inherit'}`};
 const g=$('grid');g.querySelector('thead').innerHTML=`<tr><th class="y">fast \\ slow</th>${ss.map(s=>`<th>${s}</th>`).join('')}</tr>`;
 g.querySelector('tbody').innerHTML=fs.map(f=>`<tr><th class="y">${f}</th>${ss.map(s=>{const r=by[f+'|'+s];return r?`<td class="c ${r.rule===c.rule?'sel':''}" data-f="${f}" data-s="${s}" style="${col(r.sharpe_ann)}" title="total ${fmt.f0(r.total_bp)} bp, hit ${fmt.f3(r.hit_rate)}, ${fmt.f0(r.flips_per_day)} flips/day">${fmt.f1(r.sharpe_ann)}</td>`:'<td></td>'}).join('')}</tr>`).join('');
 g.querySelectorAll('td.c').forEach(td=>td.onclick=()=>{setSel({fast:td.dataset.f,slow:td.dataset.s});load()});
 const M=[['sharpe_ann','Sharpe','f2'],['total_bp','total bp','f0'],['daily_mean_bp','mean daily bp','f1'],['hit_rate','hit ratio','f3'],['mean_trade_bp','mean bp/trade','f3'],['median_trade_bp','median bp/trade','f3'],['flips_per_day','flips/day','f0'],['median_hold_s','hold s','f0'],['breakeven_bp_per_flip','break-even bp/flip','f2'],['pos_day_share','days > 0','f2'],['max_drawdown_bp','max DD bp','f0'],['long_share','long share','f2'],['p_value','p','f3']];
 const isSel=e=>e.rule===c.rule&&e.combo===c.combo&&e.lag_s===c.lag_s&&e.mark===c.mark?'sel':'';
 table('arms',[['arm','arm','s'],['typ','type','s']].concat(M),CELL.arms,{key:'sharpe_ann',left:['arm','typ'],rowClass:isSel,rowAttr:e=>`data-arm="${e.arm}" data-typ="${e.typ}"`,onRow:r=>{setSel({arm:r.dataset.arm,typ:r.dataset.typ});load()}});
 table('signals',[['combo','signal','s'],['lag_s','lag s','n']].concat(M),CELL.signals,{key:'sharpe_ann',left:['combo'],rowClass:isSel,rowAttr:e=>`data-combo="${e.combo}" data-lag="${e.lag_s}"`,onRow:r=>{setSel({combo:r.dataset.combo,lag:r.dataset.lag});load()}});
 table('targets',[['mark','target / mark','s']].concat(M),CELL.targets,{key:'sharpe_ann',left:['mark'],rowClass:isSel,rowAttr:e=>`data-mark="${e.mark}"`,onRow:r=>{setSel({mark:r.dataset.mark});load()}});
 const V=CELL.venues||[];$('vnote').textContent=V.length?'lag 1 s, every alt coin that trades on the venue, marked at that venue\'s own mid; click a venue to filter the pairs below':'no per-venue table in this run yet';
 table('venues',[['venue','venue','s'],['n_coins','coins (avg)','f0'],['sharpe_ann','Sharpe','f2'],['total_bp','total bp','f0'],['daily_mean_bp','mean daily bp','f1'],['hit_rate','hit ratio','f3'],['flips_per_day','flips/day','f0'],['median_hold_s','hold s','f0'],['breakeven_bp_per_flip','break-even bp/flip','f2'],['pos_day_share','days > 0','f2'],['max_drawdown_bp','max DD bp','f0']],V,{key:'sharpe_ann',left:['venue'],rowClass:e=>e.venue===$('vfilter').value?'sel':'',rowAttr:e=>`data-v="${e.venue}"`,onRow:r=>{$('vfilter').value=$('vfilter').value===r.dataset.v?'':r.dataset.v;coins()}});
 const vs=[...new Set((CELL.coins||[]).map(e=>e.venue))].sort();const cur=$('vfilter').value;$('vfilter').innerHTML='<option value="">all</option>'+vs.map(v=>`<option value="${v}">${v}</option>`).join('');if(vs.includes(cur))$('vfilter').value=cur;coins();}
function coins(){const N=META.dates.length*86400,v=$('vfilter').value,cov=+$('cov').value,top=+$('topn').value;const X=(CELL.coins||[]).map(e=>Object.assign({coverage:e.secs_with_value/N},e)).filter(e=>(!v||e.venue===v)&&e.coverage>=cov);
 $('cnote').textContent=`${X.length} coin × venue series${v?' on '+v:''} with ≥ ${(100*cov).toFixed(0)} % coverage (of ${(CELL.coins||[]).length}); lag 1 s, PnL in bp of that series; sorted rows show the top ${top}`;
 table('coins',[['coin','coin','s'],['venue','venue','s'],['sharpe_ann','Sharpe (daily)','f2'],['total_bp','total bp','f0'],['days_up','days > 0','f2'],['coverage','coverage','pct']],X.slice().sort((a,b)=>((b.sharpe_ann==null?-1e9:b.sharpe_ann)-(a.sharpe_ann==null?-1e9:a.sharpe_ann))).slice(0,top),{key:'sharpe_ann',left:['coin','venue'],empty:'no per coin × venue cube in this run yet'});
 if(TS.coins)TS.coins.key=TS.coins.key||'sharpe_ann';}
['vfilter','cov','topn'].forEach(id=>$(id).onchange=coins);init();
</script></body></html>"""


# ----------------------------------------------------------------------------------------------- server
class Handler(BaseHTTPRequestHandler):
    run_id = None

    def _send(self, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else body.encode(); self.send_response(200); self.send_header("Content-Type", ctype + "; charset=utf-8"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path); q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}; run = q.get("run") or self.run_id
        try:
            if u.path == "/": return self._send(STUDY.replace("__RUN__", self.run_id), "text/html")
            if u.path == "/pivot": return self._send(PAGE.replace("__RUN__", self.run_id), "text/html")
            if u.path == "/cell":
                T = {k: _summary(run, k) for k in ("rules", "daily", "hourly", "hist", "lagprofile", "venues", "cube_index", "baseline", "hod")}
                return self._send(json.dumps(cell_doc(T, None, q["mark"], q["combo"], q["lag"], q["rule"]), separators=(",", ":")))
            if u.path == "/meta": return self._send(json.dumps(meta_doc(_plan(run), _summary(run, "baseline"), _done(run))))
            if u.path == "/rules": return self._send(json.dumps(rules_doc(_summary(run, "rules"), _summary(run, "daily")), separators=(",", ":")))
            if u.path == "/daily": return self._send(json.dumps(daily_doc(_summary(run, "daily"), q["rule"], q["combo"], q["lag"], q["mark"])))
            if u.path == "/lagprofile": return self._send(json.dumps(lagprofile_doc(_summary(run, "lagprofile"), q["rule"], q["mark"])))
            if u.path == "/coins": return self._send(json.dumps(coins_doc(_summary(run, "coins"), q["rule"], q["mark"])))
            self.send_response(404); self.end_headers()
        except Exception as e:
            self.send_response(500); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(json.dumps({"message": f"{e.__class__.__name__}: {str(e)[:300]}"}).encode())

    def log_message(self, fmt, *args): print(f"{self.address_string()} {fmt % args}", file=sys.stderr, flush=True)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--run-id", default=None); ap.add_argument("--port", type=int, default=8781); a = ap.parse_args(); a.run_id = a.run_id or settings.get("runs", "default")
    Handler.run_id = a.run_id; srv = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    print(f"MA crossover viewer on http://127.0.0.1:{a.port}/  run {a.run_id}", file=sys.stderr, flush=True); srv.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
