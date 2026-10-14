"""Report helpers vendored from the quant-research repo: page CSS, the headless Chrome path used to print the PDF, the six-field table title, the HTML
table with optional heat tints, and TABLE_JS (every table sorts on header click, columns move by drag, a clicked row highlights)."""
import html
import numpy as np

def chrome():
    """The Chrome / Chromium binary for the headless PDF print (config paths.chrome)."""
    import settings; return settings.get("paths", "chrome")
_nan = lambda v: v is None or (isinstance(v, float) and np.isnan(v))
HEAT_RED, HEAT_GREEN, HEAT_ALPHA = (214, 69, 65), (46, 160, 67), 0.28
CSS = """body{font-family:-apple-system,Helvetica,Arial,sans-serif;color:#1c1c1c;font-size:9.5pt;margin:28px 34px}h1{font-size:15pt;margin:0 0 4px}h2{font-size:11.5pt;margin:18px 0 4px}h3{font-size:10pt;margin:12px 0 3px}
p{line-height:1.4;margin:4px 0 8px}.meta{color:#666;font-size:8.5pt}table{border-collapse:collapse;width:100%;font-size:7.4pt}th{text-align:right;font-weight:600;color:#444;border-bottom:1px solid #999;padding:2pt 3pt;white-space:nowrap}
td{text-align:right;padding:1.8pt 3pt;border-bottom:1px solid #e6e6e6;white-space:nowrap}td.l,th:first-child{text-align:left}tr:nth-child(even) td{background:#fafafa}.note{color:#555;font-size:8.2pt}
table.mid{width:auto;font-size:8.2pt}table.mid td,table.mid th{padding:2pt 7pt}.pb{page-break-before:always}ul{margin:4px 0 8px 18px;line-height:1.45}@page{size:A4 landscape;margin:12mm}"""




def heat_scales(rows, cols):
    """Per heat group (a column's 5th element, default its key) the p95 of |v| over the table's finite values: the tint saturates there, so one
    scale is shared by the columns of one group (the five quantile columns). Groups with no finite non-zero value get None (no tint)."""
    vals = {}
    for c in cols:
        if len(c) > 3 and c[3]:
            vals.setdefault(c[4] if len(c) > 4 else c[0], []).extend(abs(float(r.get(c[0]))) for r in rows if not _nan(r.get(c[0])))
    return {g: (float(np.percentile(v, 95)) or None) for g, v in vals.items() if v}



def heat_style(v, kind, scale):
    """Inline background for one cell: kind "up" = positive green / negative red, kind "down" = the reverse (a cost); none for 0, null or no scale."""
    if _nan(v) or not kind or not scale or v == 0: return ""
    good = (v > 0) == (kind == "up"); rgb = HEAT_GREEN if good else HEAT_RED; a = min(abs(v) / scale, 1.0) * HEAT_ALPHA
    return f" style='background:rgba({rgb[0]},{rgb[1]},{rgb[2]},{a:.3f})'"



def title(num, scope, edge, selection, sort, top, order="desc", pb=True):
    """Every table title has the same six fields: Table N.M | scope | edge | selection | sort <key> <desc|asc> | <rows shown>; a page break before it unless pb=False."""
    return f"<h2 class='{'pb' if pb else ''}'>Table {num} | {scope} | {edge} | {selection} | sort {sort} {order} | {top}</h2>"



def table_html(rows, cols, cls=""):
    """HTML table; cols = (key, label[, fmt[, heat kind "up" | "down"[, heat group]]]). The first column and unformatted string columns are
    left-aligned (cells and header); heat cells are tinted per heat_style with heat_scales over the table."""
    sc = heat_scales(rows, cols); left = [i == 0 or (len(c) == 2 and any(isinstance(r.get(c[0]), str) for r in rows)) for i, c in enumerate(cols)]
    h = "<table class='%s'><thead><tr>" % cls + "".join(f"<th class='{'l' if l else ''}'>{html.escape(c[1])}</th>" for c, l in zip(cols, left)) + "</tr></thead><tbody>"
    for r in rows:
        tds = []
        for i, c in enumerate(cols):
            v = r.get(c[0]); txt = c[2](v) if len(c) > 2 else ("–" if v is None else html.escape(str(v)))
            st = heat_style(v, c[3], sc.get(c[4] if len(c) > 4 else c[0])) if len(c) > 3 else ""
            tds.append(f"<td class='{'l' if left[i] else ''}'{st}>{txt}</td>")
        h += "<tr>" + "".join(tds) + "</tr>"
    return h + "</tbody></table>"


# Behaviour for every <table> of a report page (PDF twin): header click sorts the rows (numeric-aware from the cell text, "–" last, toggles
# direction, ▾/▴ on the active header) and a header can be dragged onto another to move the whole column. Nothing changes until the reader
# interacts, so the printed PDF is identical; include it once before </body>.


TABLE_JS = """<style>tr.hl td{background:#ff0 !important}</style><script>
(function(){
 document.querySelectorAll('tbody').forEach(b=>b.addEventListener('click',e=>{const tr=e.target.closest('tr');if(tr&&!e.target.closest('a'))tr.classList.toggle('hl')}));
 const NUL=new Set(['','–','-','n/a','NA']);
 const val=td=>{const s=td.textContent.trim();if(NUL.has(s))return null;const m=s.replace(/[,%$\\s‡]/g,'').match(/^([-+]?\\d*\\.?\\d+(?:e[-+]?\\d+)?)([kMB])?$/i);
  if(!m)return s;const v=parseFloat(m[1]),u={k:1e3,m:1e6,b:1e9}[(m[2]||'').toLowerCase()];return u?v*u:v};
 const cmp=(x,y,desc)=>{if(x==null||y==null)return x==null?(y==null?0:1):-1;return(typeof x==='number'&&typeof y==='number'?x-y:String(x).localeCompare(String(y),undefined,{numeric:true}))*(desc?-1:1)};
 document.querySelectorAll('table').forEach(t=>{
  const head=t.tHead&&t.tHead.rows[0],body=t.tBodies[0];if(!head||!body)return;
  const ths=()=>Array.from(head.cells);
  ths().forEach(th=>{th.dataset.label=th.textContent;th.title="click: sort · drag: move column · click a row: highlight it";th.style.cursor='pointer';th.draggable=true});
  let key=-1,desc=true;
  const mark=()=>ths().forEach((th,i)=>{th.textContent=th.dataset.label+(i===key?(desc?' ▾':' ▴'):'')});
  head.addEventListener('click',e=>{const th=e.target.closest('th');if(!th)return;const i=ths().indexOf(th);
   const rows=Array.from(body.rows),vals=rows.map(r=>val(r.cells[i]));const num=vals.some(v=>typeof v==='number');
   if(i===key)desc=!desc;else{key=i;desc=num}
   rows.map((r,j)=>[r,vals[j]]).sort((a,b)=>cmp(a[1],b[1],desc)).forEach(([r])=>body.appendChild(r));mark()});
  let from=-1;
  head.addEventListener('dragstart',e=>{const th=e.target.closest('th');from=th?ths().indexOf(th):-1;e.dataTransfer.effectAllowed='move'});
  head.addEventListener('dragover',e=>{e.preventDefault();const th=e.target.closest('th');ths().forEach(h=>h.style.boxShadow=h===th?'inset 3px 0 0 #444':'');});
  head.addEventListener('dragleave',()=>ths().forEach(h=>h.style.boxShadow=''));
  head.addEventListener('drop',e=>{e.preventDefault();ths().forEach(h=>h.style.boxShadow='');const th=e.target.closest('th');const to=th?ths().indexOf(th):-1;if(from<0||to<0||from===to)return;
   Array.from(t.rows).forEach(r=>{const c=r.cells;if(c.length<=Math.max(from,to))return;r.insertBefore(c[from],from<to?c[to].nextSibling:c[to])});
   if(key===from)key=to;else if(from<key&&key<=to)key--;else if(to<=key&&key<from)key++;mark();from=-1});
 });
})();
</script>"""


