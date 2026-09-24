"""Box-drawing table dump — the user's preferred on-screen format for
per-event result tables. Grid lines between every row, one cell per value,
raw values (no rounding/interpretation)."""

import pandas as pd


def box_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    vals = [[("" if pd.isna(v) else str(v)) for v in row] for _, row in df.iterrows()]
    widths = [max(len(c), *(len(r[i]) for r in vals)) if vals else len(c)
              for i, c in enumerate(cols)]

    def line(l, m, r):
        return l + m.join("─" * (w + 2) for w in widths) + r

    def row(cells):
        return "│ " + " │ ".join(c.ljust(w) for c, w in zip(cells, widths)) + " │"

    hdr = "│ " + " │ ".join(c.center(w) for c, w in zip(cols, widths)) + " │"
    out = [line("┌", "┬", "┐"), hdr, line("├", "┼", "┤")]
    for i, r in enumerate(vals):
        out.append(row(r))
        out.append(line("├", "┼", "┤") if i < len(vals) - 1 else line("└", "┴", "┘"))
    return "\n".join(out)
