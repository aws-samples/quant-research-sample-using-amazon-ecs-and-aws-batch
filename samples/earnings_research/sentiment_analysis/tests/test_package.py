"""Package hygiene: copies importable; the gap concept is GONE."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_copies_import():
    import rules.sessions as ss
    import construction.base as cb
    import construction.ridge as cr
    import scores
    assert callable(ss.consolidate) and callable(ss.entry_price)
    assert callable(cr.build) and hasattr(cb, "BasketContext")
    assert callable(scores.load_model_list)


def test_gap_concept_is_gone():
    import rules.sessions as ss
    assert not hasattr(ss, "previous_close")
    assert not hasattr(ss, "signal_price")
    assert not hasattr(ss, "SIGNAL_MIN")


def test_no_gap_strings_anywhere():
    root = Path(__file__).resolve().parent.parent
    offenders = []
    for p in root.rglob("*.py"):
        if "tests" in p.parts:
            continue
        text = p.read_text()
        for needle in ("previous_close", "signal_price"):
            if needle in text:
                offenders.append((str(p), needle))
    assert not offenders, offenders
