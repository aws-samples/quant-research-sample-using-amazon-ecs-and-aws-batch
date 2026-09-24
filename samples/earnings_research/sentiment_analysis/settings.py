"""Account-specific settings for the earnings research packages.

Every package carries a byte-identical copy of this file. It is the ONLY place
bucket names, account ids, queue names, secret ARNs and the like enter the
code: modules call `settings.get("s3", "data_bucket")` at the point of use.

Resolution (first hit wins, nothing is read at import time):
  <pkg>/../config.json                      # exported sample layout
  <pkg>/../earnings_research.config.json    # internal repo layout
  <pkg>/../../config.json                   # packages one level deeper (tools/*)
  <pkg>/../../earnings_research.config.json

Values that still look like a placeholder (`<your-bucket>`) are refused with a
SettingsError naming the key, so a half-filled config fails loudly and early.
"""
import json
import re
from pathlib import Path
from typing import Any, Optional

SAMPLE_CONFIG = "config.json"
INTERNAL_CONFIG = "earnings_research.config.json"
_PLACEHOLDER = re.compile(r"^<.*>$")

# Logical package name -> directory relative to the config's directory.
# In the exported sample the directory IS the logical name.
_INTERNAL_LAYOUT = {
    "basket_study": "earnings_basket_study",
    "market_data": "earnings_market_data",
    "content_pipeline": "earnings_content_pipeline",
    "sentiment_analysis": "earnings_sentiment_analysis",
    "bedrock_inference": "aws_batch_inference_bedrock",
    "data_catalog_mcp": "mcp/data_catalog_mcp",
    "viewer": "tools/results_browser",
    "index_constituents": "scripts",
}


class SettingsError(RuntimeError):
    """A required setting is missing, still a placeholder, or no config exists."""


_anchor: Path = Path(__file__).resolve()
_path: Optional[Path] = None
_data: Optional[dict] = None


def reset(anchor: Optional[Path] = None) -> None:
    """Forget any loaded config. `anchor` overrides where the upward search
    starts (tests only; production code never passes it)."""
    global _anchor, _path, _data
    _anchor = Path(anchor).resolve() if anchor else Path(__file__).resolve()
    _path, _data = None, None


def use(path: Path) -> None:
    """Point at an explicit config file (tests only)."""
    global _path, _data
    _path = Path(path).resolve()
    _data = None


def loaded_path() -> Optional[Path]:
    return _path


def _discover() -> Path:
    pkg_dir = _anchor.parent
    for base in (pkg_dir.parent, pkg_dir.parent.parent):
        for name in (SAMPLE_CONFIG, INTERNAL_CONFIG):
            candidate = base / name
            if candidate.is_file():
                return candidate
    raise SettingsError(
        f"no {SAMPLE_CONFIG} or {INTERNAL_CONFIG} found above {pkg_dir}; "
        "copy the sample config.json next to the packages and fill it in")


def _load() -> dict:
    global _path, _data
    if _data is None:
        if _path is None:
            _path = _discover()
        _data = json.loads(_path.read_text())
    return _data


def root() -> Path:
    """Directory holding the config file (the packages' common parent)."""
    _load()
    assert _path is not None
    return _path.parent


def get(*keys: str) -> Any:
    """Nested lookup; None is a legal value; placeholders and missing keys raise."""
    node: Any = _load()
    dotted = ".".join(keys)
    for k in keys:
        if not isinstance(node, dict) or k not in node:
            raise SettingsError(f"setting {dotted} is missing from {_path}")
        node = node[k]
    if isinstance(node, str) and _PLACEHOLDER.match(node):
        raise SettingsError(f"setting {dotted} is still a placeholder ({node}) in {_path}")
    return node


def sibling(name: str) -> Path:
    """Directory of another package of this program, in whichever layout is present."""
    if name not in _INTERNAL_LAYOUT:
        raise SettingsError(f"unknown package {name!r}; known: {sorted(_INTERNAL_LAYOUT)}")
    base = root()
    if _path is not None and _path.name == INTERNAL_CONFIG:
        return base / _INTERNAL_LAYOUT[name]
    return base / name
