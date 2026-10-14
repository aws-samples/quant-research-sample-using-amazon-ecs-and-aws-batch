"""Account-specific settings for the imbalance_mft sample.

This is the ONLY place a bucket, region, profile, queue or job definition enters the code: modules call
`settings.get("s3", "data_bucket")` at the point of use. The config is `config.json` next to this file (or one
directory up). Values that still look like a placeholder (`<your-bucket>`) are refused with a SettingsError
naming the key, so a half-filled config fails loudly and early. Tests point at a fake config with `use()`.
"""
import json
import re
from pathlib import Path
from typing import Any, Optional

SAMPLE_CONFIG = "config.json"
_PLACEHOLDER = re.compile(r"^<.*>$")


class SettingsError(RuntimeError):
    """A required setting is missing, still a placeholder, or no config exists."""


_anchor: Path = Path(__file__).resolve()
_path: Optional[Path] = None
_data: Optional[dict] = None


def reset(anchor: Optional[Path] = None) -> None:
    global _anchor, _path, _data
    _anchor = Path(anchor).resolve() if anchor else Path(__file__).resolve(); _path, _data = None, None


def use(path: Path) -> None:
    """Point at an explicit config file (tests only)."""
    global _path, _data
    _path = Path(path).resolve(); _data = None


def _discover() -> Path:
    for base in (_anchor.parent, _anchor.parent.parent):
        p = base / SAMPLE_CONFIG
        if p.is_file(): return p
    raise SettingsError(f"no {SAMPLE_CONFIG} next to {_anchor.parent} or one level up")


def _load() -> dict:
    global _path, _data
    if _data is None:
        if _path is None: _path = _discover()
        _data = json.loads(_path.read_text())
    return _data


def get(*keys: str) -> Any:
    """Nested lookup; None is a legal value; placeholders and missing keys raise."""
    node: Any = _load(); dotted = ".".join(keys)
    for k in keys:
        if not isinstance(node, dict) or k not in node: raise SettingsError(f"setting {dotted} is missing from {_path}")
        node = node[k]
    if isinstance(node, str) and _PLACEHOLDER.match(node): raise SettingsError(f"setting {dotted} in {_path} is still the placeholder {node}")
    return node
