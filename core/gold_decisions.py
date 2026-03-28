"""Load persisted manual gold decisions (source per logical file) from JSON."""

from __future__ import annotations

import os
from typing import Dict, Optional

from core.storage import load_json, save_json


def gold_decision_key(domain_id: str, relative_path: str) -> str:
    return f"{domain_id}::{relative_path}"


def load_gold_map(path: str) -> Dict[str, str]:
    """Return map of 'domain_id::relative_path' -> gold source_id. Missing file => empty map."""
    if not path or not os.path.isfile(path):
        return {}
    raw = load_json(path)
    entries = raw.get("entries")
    if not isinstance(entries, dict):
        return {}
    return {str(k): str(v) for k, v in entries.items() if v is not None and str(v).strip() != ""}


def gold_source_for_file(gold_map: Dict[str, str], domain_id: str, relative_path: str) -> Optional[str]:
    sid = gold_map.get(gold_decision_key(domain_id, relative_path))
    return sid if sid else None


def save_gold_map(path: str, gold_map: Dict[str, str]) -> None:
    """Persist entire gold map (replaces file contents)."""
    save_json(path, {"entries": dict(sorted(gold_map.items()))})
