"""
Load GMCAssist wizard definition JSON files from ``services/wizard_defs``.
"""

import glob
import os
from typing import Any, Dict, List, Optional

from core.storage import load_json
from services.paths import project_root


def wizard_defs_dir(settings: Dict[str, Any]) -> str:
    rel = settings.get("gmcassist_wizard_defs_root", "services/wizard_defs")
    return os.path.join(project_root(), rel)


def list_wizard_definitions(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """All ``*.json`` wizard definitions under the configured root."""
    root = wizard_defs_dir(settings)
    if not os.path.isdir(root):
        return []
    out: List[Dict[str, Any]] = []
    pattern = os.path.join(root, "*.json")
    for path in sorted(glob.glob(pattern)):
        try:
            data = load_json(path)
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        wid = data.get("wizard_id")
        if not wid or not isinstance(wid, str):
            continue
        steps = data.get("steps")
        if not isinstance(steps, list) or not steps:
            continue
        out.append(data)
    return out


def load_wizard_definition(settings: Dict[str, Any], wizard_id: str) -> Optional[Dict[str, Any]]:
    """Load one wizard definition by ``wizard_id`` (filename ``<wizard_id>.json``)."""
    wid = (wizard_id or "").strip()
    if not wid:
        return None
    path = os.path.join(wizard_defs_dir(settings), "%s.json" % wid)
    if not os.path.isfile(path):
        return None
    try:
        data = load_json(path)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("wizard_id") != wid:
        return None
    return data


def get_step(wizard_def: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
    steps = wizard_def.get("steps")
    if not isinstance(steps, list):
        return None
    if index < 0 or index >= len(steps):
        return None
    s = steps[index]
    return s if isinstance(s, dict) else None


def step_title(wizard_def: Dict[str, Any], index: int) -> str:
    s = get_step(wizard_def, index)
    if not s:
        return ""
    return str(s.get("title") or s.get("id") or "")
