"""
Load JSON service definitions from ``services/defs``.

Validated definitions are plain dicts suitable for the engine and templates.
"""


import glob
import os
from typing import Any, Dict, List, Optional

from core.storage import load_json
from services.paths import project_root

REQUIRED_KEYS = ("id", "name", "description", "inputs", "steps")


class ServiceRegistryError(ValueError):
    """Invalid service definition file or content."""


def _validate_definition(data: Dict[str, Any], source_path: str) -> None:
    missing = [k for k in REQUIRED_KEYS if k not in data]
    if missing:
        raise ServiceRegistryError(f"{source_path}: missing keys {missing}")
    if not isinstance(data.get("inputs"), list):
        raise ServiceRegistryError(f"{source_path}: 'inputs' must be a list")
    if not isinstance(data.get("steps"), list):
        raise ServiceRegistryError(f"{source_path}: 'steps' must be a list")
    sid = data.get("id")
    if not sid or not isinstance(sid, str):
        raise ServiceRegistryError(f"{source_path}: 'id' must be a non-empty string")


def load_all_definitions(defs_root_relative: str) -> List[Dict[str, Any]]:
    """
    Load every ``*.json`` under the given root (relative to project root).

    Raises ServiceRegistryError if any file is invalid.
    """
    root = os.path.join(project_root(), defs_root_relative)
    if not os.path.isdir(root):
        return []

    out: List[Dict[str, Any]] = []
    pattern = os.path.join(root, "*.json")
    for path in sorted(glob.glob(pattern)):
        data = load_json(path)
        if not isinstance(data, dict):
            raise ServiceRegistryError(f"{path}: root must be a JSON object")
        _validate_definition(data, path)
        out.append(data)
    return out


def get_definition_by_id(
    service_id: str, defs_root_relative: str
) -> Optional[Dict[str, Any]]:
    """Return one service definition by ``id``, or None if not found."""
    for svc in load_all_definitions(defs_root_relative):
        if svc.get("id") == service_id:
            return svc
    return None
