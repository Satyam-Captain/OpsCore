"""
Persist GMCAssist wizard runs as one JSON file per run under ``data/gmcassist/wizard_runs``.
"""

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.storage import load_json, save_json
from services.paths import project_root

_RUN_ID_ALLOWED = set("0123456789abcdef-")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_run_id(run_id: str) -> bool:
    if not run_id or len(run_id) > 64:
        return False
    return all(c in _RUN_ID_ALLOWED for c in run_id.lower())


def wizard_runs_dir(settings: Dict[str, Any]) -> str:
    rel = settings.get("gmcassist_wizard_runs_root", "data/gmcassist/wizard_runs")
    return os.path.join(project_root(), rel)


def wizard_run_path(settings: Dict[str, Any], run_id: str) -> str:
    if not _safe_run_id(run_id):
        raise ValueError("invalid wizard_run_id")
    return os.path.join(wizard_runs_dir(settings), "%s.json" % run_id)


def create_wizard_run(
    settings: Dict[str, Any],
    *,
    wizard_id: str,
    service_id: str,
    service_label: str,
) -> Dict[str, Any]:
    """Create a new wizard run document and save it."""
    run_id = str(uuid.uuid4())
    now = _utc_now_iso()
    doc: Dict[str, Any] = {
        "wizard_run_id": run_id,
        "wizard_id": wizard_id,
        "service_id": service_id,
        "service_label": service_label,
        "request_number": "",
        "current_step_index": 0,
        "status": "active",
        "service_inputs": {},
        "context": {
            "license_server_id": None,
            "resource_id": None,
            "generation_cluster": None,
            "cannot_go_before_index": 0,
            "cluster_list": [],
            "cluster_queue_index": {},
        },
        "step_data": {},
        "rollback": {"planned": [], "notes": ""},
        "created_at": now,
        "updated_at": now,
    }
    save_wizard_run(settings, doc)
    return doc


def load_wizard_run(settings: Dict[str, Any], run_id: str) -> Optional[Dict[str, Any]]:
    if not _safe_run_id(run_id):
        return None
    path = wizard_run_path(settings, run_id)
    if not os.path.isfile(path):
        return None
    try:
        return load_json(path)
    except (OSError, ValueError):
        return None


def save_wizard_run(settings: Dict[str, Any], doc: Dict[str, Any]) -> None:
    rid = str(doc.get("wizard_run_id") or "")
    if not _safe_run_id(rid):
        raise ValueError("invalid wizard_run_id in document")
    path = wizard_run_path(settings, rid)
    save_json(path, doc)


def update_wizard_run(
    settings: Dict[str, Any], run_id: str, updates: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Merge ``updates`` into the run document and save."""
    doc = load_wizard_run(settings, run_id)
    if not doc:
        return None
    doc.update(updates)
    doc["updated_at"] = _utc_now_iso()
    save_wizard_run(settings, doc)
    return doc
