"""
Persist operation state as one JSON file per operation under ``data/services/operations``.
"""


import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.storage import load_json, save_json
from services.lifecycle_ops import default_lifecycle_dict
from services.paths import project_root

# Filenames: only UUIDs from this store (safe path segment)
_OP_ID_ALLOWED = set("0123456789abcdef-")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_op_id(op_id: str) -> bool:
    if not op_id or len(op_id) > 64:
        return False
    return all(c in _OP_ID_ALLOWED for c in op_id.lower())


def operations_dir(settings: Dict[str, Any]) -> str:
    rel = settings.get("service_operations_root", "data/services/operations")
    return os.path.join(project_root(), rel)


def operation_path(settings: Dict[str, Any], operation_id: str) -> str:
    if not _safe_op_id(operation_id):
        raise ValueError("invalid operation_id")
    return os.path.join(operations_dir(settings), f"{operation_id}.json")


def create_operation(
    settings: Dict[str, Any],
    *,
    service_id: str,
    status: str,
    request_number: str,
    inputs: Dict[str, Any],
    cluster: Optional[str] = None,
    deployment_targets: Optional[list] = None,
    current_step: Optional[str] = None,
    step_results: Optional[Dict[str, Any]] = None,
    rollback_stack: Optional[list] = None,
    workspace_control: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create a new operation file and return the stored document.

    Operations are **request-scoped**: ``cluster`` is optional (typically ``null``
    for new records). ``deployment_targets`` is reserved for future
    cluster/deployment selection; stored as a list (often empty).
    """
    op_id = str(uuid.uuid4())
    now = _utc_now_iso()
    targets = deployment_targets if deployment_targets is not None else []
    doc: Dict[str, Any] = {
        "operation_id": op_id,
        "service_id": service_id,
        "status": status,
        "current_step": current_step,
        "created_at": now,
        "updated_at": now,
        "request_number": request_number,
        "cluster": cluster,
        "deployment_targets": targets,
        "inputs": dict(inputs),
        "step_results": step_results if step_results is not None else {},
        "rollback_stack": rollback_stack if rollback_stack is not None else [],
        "service_inputs": {},
        "generation_context_cluster": None,
        "lifecycle": default_lifecycle_dict(),
    }
    if workspace_control is not None:
        doc["workspace_control"] = dict(workspace_control)
    d = operations_dir(settings)
    os.makedirs(d, exist_ok=True)
    save_json(operation_path(settings, op_id), doc)
    return doc


def _operation_sort_key(doc: Dict[str, Any]) -> tuple:
    """Newest-first ordering: updated_at, then created_at; missing timestamps sort last."""
    ts = doc.get("updated_at") or doc.get("created_at") or ""
    oid = str(doc.get("operation_id") or "")
    # Empty ts sorts before non-empty with reverse=True would be wrong; use min ISO for missing.
    effective_ts = ts if ts else "1970-01-01T00:00:00Z"
    return (effective_ts, oid)


def _load_all_operation_docs(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Load every ``*.json`` in the operations directory; skip unreadable or invalid files."""
    d = operations_dir(settings)
    if not os.path.isdir(d):
        return []
    out: List[Dict[str, Any]] = []
    for name in os.listdir(d):
        if not name.endswith(".json"):
            continue
        op_id = name[:-5]
        if not _safe_op_id(op_id):
            continue
        path = os.path.join(d, name)
        try:
            data = load_json(path)
        except (OSError, ValueError, TypeError):
            continue
        if not isinstance(data, dict):
            continue
        if not data.get("operation_id"):
            data = {**data, "operation_id": op_id}
        out.append(data)
    return out


def list_operations(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """All operations, newest first (by ``updated_at`` / ``created_at``)."""
    return find_operations(settings)


def find_all_active_workspaces(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    All operations that are still **active** (not terminal), across every service/request.

    Used for global workspace locking. Terminal rules live in
    ``workspace_lifecycle.is_workspace_terminal``.
    """
    from services.workspace_lifecycle import is_workspace_active

    docs = _load_all_operation_docs(settings)
    active = [d for d in docs if is_workspace_active(d)]
    active.sort(key=_operation_sort_key, reverse=True)
    return active


def find_active_workspaces(
    settings: Dict[str, Any],
    service_id: str,
    request_number: str,
) -> List[Dict[str, Any]]:
    """
    Operations for the same service + request that are still **active** (not terminal).

    Terminal rules live in ``workspace_lifecycle.is_workspace_terminal``.
    """
    from services.workspace_lifecycle import is_workspace_active

    sid = str(service_id or "").strip()
    rn = str(request_number or "").strip()
    if not sid or not rn:
        return []
    docs = find_operations(settings, service_id=sid, request_number=rn)
    return [d for d in docs if is_workspace_active(d)]


def find_operations(
    settings: Dict[str, Any],
    service_id: Optional[str] = None,
    request_number: Optional[str] = None,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Filter operations loaded from JSON files. All filters are optional (AND logic).

    ``request_number`` is compared stripped, case-sensitive, to the stored value.
    """
    docs = _load_all_operation_docs(settings)
    if service_id is not None and str(service_id).strip() != "":
        sid = str(service_id).strip()
        docs = [d for d in docs if str(d.get("service_id", "")).strip() == sid]
    if request_number is not None and str(request_number).strip() != "":
        rn = str(request_number).strip()
        docs = [d for d in docs if str(d.get("request_number", "")).strip() == rn]
    if status is not None and str(status).strip() != "":
        st = str(status).strip()
        docs = [d for d in docs if str(d.get("status", "")).strip() == st]
    docs.sort(key=_operation_sort_key, reverse=True)
    return docs


def load_operation(settings: Dict[str, Any], operation_id: str) -> Optional[Dict[str, Any]]:
    """Load operation JSON or None if missing."""
    if not _safe_op_id(operation_id):
        return None
    path = operation_path(settings, operation_id)
    if not os.path.isfile(path):
        return None
    try:
        return load_json(path)
    except (OSError, ValueError):
        return None


def update_operation(
    settings: Dict[str, Any], operation_id: str, updates: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Merge ``updates`` into the operation document and save. Returns new doc or None."""
    doc = load_operation(settings, operation_id)
    if not doc:
        return None
    doc.update(updates)
    doc["updated_at"] = _utc_now_iso()
    save_json(operation_path(settings, operation_id), doc)
    return doc


def delete_operation(settings: Dict[str, Any], operation_id: str) -> bool:
    """
    Remove the operation JSON file only (no sandbox DB / request dirs).
    Returns True if the file existed and was removed.
    """
    if not _safe_op_id(operation_id):
        return False
    path = operation_path(settings, operation_id)
    if not os.path.isfile(path):
        return False
    try:
        os.remove(path)
        return True
    except OSError:
        return False
