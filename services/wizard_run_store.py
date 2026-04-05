"""
Persist GMCAssist wizard runs as one JSON file per run under ``data/gmcassist/wizard_runs``.
"""

import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from core.storage import load_json, save_json
from services.gmcassist_cluster_resources import CLUSTER_RESOURCE_ROLLBACK_ROWS_KEY
from services.paths import project_root
from services.wizard_loader import load_wizard_definition

_RUN_ID_ALLOWED = set("0123456789abcdef-")

# Runs that do not block starting another wizard or entering a new request.
_NON_BLOCKING_STATUSES = frozenset({"completed", "abandoned"})

# Pre-request runs with no progress older than this are auto-abandoned (catalog exclusivity only).
_PLACEHOLDER_STALE_HOURS_DEFAULT = 72.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _placeholder_stale_hours() -> float:
    raw = os.environ.get("GMCASSIST_PLACEHOLDER_STALE_HOURS")
    if raw is None or not str(raw).strip():
        return _PLACEHOLDER_STALE_HOURS_DEFAULT
    try:
        v = float(str(raw).strip())
        return v if v > 0 else _PLACEHOLDER_STALE_HOURS_DEFAULT
    except ValueError:
        return _PLACEHOLDER_STALE_HOURS_DEFAULT


def _parse_iso_utc_naive(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1]
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _doc_timestamp_for_stale_check(doc: Dict[str, Any]) -> Optional[datetime]:
    for key in ("updated_at", "created_at"):
        dt = _parse_iso_utc_naive(doc.get(key))
        if dt:
            return dt
    return None


def wizard_run_has_meaningful_progress(doc: Dict[str, Any]) -> bool:
    """
    True if the run has moved past a trivial placeholder (request, step index, data, or context).
    Used to avoid abandoning runs that genuinely started work without a confirmed request number.
    """
    if normalize_wizard_request_number(raw_request_number_from_run(doc)):
        return True
    if int(doc.get("current_step_index") or 0) > 0:
        return True
    sd = doc.get("step_data")
    if isinstance(sd, dict) and sd:
        return True
    pcr = doc.get("pending_cluster_resources")
    if isinstance(pcr, list) and pcr:
        return True
    ctx = doc.get("context")
    if not isinstance(ctx, dict):
        return False
    ls = ctx.get("license_server_id")
    if ls is not None:
        try:
            if int(ls) > 0:
                return True
        except (TypeError, ValueError):
            pass
    rid = ctx.get("resource_id")
    if rid is not None:
        try:
            if int(rid) > 0:
                return True
        except (TypeError, ValueError):
            pass
    cr = ctx.get("cluster_resource_row_ids")
    if isinstance(cr, list) and cr:
        return True
    crr = ctx.get(CLUSTER_RESOURCE_ROLLBACK_ROWS_KEY)
    if isinstance(crr, list) and crr:
        return True
    cl = ctx.get("cluster_list")
    if isinstance(cl, list) and cl:
        return True
    if ctx.get("generation_cluster"):
        return True
    pcl = ctx.get("published_clusters_log")
    if isinstance(pcl, list) and pcl:
        return True
    return False


def build_step_advance_updates(
    current_step_index: int,
    steps_len: int,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Set ``current_step_index`` to current+1 and ``status`` to ``completed`` when advancing past the last step.
    """
    new_idx = int(current_step_index) + 1
    out: Dict[str, Any] = dict(extra) if extra else {}
    out["current_step_index"] = new_idx
    if steps_len > 0 and new_idx >= int(steps_len):
        out["status"] = "completed"
    return out


def maybe_repair_run_past_final_step(
    settings: Dict[str, Any],
    run_id: str,
    doc: Dict[str, Any],
) -> Dict[str, Any]:
    """
    If index is past the last step but status is still blocking, mark ``completed``.
    Fixes runs that advanced on the final step without setting status.
    """
    st = str(doc.get("status") or "").strip().lower()
    if st in _NON_BLOCKING_STATUSES:
        return doc
    wid = str(doc.get("wizard_id") or "").strip()
    if not wid:
        return doc
    wdef = load_wizard_definition(settings, wid)
    if not wdef:
        return doc
    steps = wdef.get("steps")
    if not isinstance(steps, list) or not steps:
        return doc
    n = len(steps)
    idx = int(doc.get("current_step_index") or 0)
    if n <= 0 or idx < n:
        return doc
    updated = update_wizard_run(
        settings,
        run_id,
        {"status": "completed", "current_step_index": n - 1},
    )
    return updated if updated else doc


def maybe_abandon_stale_placeholder_run(
    settings: Dict[str, Any],
    run_id: str,
    doc: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Abandon old runs that never confirmed a request number and never made progress (ghost ``Start``).
    """
    st = str(doc.get("status") or "").strip().lower()
    if st in _NON_BLOCKING_STATUSES:
        return doc
    if normalize_wizard_request_number(raw_request_number_from_run(doc)):
        return doc
    if wizard_run_has_meaningful_progress(doc):
        return doc
    ref_dt = _doc_timestamp_for_stale_check(doc)
    if ref_dt is None:
        return doc
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    max_age = timedelta(hours=_placeholder_stale_hours())
    if (now - ref_dt) < max_age:
        return doc
    updated = abandon_wizard_run(
        settings,
        run_id,
        "auto_stale_placeholder_no_request",
    )
    return updated if updated else doc


def reconcile_wizard_run_for_blocking(
    settings: Dict[str, Any],
    run_id: str,
    doc: Dict[str, Any],
) -> Dict[str, Any]:
    """Apply repair + stale-placeholder abandonment before exclusivity checks."""
    doc = maybe_repair_run_past_final_step(settings, run_id, doc)
    doc = maybe_abandon_stale_placeholder_run(settings, run_id, doc)
    return doc


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
            "cluster_resource_row_ids": [],
            CLUSTER_RESOURCE_ROLLBACK_ROWS_KEY: [],
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


def normalize_wizard_request_number(raw: Any) -> str:
    """
    Canonical form for comparing request numbers (e.g. ``4447`` and ``REQ-4447`` match).

    Returns empty string if missing/whitespace after stripping.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    u = s.upper()
    if u.startswith("REQ-"):
        s = s[4:].strip()
    return s.strip()


def raw_request_number_from_run(doc: Dict[str, Any]) -> str:
    """Best-effort display/raw request number from a run document."""
    top = doc.get("request_number")
    if isinstance(top, str) and top.strip():
        return top.strip()
    si = doc.get("service_inputs")
    if isinstance(si, dict):
        v = si.get("request_number")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def is_wizard_run_blocking(doc: Dict[str, Any]) -> bool:
    """
    True if this run still holds the global "one active wizard" lock.

    Blocking: missing/unknown status, ``active``, ``in_progress``, etc.
    Non-blocking: ``completed``, ``abandoned``.
    """
    s = str(doc.get("status") or "").strip().lower()
    return s not in _NON_BLOCKING_STATUSES


def iter_wizard_runs(
    settings: Dict[str, Any],
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Load all valid wizard run documents under the runs directory.

    Skips unreadable or invalid files. Order is not guaranteed.
    """
    out: List[Tuple[str, Dict[str, Any]]] = []
    root = wizard_runs_dir(settings)
    if not os.path.isdir(root):
        return out
    try:
        names = os.listdir(root)
    except OSError:
        return out
    for name in names:
        if not name.endswith(".json"):
            continue
        run_id = name[:-5]
        if not _safe_run_id(run_id):
            continue
        try:
            doc = load_wizard_run(settings, run_id)
        except (OSError, ValueError, TypeError):
            continue
        if not isinstance(doc, dict):
            continue
        out.append((run_id, doc))
    return out


def _sort_key_run(doc: Dict[str, Any]) -> str:
    return str(doc.get("updated_at") or doc.get("created_at") or "")


def list_blocking_wizard_runs(
    settings: Dict[str, Any],
) -> List[Tuple[str, Dict[str, Any]]]:
    """Blocking runs, newest ``updated_at`` first."""
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for rid, d in iter_wizard_runs(settings):
        d2 = reconcile_wizard_run_for_blocking(settings, rid, d)
        if is_wizard_run_blocking(d2):
            rows.append((rid, d2))
    rows.sort(key=lambda x: _sort_key_run(x[1]), reverse=True)
    return rows


def find_any_blocking_wizard_run(
    settings: Dict[str, Any],
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Most recently updated blocking run, or ``None``."""
    rows = list_blocking_wizard_runs(settings)
    return rows[0] if rows else None


def find_blocking_run_for_normalized_request(
    settings: Dict[str, Any], normalized_request: str
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Blocking run whose request number matches ``normalized_request`` (non-empty), newest first.
    """
    if not normalized_request:
        return None
    matches: List[Tuple[str, Dict[str, Any]]] = []
    for rid, doc in list_blocking_wizard_runs(settings):
        if normalize_wizard_request_number(raw_request_number_from_run(doc)) == normalized_request:
            matches.append((rid, doc))
    return matches[0] if matches else None


def abandon_wizard_run(
    settings: Dict[str, Any],
    run_id: str,
    reason: str,
    superseded_by_run_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Mark a run ``abandoned`` so it no longer blocks global exclusivity."""
    if not _safe_run_id(run_id):
        return None
    updates: Dict[str, Any] = {
        "status": "abandoned",
        "abandoned_at": _utc_now_iso(),
        "abandoned_reason": str(reason or "abandoned"),
    }
    if superseded_by_run_id and _safe_run_id(superseded_by_run_id):
        updates["superseded_by_wizard_run_id"] = superseded_by_run_id
    return update_wizard_run(settings, run_id, updates)


def mark_wizard_run_superadmin_reset(
    settings: Dict[str, Any],
    run_id: str,
    audit: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    After successful DB rollback: mark run ``abandoned``, clear applied DB ids from context,
    record audit (prior ids / request) for traceability.
    """
    if not _safe_run_id(run_id):
        return None
    doc = load_wizard_run(settings, run_id)
    if not doc:
        return None
    old_ctx = doc.get("context")
    if not isinstance(old_ctx, dict):
        old_ctx = {}
    new_ctx = dict(old_ctx)
    new_ctx["cluster_resource_row_ids"] = []
    new_ctx[CLUSTER_RESOURCE_ROLLBACK_ROWS_KEY] = []
    new_ctx["license_server_id"] = None
    new_ctx["resource_id"] = None
    now = _utc_now_iso()
    updates: Dict[str, Any] = {
        "status": "abandoned",
        "abandoned_at": now,
        "abandoned_reason": "superadmin_deleted",
        "superadmin_reset_at": now,
        "superadmin_reset_by": "gmcassist_superadmin_session",
        "superadmin_reset_audit": dict(audit) if isinstance(audit, dict) else {},
        "context": new_ctx,
    }
    return update_wizard_run(settings, run_id, updates)
