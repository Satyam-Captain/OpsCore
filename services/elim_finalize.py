"""
ELIM request readiness and `.finalized` marker (mock / laptop only).

Readiness combines operation JSON with filesystem checks where applicable.
"""


import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from services.adapters import build_adapters
from services.adapters.request_base import RequestDirAdapterBase
from services.elim_db import elim_db_already_applied, elim_rollback_executed
from services.elim_lsb import (
    ELIM_CONFIG_TYPES,
    LS_CONFIG_TYPES,
    normalize_generation_cluster,
    normalize_publish_state,
)
from services.paths import project_root

FINALIZED_FILENAME = ".finalized"


def finalize_timestamp_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_required_elim_published_configs() -> List[str]:
    return list(ELIM_CONFIG_TYPES)


def get_required_ls_published_configs() -> List[str]:
    return list(LS_CONFIG_TYPES)


def _request_adapter(settings: Dict[str, Any]) -> RequestDirAdapterBase:
    _, req, _ = build_adapters(settings)
    return req


def finalized_marker_path(settings: Dict[str, Any], request_number: str) -> str:
    """Absolute path to ``REQ-<n>/.finalized``."""
    rn = str(request_number or "").strip()
    if not rn:
        raise ValueError("empty request_number")
    ad = _request_adapter(settings)
    req_dir = ad.get_request_path(rn)
    return os.path.join(req_dir, FINALIZED_FILENAME)


def request_has_finalized_marker(settings: Dict[str, Any], request_number: str) -> bool:
    try:
        return os.path.isfile(finalized_marker_path(settings, request_number))
    except ValueError:
        return False


def marker_path_for_storage(abs_path: str) -> str:
    """Path relative to project root when possible (for operation JSON)."""
    root = project_root()
    try:
        return os.path.relpath(abs_path, root).replace("\\", "/")
    except ValueError:
        return abs_path


def _publish_consistent(
    pub_map: Dict[str, Dict[str, Any]], config_type: str, expected_path: str
) -> Tuple[bool, str]:
    ent = pub_map.get(config_type) if isinstance(pub_map.get(config_type), dict) else {}
    status_ok = ent.get("status") == "done"
    dest = ent.get("destination_path")
    dest_str = str(dest).strip() if dest else ""

    if not expected_path:
        return False, "request path unknown"

    file_ok = os.path.isfile(expected_path)
    if not file_ok:
        return False, "file missing under request directory"

    if not status_ok:
        return False, "publish step not recorded as done in operation state"

    if dest_str:
        if os.path.normpath(dest_str) != os.path.normpath(expected_path):
            return False, "stored destination_path does not match expected publish location"
        if not os.path.isfile(dest_str):
            return False, "stored destination_path file missing on disk"

    return True, ""


def compute_license_resource_readiness(
    settings: Dict[str, Any],
    doc: Dict[str, Any],
    required_publish_config_ids: Tuple[str, ...],
) -> Dict[str, Any]:
    checks: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    check_rows: List[Dict[str, Any]] = []

    def _row(cid: str, data: Dict[str, Any]) -> None:
        checks[cid] = data
        check_rows.append({"id": cid, **data})

    db_ok = elim_db_already_applied(doc)
    _row(
        "db_apply_done",
        {
            "ok": db_ok,
            "label": "Database apply completed successfully",
            "informational": False,
            "detail": None,
        },
    )
    if not db_ok:
        missing.append("Database inserts are not applied (no completed db_apply steps).")

    gen_cluster = normalize_generation_cluster(doc.get("generation_context_cluster"))
    gen_ok = bool(gen_cluster)
    _row(
        "generation_context_present",
        {
            "ok": gen_ok,
            "label": "Generation context cluster is set",
            "informational": False,
            "detail": gen_cluster,
        },
    )
    if not gen_ok:
        missing.append("Generation context cluster is not set.")

    req_no = str(doc.get("request_number") or "").strip()
    req_dir = ""
    req_dir_ok = False
    adapter_note = ""
    try:
        if req_no:
            ad = _request_adapter(settings)
            req_dir = ad.get_request_path(req_no)
            req_dir_ok = ad.request_exists(req_no)
    except ValueError as e:
        adapter_note = str(e)
        req_dir_ok = False

    _row(
        "request_dir_exists",
        {
            "ok": req_dir_ok,
            "label": "Request directory exists",
            "informational": False,
            "detail": req_dir or None,
            "error": adapter_note or None,
        },
    )
    if not req_no:
        missing.append("Operation has no request number.")
    elif not req_dir_ok:
        missing.append(
            "Request directory is missing (expected under sandbox requests/LSF10CFG)."
        )

    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    pub_map = normalize_publish_state(sr.get("publish"))

    for ct in required_publish_config_ids:
        exp = os.path.join(req_dir, ct) if req_dir else ""
        cid = f"published_{ct.replace('.', '_')}"
        sub_ok, sub_msg = _publish_consistent(pub_map, ct, exp)
        _row(
            cid,
            {
                "ok": sub_ok,
                "label": f"Published {ct} (on disk + operation state)",
                "informational": False,
                "detail": exp or None,
                "message": sub_msg or None,
            },
        )
        if not sub_ok:
            missing.append(f"{ct}: {sub_msg}")

    rb_ok = not elim_rollback_executed(doc)
    _row(
        "rollback_not_executed",
        {
            "ok": rb_ok,
            "label": "Rollback has not been executed after apply",
            "informational": False,
            "detail": None,
        },
    )
    if not rb_ok:
        missing.append("Rollback was executed after apply; finalization is blocked.")

    fin_path = ""
    fin_exists = False
    try:
        if req_no:
            fin_path = finalized_marker_path(settings, req_no)
            fin_exists = os.path.isfile(fin_path)
    except ValueError:
        fin_path = ""

    _row(
        "finalized_already_present",
        {
            "ok": True,
            "label": ".finalized marker",
            "informational": True,
            "detail": fin_path or None,
            "marker_exists": fin_exists,
        },
    )

    core_ids = [
        "db_apply_done",
        "generation_context_present",
        "request_dir_exists",
        *[f"published_{ct.replace('.', '_')}" for ct in required_publish_config_ids],
        "rollback_not_executed",
    ]
    ready = all(checks[k]["ok"] for k in core_ids)

    return {
        "ready": ready,
        "can_create_marker": ready and not fin_exists,
        "checks": checks,
        "check_rows": check_rows,
        "missing_items": missing,
        "request_number": req_no or None,
        "request_dir": req_dir or None,
        "finalized_marker_path": fin_path or None,
        "finalized_marker_exists": fin_exists,
    }


def compute_elim_readiness(settings: Dict[str, Any], doc: Dict[str, Any]) -> Dict[str, Any]:
    return compute_license_resource_readiness(settings, doc, ELIM_CONFIG_TYPES)


def compute_ls_readiness(settings: Dict[str, Any], doc: Dict[str, Any]) -> Dict[str, Any]:
    return compute_license_resource_readiness(settings, doc, LS_CONFIG_TYPES)


def create_finalized_marker(
    settings: Dict[str, Any], request_number: str
) -> Tuple[str, str]:
    """
    Create an empty ``.finalized`` file under the request directory.

    Returns ``(absolute_path, status)`` where status is ``created`` or
    ``already_present``. Raises ``ValueError`` for invalid request number.
    """
    ad = _request_adapter(settings)
    rn = str(request_number).strip()
    req_dir = ad.get_request_path(rn)
    os.makedirs(req_dir, exist_ok=True)
    path = os.path.join(req_dir, FINALIZED_FILENAME)
    if os.path.isfile(path):
        return path, "already_present"
    open(path, "a", encoding="utf-8").close()
    return path, "created"


def merge_finalization_step_result(
    step_results: Dict[str, Any],
    *,
    status: str,
    marker_path: str,
    created_at: str,
) -> Dict[str, Any]:
    out = dict(step_results)
    out["finalization"] = {
        "status": status,
        "marker_path": marker_path,
        "created_at": created_at,
    }
    return out
