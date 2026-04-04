"""
License-resource DB path: map ``service_inputs`` to JSON DB rows, preview, apply, rollback.

Supported service ids: ``add_license_resource_elim`` (ELIM) and ``add_license_resource_ls`` (LS).
Same tables and apply steps; rollback uses the active DB adapter's ``delete_row_by_id``
(JSON file or MariaDB, depending on ``service_db_backend``).
"""


from typing import Any, Dict, List, Optional, Tuple

ELIM_SERVICE_ID = "add_license_resource_elim"
LS_SERVICE_ID = "add_license_resource_ls"

TABLE_LICENSE_SERVERS = "license_servers"
TABLE_RESOURCES_REF = "resources_REF"
TABLE_CLUSTER_RESOURCES = "cluster_resources"

LICENSE_SERVER_KEYS = [
    "application",
    "site",
    "licCollector_ID",
    "company",
    "licType",
    "port",
    "ready4MIP",
    "licservers",
    "features",
]

RESOURCE_REF_KEYS_BODY = [
    "resource",
    "description",
    "resourcegroup_ID",
    "active",
    "useReason",
    "useLS",
    "wlDistLS",
    "ls_alloc_buffer",
    "skipSite2WAN",
    "skipmodlicenses",
    "res_type",
    "res_increasing",
    "res_interval",
    "res_builtin",
    "res_release",
    "res_consumable",
    "fixdyn",
    "resusage_method",
]

LICENSE_SERVER_INT_KEYS = frozenset({"licCollector_ID", "port", "ready4MIP"})

RESOURCE_REF_INT_KEYS = frozenset(
    {
        "resourcegroup_ID",
        "active",
        "useReason",
        "useLS",
        "wlDistLS",
        "ls_alloc_buffer",
        "skipSite2WAN",
        "res_interval",
        "res_builtin",
    }
)

PREVIEW_LICENSE_SERVER_PLACEHOLDER = "<from previous insert>"


def is_elim_operation(doc: Dict[str, Any]) -> bool:
    return str(doc.get("service_id") or "") == ELIM_SERVICE_ID


def is_ls_operation(doc: Dict[str, Any]) -> bool:
    return str(doc.get("service_id") or "") == LS_SERVICE_ID


def is_license_resource_db_operation(doc: Dict[str, Any]) -> bool:
    """True for ELIM or LS — same DB preview/apply/rollback mechanics."""
    return is_elim_operation(doc) or is_ls_operation(doc)


def is_license_resource_workspace_operation(doc: Dict[str, Any]) -> bool:
    """Workspace, stages, and table UI for ELIM or LS."""
    return is_license_resource_db_operation(doc)


def _raw(si: Dict[str, Any], key: str) -> str:
    v = si.get(key)
    if v is None:
        return ""
    return str(v).strip()


def _require_str(si: Dict[str, Any], key: str, errors: List[str]) -> Optional[str]:
    s = _raw(si, key)
    if not s:
        errors.append(f"Missing or empty: {key}")
        return None
    return s


def _require_int(si: Dict[str, Any], key: str, errors: List[str]) -> Optional[int]:
    s = _raw(si, key)
    if not s:
        errors.append(f"Missing or empty: {key}")
        return None
    try:
        return int(s, 10)
    except ValueError:
        errors.append(f"Integer required: {key}")
        return None


def build_license_server_row(
    si: Dict[str, Any], errors: List[str]
) -> Dict[str, Any]:
    """Typed row for ``license_servers`` (no ``ID``)."""
    row: Dict[str, Any] = {}
    for key in LICENSE_SERVER_KEYS:
        if key in LICENSE_SERVER_INT_KEYS:
            v = _require_int(si, key, errors)
            if v is not None:
                row[key] = v
        else:
            v = _require_str(si, key, errors)
            if v is not None:
                row[key] = v
    return row


def _validate_resource_ref_body(si: Dict[str, Any], errors: List[str]) -> None:
    """Validate resource row fields other than ``license_server_ID``."""
    for key in RESOURCE_REF_KEYS_BODY:
        if key in RESOURCE_REF_INT_KEYS:
            _require_int(si, key, errors)
        else:
            _require_str(si, key, errors)


def build_resource_ref_row(
    si: Dict[str, Any],
    license_server_id: Optional[int],
    errors: List[str],
    *,
    preview: bool,
) -> Dict[str, Any]:
    """
    Typed row for ``resources_REF`` (no ``ID``).

    If ``preview`` and ``license_server_id`` is None, ``license_server_ID`` is a placeholder string.
    """
    row: Dict[str, Any] = {}
    for key in RESOURCE_REF_KEYS_BODY:
        if key in RESOURCE_REF_INT_KEYS:
            v = _require_int(si, key, errors)
            if v is not None:
                row[key] = v
        else:
            v = _require_str(si, key, errors)
            if v is not None:
                row[key] = v
    if preview and license_server_id is None:
        row["license_server_ID"] = PREVIEW_LICENSE_SERVER_PLACEHOLDER
    elif license_server_id is not None:
        row["license_server_ID"] = license_server_id
    else:
        errors.append("license_server_ID was not set (internal error)")
    return row


def validate_elim_service_inputs(si: Optional[Dict[str, Any]]) -> List[str]:
    """Return human-readable errors; empty list means OK for preview/apply."""
    if not si or not isinstance(si, dict) or len(si) == 0:
        return ["service_inputs is missing or empty"]
    errors: List[str] = []
    build_license_server_row(si, errors)
    _validate_resource_ref_body(si, errors)
    return errors


def validate_license_server_inputs_only(si: Optional[Dict[str, Any]]) -> List[str]:
    """Validate only ``license_servers`` row fields (wizard partial steps)."""
    if not si or not isinstance(si, dict) or len(si) == 0:
        return ["service_inputs is missing or empty"]
    errors: List[str] = []
    build_license_server_row(si, errors)
    return errors


def validate_resource_ref_inputs_only(si: Dict[str, Any], license_server_id: int) -> List[str]:
    """Validate ``resources_REF`` row when ``license_server_id`` is already known."""
    errors: List[str] = []
    build_resource_ref_row(si, license_server_id, errors, preview=False)
    return errors


def build_elim_preview(
    si: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Return (license_servers row, resources_REF row with placeholder ID, errors).

    ``errors`` comes from ``validate_elim_service_inputs``; rows are built with
    empty error collectors so shape matches a valid apply when inputs are OK.
    """
    errors = validate_elim_service_inputs(si)
    ls_row = build_license_server_row(si, [])
    rr_row = build_resource_ref_row(si, None, [], preview=True)
    return ls_row, rr_row, errors


def get_db_apply_steps(doc: Dict[str, Any]) -> List[Any]:
    sr = doc.get("step_results")
    if not isinstance(sr, dict):
        return []
    steps = sr.get("db_apply")
    return steps if isinstance(steps, list) else []


def elim_db_step_done(doc: Dict[str, Any], step_name: str) -> bool:
    """True if ``db_apply`` contains a matching step with ``status`` done."""
    for s in get_db_apply_steps(doc):
        if isinstance(s, dict) and s.get("step") == step_name and s.get("status") == "done":
            return True
    return False


def elim_inserted_id_for_step(doc: Dict[str, Any], step_name: str) -> Optional[int]:
    for s in get_db_apply_steps(doc):
        if not isinstance(s, dict) or s.get("step") != step_name or s.get("status") != "done":
            continue
        raw = s.get("inserted_id")
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    return None


def elim_db_already_applied(doc: Dict[str, Any]) -> bool:
    """Any db_apply step completed (supports partial table-scoped applies)."""
    for s in get_db_apply_steps(doc):
        if isinstance(s, dict) and s.get("status") == "done":
            return True
    return False


def elim_db_fully_applied(doc: Dict[str, Any]) -> bool:
    """Both ELIM inserts recorded as done (full-service apply complete)."""
    return elim_db_step_done(doc, "insert_license_server") and elim_db_step_done(
        doc, "insert_resource"
    )


def apply_elim_license_server_only(
    db: Any, si: Dict[str, Any]
) -> Tuple[bool, Optional[int], List[str]]:
    """Insert ``license_servers`` row only (table-scoped apply)."""
    pre = validate_license_server_inputs_only(si)
    if pre:
        return False, None, pre
    ls_row = build_license_server_row(si, [])
    try:
        ls_id = db.insert_row(TABLE_LICENSE_SERVERS, ls_row)
    except (OSError, ValueError, TypeError, KeyError) as e:
        return False, None, [f"Insert license_servers failed: {e}"]
    return True, ls_id, []


def apply_elim_resource_only(
    db: Any, si: Dict[str, Any], license_server_id: int
) -> Tuple[bool, Optional[int], List[str]]:
    """Insert ``resources_REF`` row only; ``license_server_id`` must already exist."""
    pre = validate_resource_ref_inputs_only(si, license_server_id)
    if pre:
        return False, None, pre
    rr_apply = build_resource_ref_row(si, license_server_id, [], preview=False)
    try:
        rr_id = db.insert_row(TABLE_RESOURCES_REF, rr_apply)
    except (OSError, ValueError, TypeError, KeyError) as e:
        return False, None, [f"Insert resources_REF failed: {e}"]
    return True, rr_id, []


def apply_elim_inserts(
    db: Any, si: Dict[str, Any]
) -> Tuple[bool, Optional[int], Optional[int], List[str]]:
    """
    Insert license_servers then resources_REF. Returns (ok, ls_id, rr_id, errors).

    On failure after first insert, errors describe the problem; first row may remain (no auto-undo).
    """
    pre = validate_elim_service_inputs(si)
    if pre:
        return False, None, None, pre
    ls_row = build_license_server_row(si, [])
    try:
        ls_id = db.insert_row(TABLE_LICENSE_SERVERS, ls_row)
    except (OSError, ValueError, TypeError, KeyError) as e:
        return False, None, None, [f"Insert license_servers failed: {e}"]
    rr_apply = build_resource_ref_row(si, ls_id, [], preview=False)
    try:
        rr_id = db.insert_row(TABLE_RESOURCES_REF, rr_apply)
    except (OSError, ValueError, TypeError, KeyError) as e:
        return False, ls_id, None, [f"Insert resources_REF failed: {e}"]
    return True, ls_id, rr_id, []


def build_db_apply_step_license_server(ls_id: int) -> Dict[str, Any]:
    return {
        "step": "insert_license_server",
        "status": "done",
        "inserted_id": ls_id,
        "table_id": TABLE_LICENSE_SERVERS,
    }


def build_db_apply_step_resource(rr_id: int) -> Dict[str, Any]:
    return {
        "step": "insert_resource",
        "status": "done",
        "inserted_id": rr_id,
        "table_id": TABLE_RESOURCES_REF,
    }


def build_db_apply_step_results(ls_id: int, rr_id: int) -> List[Dict[str, Any]]:
    return [
        build_db_apply_step_license_server(ls_id),
        build_db_apply_step_resource(rr_id),
    ]


def merge_db_apply_steps(
    existing: List[Any], *new_steps: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Replace steps with the same ``step`` name, then append new ones; keep stable order."""
    by_step: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for s in existing:
        if not isinstance(s, dict):
            continue
        name = str(s.get("step") or "")
        if not name:
            continue
        if name not in by_step:
            order.append(name)
        by_step[name] = dict(s)
    for ns in new_steps:
        name = str(ns.get("step") or "")
        if not name:
            continue
        if name not in by_step:
            order.append(name)
        by_step[name] = dict(ns)
    return [by_step[k] for k in order if k in by_step]


def build_rollback_stack_license_only(ls_id: int) -> List[Dict[str, Any]]:
    """Rollback when only ``license_servers`` has been inserted."""
    return [{"type": "delete_by_id", "table": TABLE_LICENSE_SERVERS, "id": ls_id}]


def build_rollback_stack(ls_id: int, rr_id: int) -> List[Dict[str, Any]]:
    """Resource row first (reverse apply order for future delete execution)."""
    return [
        {"type": "delete_by_id", "table": TABLE_RESOURCES_REF, "id": rr_id},
        {"type": "delete_by_id", "table": TABLE_LICENSE_SERVERS, "id": ls_id},
    ]


def elim_rollback_executed(doc: Dict[str, Any]) -> bool:
    return bool(doc.get("rollback_executed"))


def elim_can_rollback(doc: Dict[str, Any]) -> bool:
    """
    ELIM/LS op may run rollback: DB apply done, stack non-empty, rollback not yet executed.
    """
    if not is_license_resource_db_operation(doc) or not elim_db_already_applied(doc):
        return False
    if elim_rollback_executed(doc):
        return False
    rb = doc.get("rollback_stack")
    return isinstance(rb, list) and len(rb) > 0


def rollback_step_name(table: str) -> str:
    if table == TABLE_RESOURCES_REF:
        return "delete_resource"
    if table == TABLE_LICENSE_SERVERS:
        return "delete_license_server"
    if table == TABLE_CLUSTER_RESOURCES:
        return "delete_cluster_resource"
    return "delete_row"


def build_rollback_preview_items(
    db: Any, rollback_stack: List[Any]
) -> List[Dict[str, Any]]:
    """
    One entry per stack item: table, id, row snapshot or None, note for missing rows.
    """
    out: List[Dict[str, Any]] = []
    if not isinstance(rollback_stack, list):
        return out
    for raw in rollback_stack:
        if not isinstance(raw, dict):
            continue
        if raw.get("type") != "delete_by_id":
            continue
        table = raw.get("table")
        rid_raw = raw.get("id")
        try:
            rid = int(rid_raw)
        except (TypeError, ValueError):
            out.append(
                {
                    "table": table,
                    "id": rid_raw,
                    "row": None,
                    "note": "Invalid id in rollback stack.",
                }
            )
            continue
        if not table or not isinstance(table, str):
            out.append(
                {
                    "table": table or "—",
                    "id": rid,
                    "row": None,
                    "note": "Missing table name.",
                }
            )
            continue
        try:
            row = db.get_row_by_id(str(table), rid)
        except (ValueError, TypeError, OSError):
            row = None
        out.append(
            {
                "table": table,
                "id": rid,
                "row": row,
                "note": (
                    None
                    if row is not None
                    else "Already deleted or missing from mock DB."
                ),
            }
        )
    return out


def execute_elim_rollback(db: Any, rollback_stack: List[Any]) -> List[Dict[str, Any]]:
    """
    Run deletes in stack order; collect per-step results. Continues after failures.

    Each result: step, status (done | not_found | failed | skipped), id, optional error.
    """
    results: List[Dict[str, Any]] = []
    if not isinstance(rollback_stack, list):
        return results
    for raw in rollback_stack:
        if not isinstance(raw, dict) or raw.get("type") != "delete_by_id":
            results.append(
                {
                    "step": "skipped",
                    "status": "skipped",
                    "id": None,
                    "error": "Invalid rollback entry",
                }
            )
            continue
        table = raw.get("table")
        try:
            rid = int(raw.get("id"))
        except (TypeError, ValueError):
            results.append(
                {
                    "step": "skipped",
                    "status": "failed",
                    "id": raw.get("id"),
                    "error": "Invalid id",
                }
            )
            continue
        if not table or not isinstance(table, str):
            results.append(
                {
                    "step": "skipped",
                    "status": "failed",
                    "id": rid,
                    "error": "Missing table",
                }
            )
            continue
        step = rollback_step_name(table)
        try:
            removed = db.delete_row_by_id(table, rid)
        except (OSError, ValueError, TypeError) as e:
            results.append(
                {
                    "step": step,
                    "status": "failed",
                    "id": rid,
                    "error": str(e),
                }
            )
            continue
        results.append(
            {
                "step": step,
                "status": "done" if removed else "not_found",
                "id": rid,
            }
        )
    return results
