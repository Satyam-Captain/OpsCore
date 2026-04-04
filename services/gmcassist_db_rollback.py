"""
GMCAssist: rollback DB inserts when navigating Back onto a completed ``db_insert`` step.

Uses the same delete-by-id mechanics as ELIM workspace rollback (``execute_elim_rollback``).
Does not touch publish/file rollback.
"""

from typing import Any, Dict, List, Optional, Tuple

from services.adapters import build_adapters
from services.elim_db import (
    TABLE_LICENSE_SERVERS,
    TABLE_RESOURCES_REF,
    execute_elim_rollback,
)
from services.gmcassist_wizard_steps import ensure_wizard_context


def _norm_insert_target(raw: Any) -> str:
    return str(raw or "").strip().lower()


def _is_license_target(s: str) -> bool:
    return s in ("license_servers", TABLE_LICENSE_SERVERS.lower(), "license_server")


def _is_resource_target(s: str) -> bool:
    return s in ("resources_ref", TABLE_RESOURCES_REF.lower())


def _pop_step_data_for_targets(run: Dict[str, Any], wizard_def: Dict[str, Any], kinds: Tuple[str, ...]) -> None:
    """Remove ``step_data`` entries for db_insert steps whose insert_target matches ``kinds``."""
    steps = wizard_def.get("steps")
    if not isinstance(steps, list):
        return
    sd = run.setdefault("step_data", {})
    if not isinstance(sd, dict):
        return
    kinds_set = set(kinds)
    for st in steps:
        if not isinstance(st, dict) or str(st.get("type") or "").strip() != "db_insert":
            continue
        it = _norm_insert_target(st.get("insert_target"))
        key_kind = None
        if _is_license_target(it):
            key_kind = "license"
        elif _is_resource_target(it):
            key_kind = "resource"
        if key_kind and key_kind in kinds_set:
            sid = str(st.get("id") or "").strip()
            if sid and sid in sd:
                del sd[sid]


def maybe_rollback_before_navigate_back(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    wizard_def: Dict[str, Any],
    target_index: int,
) -> Tuple[bool, Optional[str]]:
    """
    Called when navigating Back from ``target_index + 1`` to ``target_index``.

    If ``steps[target_index]`` is a ``db_insert`` step whose row(s) are reflected in
    ``context`` / ``step_data``, delete those rows via the DB adapter.

    Returns ``(True, None)`` if nothing to do or rollback succeeded without a user message.
    Returns ``(True, msg)`` on successful rollback (optional flash).
    Returns ``(False, err)`` on failure (do not change step index).
    """
    steps = wizard_def.get("steps")
    if not isinstance(steps, list) or target_index < 0 or target_index >= len(steps):
        return True, None

    step_def = steps[target_index]
    if not isinstance(step_def, dict):
        return True, None
    if str(step_def.get("type") or "").strip() != "db_insert":
        return True, None

    it_raw = _norm_insert_target(step_def.get("insert_target"))
    if not it_raw:
        return True, None

    ctx = ensure_wizard_context(run)
    db, _, _ = build_adapters(settings)

    stack: List[Dict[str, Any]] = []

    if _is_resource_target(it_raw):
        rr_raw = ctx.get("resource_id")
        if rr_raw is None:
            _pop_step_data_for_targets(run, wizard_def, ("resource",))
            return True, None
        try:
            rr_id = int(rr_raw)
        except (TypeError, ValueError):
            return False, "Invalid resource_id in wizard context; cannot roll back safely."
        stack.append({"type": "delete_by_id", "table": TABLE_RESOURCES_REF, "id": rr_id})

    elif _is_license_target(it_raw):
        # License rollback: delete dependent resource row first if still present.
        rr_raw = ctx.get("resource_id")
        ls_raw = ctx.get("license_server_id")
        if rr_raw is not None:
            try:
                stack.append(
                    {
                        "type": "delete_by_id",
                        "table": TABLE_RESOURCES_REF,
                        "id": int(rr_raw),
                    }
                )
            except (TypeError, ValueError):
                return False, "Invalid resource_id in wizard context; cannot roll back safely."
        if ls_raw is not None:
            try:
                stack.append(
                    {
                        "type": "delete_by_id",
                        "table": TABLE_LICENSE_SERVERS,
                        "id": int(ls_raw),
                    }
                )
            except (TypeError, ValueError):
                return False, "Invalid license_server_id in wizard context; cannot roll back safely."
        if not stack:
            _pop_step_data_for_targets(run, wizard_def, ("license", "resource"))
            ctx["resource_id"] = None
            ctx["license_server_id"] = None
            return True, None
    else:
        return True, None

    results = execute_elim_rollback(db, stack)
    failed = [r for r in results if r.get("status") == "failed"]
    if failed:
        err_parts = []
        for r in failed:
            e = r.get("error")
            if e:
                err_parts.append(str(e))
        msg = "; ".join(err_parts) if err_parts else "Database delete failed."
        return (
            False,
            "Rollback failed (%s). Wizard state was not changed. "
            "Check DB connectivity, permissions, and whether the row still exists."
            % msg,
        )

    # Clear context keys for rows we attempted to remove (even if not_found — idempotent UI state).
    if _is_resource_target(it_raw):
        ctx["resource_id"] = None
        _pop_step_data_for_targets(run, wizard_def, ("resource",))
        return True, "Rolled back resources_REF insert."

    if _is_license_target(it_raw):
        ctx["resource_id"] = None
        ctx["license_server_id"] = None
        _pop_step_data_for_targets(run, wizard_def, ("license", "resource"))
        return True, "Rolled back license server (and resource, if present)."

    return True, None
