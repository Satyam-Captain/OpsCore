"""
GMCAssist: rollback DB inserts when navigating Back onto a completed ``db_insert`` step.

Uses the same delete-by-id mechanics as ELIM workspace rollback (``execute_elim_rollback``).
Does not touch publish/file rollback.
"""

from typing import Any, Dict, List, Optional, Tuple

from services.adapters import build_adapters
from services.elim_db import (
    TABLE_CLUSTER_RESOURCES,
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


def _is_cluster_resources_apply_step(step_def: Dict[str, Any]) -> bool:
    return str(step_def.get("type") or "").strip() == "cluster_resources_apply"


def _cluster_resources_apply_index(wizard_def: Dict[str, Any]) -> Optional[int]:
    steps = wizard_def.get("steps")
    if not isinstance(steps, list):
        return None
    for i, st in enumerate(steps):
        if isinstance(st, dict) and _is_cluster_resources_apply_step(st):
            return i
    return None


def _needs_cluster_resources_rollback(
    apply_index: Optional[int],
    from_index: int,
    target_index: int,
    raw_ids: Any,
) -> bool:
    """
    True when backing up past the cluster_resources apply step while inserted IDs are recorded.

    Covers:
    - from generate (or later) back onto apply (target <= apply_index < from)
    - from apply back onto form (from == apply_index, target < apply_index)
    - any further Back before apply with stale IDs (e.g. 12→11) so DB does not keep orphan rows
    """
    if apply_index is None:
        return False
    if not isinstance(raw_ids, list) or len(raw_ids) == 0:
        return False
    if from_index <= target_index:
        return False
    if from_index > apply_index and target_index <= apply_index:
        return True
    if from_index == apply_index and target_index < apply_index:
        return True
    if from_index < apply_index and target_index < apply_index:
        return True
    return False


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
        if not isinstance(st, dict):
            continue
        st_t = str(st.get("type") or "").strip()
        key_kind = None
        if st_t == "db_insert":
            it = _norm_insert_target(st.get("insert_target"))
            if _is_license_target(it):
                key_kind = "license"
            elif _is_resource_target(it):
                key_kind = "resource"
        elif st_t == "cluster_resources_apply":
            key_kind = "cluster_resources"
        if key_kind and key_kind in kinds_set:
            sid = str(st.get("id") or "").strip()
            if sid and sid in sd:
                del sd[sid]


def _rollback_cluster_resources_rows(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    wizard_def: Dict[str, Any],
    ctx: Dict[str, Any],
    raw_ids: List[Any],
) -> Tuple[bool, Optional[str]]:
    db, _, _ = build_adapters(settings)
    stack: List[Dict[str, Any]] = []
    for raw in reversed(raw_ids):
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            return False, "Invalid cluster_resource row id in wizard context; cannot roll back safely."
        stack.append({"type": "delete_by_id", "table": TABLE_CLUSTER_RESOURCES, "id": cid})
    results = execute_elim_rollback(db, stack)
    failed = [r for r in results if r.get("status") == "failed"]
    if failed:
        err_parts = [str(r.get("error")) for r in failed if r.get("error")]
        msg = "; ".join(err_parts) if err_parts else "Database delete failed."
        return (
            False,
            "Rollback failed (%s). Wizard state was not changed. "
            "Check DB connectivity, permissions, and whether the row still exists."
            % msg,
        )
    ctx["cluster_resource_row_ids"] = []
    _pop_step_data_for_targets(run, wizard_def, ("cluster_resources",))
    return True, "Rolled back cluster_resources inserts."


def maybe_rollback_before_navigate_back(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    wizard_def: Dict[str, Any],
    target_index: int,
    from_index: int,
) -> Tuple[bool, Optional[str]]:
    """
    Called when navigating Back from ``from_index`` to ``target_index`` (typically ``from_index - 1``).

    If ``steps[target_index]`` is a ``db_insert`` step whose row(s) are reflected in
    ``context`` / ``step_data``, delete those rows via the DB adapter.

    ``cluster_resources`` inserts are rolled back when backing up across the apply step
    (including from apply → form) while ``context.cluster_resource_row_ids`` is non-empty.

    Returns ``(True, None)`` if nothing to do or rollback succeeded without a user message.
    Returns ``(True, msg)`` on successful rollback (optional flash).
    Returns ``(False, err)`` on failure (do not change step index).
    """
    steps = wizard_def.get("steps")
    if not isinstance(steps, list) or target_index < 0 or target_index >= len(steps):
        return True, None
    if from_index < 0 or from_index >= len(steps):
        return True, None

    ctx = ensure_wizard_context(run)
    apply_i = _cluster_resources_apply_index(wizard_def)
    raw_cr = ctx.get("cluster_resource_row_ids")

    if _needs_cluster_resources_rollback(apply_i, from_index, target_index, raw_cr):
        return _rollback_cluster_resources_rows(
            settings, run, wizard_def, ctx, list(raw_cr)
        )

    # Landed on cluster_resources_apply with no inserted rows: clear stale step_data only.
    if apply_i is not None and target_index == apply_i:
        if not isinstance(raw_cr, list) or len(raw_cr) == 0:
            _pop_step_data_for_targets(run, wizard_def, ("cluster_resources",))
            return True, None

    step_def = steps[target_index]
    if not isinstance(step_def, dict):
        return True, None
    stype = str(step_def.get("type") or "").strip()
    if stype != "db_insert":
        return True, None

    it_raw = _norm_insert_target(step_def.get("insert_target"))
    if not it_raw:
        return True, None

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
