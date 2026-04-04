"""
Prototype GMCAssist superadmin: session elevation and active-run DB rollback + abandoned mark.

Password: environment variable ``GMCASSIST_SUPERADMIN_PASSWORD``, else fallback ``satyam``.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

SESSION_KEY_SUPERADMIN = "gmcassist_superadmin"

_ENV_PASSWORD = "GMCASSIST_SUPERADMIN_PASSWORD"
_FALLBACK_PASSWORD = "satyam"

_PREVIEW_CLUSTER_RESOURCES = ("ID", "cluster_ID", "resource_ID", "resource_value")
_PREVIEW_RESOURCES_REF = ("ID", "resource", "description", "license_server_ID")
_PREVIEW_LICENSE_SERVERS = ("ID", "application", "site", "company")


def configured_superadmin_password() -> str:
    raw = os.environ.get(_ENV_PASSWORD)
    if raw is not None and str(raw).strip():
        return str(raw).strip()
    return _FALLBACK_PASSWORD


def verify_superadmin_password(candidate: str) -> bool:
    return (candidate or "") == configured_superadmin_password()


def session_is_gmcassist_superadmin(session_obj: Any) -> bool:
    try:
        return bool(session_obj.get(SESSION_KEY_SUPERADMIN))
    except (AttributeError, TypeError):
        return False


def _subset_row(row: Optional[Dict[str, Any]], keys: Tuple[str, ...]) -> Dict[str, Any]:
    if not row:
        return {}
    out: Dict[str, Any] = {}
    for k in keys:
        if k in row:
            out[k] = row.get(k)
    return out


def build_superadmin_reset_preview(
    db: Any, run: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Ordered sections for UI: cluster_resources rows, then resources_REF, then license_servers.

    Each section: ``{"table": str, "rows": [{"id": int, "fields": dict}, ...]}``.
    """
    sections: List[Dict[str, Any]] = []
    ctx = run.get("context") if isinstance(run.get("context"), dict) else {}

    cr_ids = ctx.get("cluster_resource_row_ids")
    if isinstance(cr_ids, list) and cr_ids:
        block: Dict[str, Any] = {"table": "cluster_resources", "rows": []}
        for raw in cr_ids:
            try:
                pk = int(raw)
            except (TypeError, ValueError):
                continue
            row = None
            try:
                row = db.get_row_by_id("cluster_resources", pk)
            except (ValueError, TypeError, OSError):
                row = None
            block["rows"].append(
                {"id": pk, "fields": _subset_row(row, _PREVIEW_CLUSTER_RESOURCES)}
            )
        if block["rows"]:
            sections.append(block)

    rr_raw = ctx.get("resource_id")
    if rr_raw is not None:
        try:
            rri = int(rr_raw)
        except (TypeError, ValueError):
            rri = None
        if rri is not None:
            row = None
            try:
                row = db.get_row_by_id("resources_REF", rri)
            except (ValueError, TypeError, OSError):
                row = None
            sections.append(
                {
                    "table": "resources_REF",
                    "rows": [{"id": rri, "fields": _subset_row(row, _PREVIEW_RESOURCES_REF)}],
                }
            )

    ls_raw = ctx.get("license_server_id")
    if ls_raw is not None:
        try:
            lsi = int(ls_raw)
        except (TypeError, ValueError):
            lsi = None
        if lsi is not None:
            row = None
            try:
                row = db.get_row_by_id("license_servers", lsi)
            except (ValueError, TypeError, OSError):
                row = None
            sections.append(
                {
                    "table": "license_servers",
                    "rows": [{"id": lsi, "fields": _subset_row(row, _PREVIEW_LICENSE_SERVERS)}],
                }
            )

    return sections


def apply_superadmin_db_rollback_from_run(db: Any, run: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Delete DB rows in dependency order: cluster_resources, resources_REF, license_servers.

    Returns ``(True, "")`` on success, or ``(False, message)``.
    """
    ctx = run.get("context") if isinstance(run.get("context"), dict) else {}

    cr_ids = ctx.get("cluster_resource_row_ids")
    if not isinstance(cr_ids, list):
        cr_ids = []
    for raw in reversed(list(cr_ids)):
        try:
            pk = int(raw)
        except (TypeError, ValueError):
            continue
        try:
            db.delete_row_by_id("cluster_resources", pk)
        except (ValueError, TypeError, OSError) as e:
            return False, "cluster_resources rollback failed (ID %s): %s" % (pk, e)

    rr_raw = ctx.get("resource_id")
    if rr_raw is not None:
        try:
            rri = int(rr_raw)
        except (TypeError, ValueError):
            return False, "Invalid resource_id in wizard context."
        try:
            db.delete_row_by_id("resources_REF", rri)
        except (ValueError, TypeError, OSError) as e:
            return False, "resources_REF rollback failed: %s" % e

    ls_raw = ctx.get("license_server_id")
    if ls_raw is not None:
        try:
            lsi = int(ls_raw)
        except (TypeError, ValueError):
            return False, "Invalid license_server_id in wizard context."
        try:
            db.delete_row_by_id("license_servers", lsi)
        except (ValueError, TypeError, OSError) as e:
            return False, "license_servers rollback failed: %s" % e

    return True, ""


def build_superadmin_reset_audit(run: Dict[str, Any]) -> Dict[str, Any]:
    ctx = run.get("context") if isinstance(run.get("context"), dict) else {}
    return {
        "prior_cluster_resource_row_ids": list(ctx.get("cluster_resource_row_ids") or []),
        "prior_resource_id": ctx.get("resource_id"),
        "prior_license_server_id": ctx.get("license_server_id"),
        "prior_request_number": str(run.get("request_number") or ""),
    }
