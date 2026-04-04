"""
GMCAssist: ``cluster_resources`` rows (cluster_ID, resource_ID, resource_value) before lsf.cluster.

Assumes table ``cluster_resources`` has integer auto-increment primary key ``ID`` (MariaDB + JSON mock).
"""

from typing import Any, Dict, List, Optional, Tuple

from services.elim_db import TABLE_CLUSTER_RESOURCES


def wizard_def_includes_cluster_resources(wizard_def: Dict[str, Any]) -> bool:
    """True if this wizard defines the cluster_resources form/apply steps."""
    steps = wizard_def.get("steps")
    if not isinstance(steps, list):
        return False
    for s in steps:
        if not isinstance(s, dict):
            continue
        t = str(s.get("type") or "").strip()
        if t in ("cluster_resources_form", "cluster_resources_apply"):
            return True
    return False


def _cluster_row_display_name(row: Dict[str, Any]) -> str:
    """First non-ID string value (handles MariaDB key casing)."""
    best = ""
    for rk, rv in row.items():
        if str(rk).upper() == "ID":
            continue
        if rv is None:
            continue
        s = str(rv).strip()
        if s:
            # Prefer conventional name column when multiple keys exist
            lk = str(rk).lower()
            if lk == "cluster":
                return s
            if not best:
                best = s
    return best


def cluster_help_rows_from_db(db: Any) -> List[Dict[str, Any]]:
    """
    Rows for cluster_ID picker help: numeric ``ID`` (filled into the form) and ``cluster`` name (display).

    Tries common ``clusters`` column sets; ``ID`` + ``cluster`` matches JSON mock and typical MariaDB schema.
    """
    candidates = (
        ["ID", "cluster"],
        ["ID", "name"],
        ["ID", "clusterName"],
        ["ID", "hostname"],
        ["ID"],
    )
    for cols in candidates:
        try:
            raw = db.get_all_rows("clusters", cols)
        except Exception:
            continue
        if not isinstance(raw, list):
            continue
        out: List[Dict[str, Any]] = []
        for r in raw:
            if not isinstance(r, dict):
                continue
            try:
                cid = int(r.get("ID"))
            except (TypeError, ValueError):
                continue
            if cid <= 0:
                continue
            cname = _cluster_row_display_name(r)
            out.append({"ID": cid, "cluster": cname})
        if out:
            out.sort(key=lambda x: int(x.get("ID") or 0))
            return out
    return []


def parse_cluster_resources_form(form: Any) -> Tuple[List[str], List[str]]:
    """Return parallel lists from repeated form fields."""
    ids_c: List[str] = []
    vals: List[str] = []
    if form is not None:
        try:
            ids_c = [str(x).strip() for x in form.getlist("cr_cluster_id")]
        except (AttributeError, TypeError):
            ids_c = []
        try:
            vals = [str(x).strip() for x in form.getlist("cr_resource_value")]
        except (AttributeError, TypeError):
            vals = []
    return ids_c, vals


def build_cluster_resources_rows(
    cluster_ids_raw: List[str],
    resource_values_raw: List[str],
    resource_id: int,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Build row dicts for insert. Returns (rows, errors).

    Skips completely empty pairs; requires at least one non-empty row.
    """
    errors: List[str] = []
    n = max(len(cluster_ids_raw), len(resource_values_raw))
    rows: List[Dict[str, Any]] = []
    for i in range(n):
        c_raw = cluster_ids_raw[i] if i < len(cluster_ids_raw) else ""
        v_raw = resource_values_raw[i] if i < len(resource_values_raw) else ""
        if not c_raw and not v_raw:
            continue
        if not c_raw:
            errors.append("Row %s: cluster_ID is required." % (i + 1,))
            continue
        if not v_raw:
            errors.append("Row %s: resource_value is required." % (i + 1,))
            continue
        try:
            cid = int(c_raw, 10)
        except ValueError:
            errors.append("Row %s: cluster_ID must be an integer." % (i + 1,))
            continue
        if cid <= 0:
            errors.append("Row %s: cluster_ID must be positive." % (i + 1,))
            continue
        rows.append(
            {
                "cluster_ID": cid,
                "resource_ID": int(resource_id),
                "resource_value": v_raw,
            }
        )
    if not rows and not errors:
        errors.append("Add at least one cluster_resources row (cluster_ID and resource_value).")
    return rows, errors


def validate_cluster_resources_prereq(resource_id: Optional[int]) -> Optional[str]:
    if resource_id is None:
        return "resource_id is missing; complete resources_REF apply first."
    try:
        rid = int(resource_id)
    except (TypeError, ValueError):
        return "resource_id in context is invalid."
    if rid <= 0:
        return "resource_id must be a positive integer."
    return None


def apply_cluster_resources_batch(
    db: Any, rows: List[Dict[str, Any]]
) -> Tuple[bool, List[int], str]:
    """
    Insert all rows; on any failure roll back inserts from this batch only.

    Returns (ok, inserted_ids, error_message).
    """
    if not rows:
        return False, [], "No rows to insert."
    inserted: List[int] = []
    for row in rows:
        try:
            new_id = db.insert_row(TABLE_CLUSTER_RESOURCES, dict(row))
            inserted.append(int(new_id))
        except (OSError, ValueError, TypeError, KeyError) as e:
            for rid in reversed(inserted):
                try:
                    db.delete_row_by_id(TABLE_CLUSTER_RESOURCES, rid)
                except (OSError, ValueError, TypeError):
                    pass
            return False, [], str(e)
    return True, inserted, ""


def pending_cluster_resources_from_run(run: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = run.get("pending_cluster_resources")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            out.append(dict(item))
    return out
