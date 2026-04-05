"""
Resolve field help from service ``guided_inputs`` metadata and JSON DB adapter.

Table/column names never come from the client; only field keys allowed by the
service definition are accepted, then server-side metadata selects the query.
"""


from typing import Any, Dict, List, Optional, Tuple

from services.adapters.db_base import DbAdapterBase

# Allowed help_mode values in service definitions
MODE_UNIQUE = "unique_values"
MODE_LOOKUP = "lookup_rows"


def iter_guided_fields(service_def: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a copy of ``guided_inputs`` list entries (empty if missing/invalid)."""
    raw = service_def.get("guided_inputs")
    if not isinstance(raw, list):
        return []
    return [f for f in raw if isinstance(f, dict) and f.get("key")]


def find_guided_field(
    service_def: Dict[str, Any], field_key: str
) -> Optional[Dict[str, Any]]:
    for f in iter_guided_fields(service_def):
        if f.get("key") == field_key:
            return f
    return None


def _row_numeric_id_value(row: Dict[str, Any]) -> Optional[int]:
    """Primary key from a ``clusters`` row (``ID`` / ``id`` / any key that uppercases to ``ID``)."""
    if not isinstance(row, dict):
        return None
    for rk, rv in row.items():
        if str(rk).upper() != "ID":
            continue
        try:
            i = int(rv)
            return i if i > 0 else None
        except (TypeError, ValueError):
            return None
    return None


_NAME_KEYS_PREFERRED = (
    "cluster",
    "cluster_name",
    "name",
    "clustername",
    "hostname",
)


def _lic_collector_cluster_display_name(row: Optional[Dict[str, Any]]) -> str:
    """
    Display name for licCollector help: known name columns first, then any non-ID string.
    Matches JSON mock (``cluster``) and common MariaDB variants (``name``, ``cluster_name``, …).
    """
    if not isinstance(row, dict) or not row:
        return ""
    lower_key = {str(k).lower(): k for k in row}
    for pref in _NAME_KEYS_PREFERRED:
        orig = lower_key.get(pref)
        if orig is None:
            continue
        rv = row.get(orig)
        if rv is None:
            continue
        s = str(rv).strip()
        if s:
            return s
    for rk, rv in row.items():
        if str(rk).upper() == "ID":
            continue
        if rv is None:
            continue
        s = str(rv).strip()
        if s:
            return s
    return ""


def _load_clusters_by_id_map(db: DbAdapterBase) -> Dict[int, Dict[str, Any]]:
    """
    Map ``clusters`` primary key → full row (same logical join as ``clusters.ID = licCollector_ID``).

    Tries column subsets that exist on JSON mock / MariaDB; ends with ``SELECT *``-equivalent load.
    """
    candidates = (
        ["ID", "cluster", "name", "cluster_name", "clusterName", "hostname"],
        ["ID", "cluster"],
        ["ID", "name"],
        ["ID", "cluster_name"],
        ["ID", "clusterName"],
        ["ID", "hostname"],
        ["ID"],
        None,
    )
    for colset in candidates:
        try:
            raw = db.get_all_rows("clusters", colset)
        except Exception:
            continue
        if not isinstance(raw, list):
            continue
        out: Dict[int, Dict[str, Any]] = {}
        for r in raw:
            if not isinstance(r, dict):
                continue
            rid = _row_numeric_id_value(r)
            if rid is None:
                continue
            out[rid] = r
        if out:
            return out
    return {}


def _build_lic_collector_id_help_rows(
    db: DbAdapterBase,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Distinct ``licCollector_ID`` from ``license_servers``, with names from ``clusters`` by ID match.

    Uses an in-memory map (equivalent to ``LEFT JOIN clusters c ON c.ID = ls.licCollector_ID``).
    """
    try:
        raw_ids = db.get_unique_values("license_servers", "licCollector_ID")
    except Exception as e:
        return [], "licCollector_ID help failed: %s" % e
    try:
        cluster_map = _load_clusters_by_id_map(db)
    except Exception as e:
        return [], "licCollector_ID help failed: %s" % e
    rows: List[Dict[str, Any]] = []
    seen = set()
    for raw in raw_ids:
        if raw is None:
            continue
        try:
            lid = int(raw)
        except (TypeError, ValueError):
            continue
        if lid in seen:
            continue
        seen.add(lid)
        crow = cluster_map.get(lid)
        label = _lic_collector_cluster_display_name(crow)
        rows.append({"ID": lid, "cluster": label})
    rows.sort(key=lambda r: int(r.get("ID") or 0))
    return rows, None


def build_help_payload(
    db: DbAdapterBase, field_def: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Build JSON-serializable help payload for one guided field.

    Returns (payload, error_message). error_message set => HTTP error body.
    """
    key = field_def.get("key")
    if not key:
        return None, "invalid field definition"

    if str(key) == "licCollector_ID":
        rows, err = _build_lic_collector_id_help_rows(db)
        if err:
            return None, err
        payload = {
            "mode": MODE_LOOKUP,
            "field": str(key),
            "rows": rows,
            "fill_column": "ID",
        }
        return payload, None

    mode = field_def.get("help_mode")
    if mode == MODE_UNIQUE:
        table = field_def.get("help_table") or field_def.get("table")
        column = field_def.get("help_column") or field_def.get("column")
        if not table or not column:
            return None, "help metadata incomplete"
        try:
            values = db.get_unique_values(str(table), str(column))
        except ValueError:
            return None, "invalid help table"
        fill_column = str(field_def.get("column") or field_def.get("key") or "")
        payload = {
            "mode": MODE_UNIQUE,
            "field": key,
            "values": values,
        }
        if fill_column:
            payload["fill_column"] = fill_column
        return payload, None

    if mode == MODE_LOOKUP:
        table = field_def.get("help_table")
        if not table:
            return None, "help metadata incomplete"
        cols = field_def.get("help_columns")
        col_list: Optional[List[str]] = None
        if isinstance(cols, list) and cols:
            col_list = [str(c) for c in cols]
        try:
            rows = db.get_all_rows(str(table), col_list)
        except ValueError:
            return None, "invalid help table"
        fill_column = str(field_def.get("column") or field_def.get("key") or "")
        payload = {
            "mode": MODE_LOOKUP,
            "field": key,
            "rows": rows,
        }
        if fill_column:
            payload["fill_column"] = fill_column
        return payload, None

    return None, "help not configured for this field"
