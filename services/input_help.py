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
        return {
            "mode": MODE_UNIQUE,
            "field": key,
            "values": values,
        }, None

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
        return {
            "mode": MODE_LOOKUP,
            "field": key,
            "rows": rows,
        }, None

    return None, "help not configured for this field"
