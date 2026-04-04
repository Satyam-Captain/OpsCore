"""
Read-only SQL-style INSERT previews for GMCAssist DB steps (no execution).
"""

from typing import Any, Dict


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if value is True or value is False:
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    s = str(value).replace("'", "''")
    return "'%s'" % s


def format_insert_preview(table_name: str, row: Dict[str, Any]) -> str:
    """
    Build a readable INSERT preview from a row dict (e.g. ELIM preview rows).

    Table name is taken from server-side metadata only; not from user input paths.
    """
    tn = (table_name or "").strip()
    if not tn or not isinstance(row, dict):
        return ""
    cols = [str(k) for k in row.keys()]
    if not cols:
        return "-- (empty row)"
    col_line = ",\n  ".join(cols)
    val_line = ",\n  ".join(_sql_literal(row[k]) for k in row.keys())
    return "INSERT INTO %s (\n  %s\n)\nVALUES (\n  %s\n);" % (tn, col_line, val_line)
