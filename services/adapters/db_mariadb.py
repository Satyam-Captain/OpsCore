"""
MariaDB adapter (read-only first pass): guided input help and blueprint prefill against tLSF.

Writes (insert/delete) are not supported; callers that need apply/rollback should use
``service_db_backend=json`` until write support is added.
"""


import os
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional

from services.adapters.db_base import DbAdapterBase

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_ident(name: str, kind: str) -> str:
    if not name or not _SAFE_IDENT.match(name):
        raise ValueError(f"invalid {kind}: {name!r}")
    return name


def _quote_ident(name: str, kind: str) -> str:
    _validate_ident(name, kind)
    return f"`{name}`"


def _json_safe_cell(value: Any) -> Any:
    """Match JSON-ish shapes from JsonFileDbAdapter (numbers, strings, ISO dates)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    # datetime.date / datetime.datetime from pymysql
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            return str(value)
    return value


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe_cell(v) for k, v in row.items()}


class MariaDbAdapter(DbAdapterBase):
    """Read-only MariaDB access; connection parameters come from settings / environment."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connect_timeout: int = 10,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._user = user
        self._password = password
        self._database = database
        self._connect_timeout = int(connect_timeout)

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "MariaDbAdapter":
        host = str(settings.get("service_mariadb_host") or "").strip()
        if not host:
            raise ValueError(
                "service_mariadb_host is required when service_db_backend is mariadb"
            )
        port_raw = settings.get("service_mariadb_port", 3306)
        try:
            port = int(port_raw)
        except (TypeError, ValueError) as e:
            raise ValueError("service_mariadb_port must be an integer") from e
        user = str(settings.get("service_mariadb_user") or "").strip()
        if not user:
            raise ValueError(
                "service_mariadb_user is required when service_db_backend is mariadb"
            )
        pw = os.environ.get("OPSCORE_MARIADB_PASSWORD")
        if pw is None:
            pw = settings.get("service_mariadb_password")
        if pw is None:
            pw = ""
        password = str(pw)
        dbname = str(settings.get("service_mariadb_database") or "tLSF").strip()
        if not dbname:
            raise ValueError("service_mariadb_database must not be empty")
        ct_raw = settings.get("service_mariadb_connect_timeout", 10)
        try:
            connect_timeout = int(ct_raw)
        except (TypeError, ValueError) as e:
            raise ValueError("service_mariadb_connect_timeout must be an integer") from e
        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=dbname,
            connect_timeout=max(1, connect_timeout),
        )

    def _connect(self) -> Any:
        try:
            import pymysql
            from pymysql.cursors import DictCursor
        except ImportError as e:
            raise ImportError(
                "pymysql is required for service_db_backend=mariadb. "
                "Install with: pip install pymysql"
            ) from e
        return pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            cursorclass=DictCursor,
            connect_timeout=self._connect_timeout,
            autocommit=True,
        )

    def get_table_columns(self, table_name: str) -> List[str]:
        """Column names in ordinal order for ``table_name``."""
        _validate_ident(table_name, "table_name")
        sql = (
            "SELECT COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION"
        )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (self._database, table_name))
                rows = cur.fetchall()
        finally:
            conn.close()
        out: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = row.get("COLUMN_NAME")
            if isinstance(name, str) and name:
                out.append(name)
        return out

    def get_primary_key_columns(self, table_name: str) -> List[str]:
        """Primary key columns in key order for ``table_name``."""
        _validate_ident(table_name, "table_name")
        sql = (
            "SELECT COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY' "
            "ORDER BY ORDINAL_POSITION"
        )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (self._database, table_name))
                rows = cur.fetchall()
        finally:
            conn.close()
        out: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = row.get("COLUMN_NAME")
            if isinstance(name, str) and name:
                out.append(name)
        return out

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Column-level schema rows for compatibility checks.

        Each row includes: column_name, data_type, is_nullable, column_default, column_key, extra.
        """
        _validate_ident(table_name, "table_name")
        sql = (
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION"
        )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (self._database, table_name))
                rows = cur.fetchall()
        finally:
            conn.close()
        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            out.append(
                {
                    "column_name": str(row.get("COLUMN_NAME") or ""),
                    "data_type": str(row.get("DATA_TYPE") or ""),
                    "is_nullable": str(row.get("IS_NULLABLE") or ""),
                    "column_default": _json_safe_cell(row.get("COLUMN_DEFAULT")),
                    "column_key": str(row.get("COLUMN_KEY") or ""),
                    "extra": str(row.get("EXTRA") or ""),
                }
            )
        return out

    def get_unique_values(self, table_name: str, column_name: str) -> List[Any]:
        tq = _quote_ident(table_name, "table_name")
        cq = _quote_ident(column_name, "column_name")
        sql = (
            f"SELECT DISTINCT {cq} AS v FROM {tq} "
            f"WHERE {cq} IS NOT NULL ORDER BY {cq}"
        )
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        finally:
            conn.close()
        seen = set()
        out: List[Any] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            v = r.get("v")
            if v is None:
                continue
            key = repr(v)
            if key not in seen:
                seen.add(key)
                out.append(_json_safe_cell(v))
        return out

    def get_all_rows(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        tq = _quote_ident(table_name, "table_name")
        if columns:
            parts = [_quote_ident(c, "column") for c in columns]
            col_sql = ", ".join(parts)
            sql = f"SELECT {col_sql} FROM {tq}"
        else:
            sql = f"SELECT * FROM {tq}"
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                raw_rows = cur.fetchall()
        finally:
            conn.close()
        if not isinstance(raw_rows, list):
            return []
        return [_normalize_row(dict(r)) for r in raw_rows if isinstance(r, dict)]

    def get_sample_row(self, table_name: str) -> Optional[Dict[str, Any]]:
        tq = _quote_ident(table_name, "table_name")
        sql = f"SELECT * FROM {tq} LIMIT 1"
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                one = cur.fetchone()
        finally:
            conn.close()
        if not one or not isinstance(one, dict):
            return None
        return _normalize_row(dict(one))

    def get_row_by_id(self, table_name: str, row_id: int) -> Optional[Dict[str, Any]]:
        try:
            target = int(row_id)
        except (TypeError, ValueError):
            return None
        tq = _quote_ident(table_name, "table_name")
        idq = _quote_ident("ID", "column_name")
        sql = f"SELECT * FROM {tq} WHERE {idq} = %s LIMIT 1"
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (target,))
                one = cur.fetchone()
        finally:
            conn.close()
        if not one or not isinstance(one, dict):
            return None
        return _normalize_row(dict(one))

    def insert_row(self, table_name: str, row_dict: Dict[str, Any]) -> int:
        _validate_ident(table_name, "table_name")
        raise ValueError(
            "MariaDB adapter is read-only; database apply/insert requires "
            "service_db_backend=json until write support is implemented."
        )

    def delete_row_by_id(self, table_name: str, row_id: int) -> bool:
        _validate_ident(table_name, "table_name")
        raise ValueError(
            "MariaDB adapter is read-only; database rollback/delete requires "
            "service_db_backend=json until write support is implemented."
        )
