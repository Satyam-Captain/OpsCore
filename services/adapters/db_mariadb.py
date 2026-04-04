"""
MariaDB adapter for tLSF: reads for guided input / prefill; incremental writes.

Write support: ``license_servers`` and ``resources_REF`` insert/delete by numeric primary key
``ID`` only. Any other table raises ``ValueError``.
"""


import os
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from services.adapters.db_base import DbAdapterBase

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Writable in this step only; must match ``LICENSE_SERVER_KEYS`` in ``elim_db`` (no ID — DB auto-increment).
_LICENSE_SERVERS_INSERT_COLUMNS = (
    "application",
    "site",
    "licCollector_ID",
    "company",
    "licType",
    "port",
    "ready4MIP",
    "licservers",
    "features",
)

# Must match ``TABLE_*`` / column lists in ``elim_db`` (no ``ID`` in payload — DB auto-increment).
_MARIADB_WRITE_TABLES = frozenset({"license_servers", "resources_REF"})

# Same order as ``RESOURCE_REF_KEYS_BODY`` + ``license_server_ID`` in ``elim_db.build_resource_ref_row``.
_RESOURCES_REF_INSERT_COLUMNS = (
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
    "license_server_ID",
)


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
    """MariaDB access: reads everywhere; narrow allowlisted writes for ELIM/LS tables."""

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

    def _reject_unsupported_write_table(self, table_name: str, operation: str) -> None:
        _validate_ident(table_name, "table_name")
        if table_name not in _MARIADB_WRITE_TABLES:
            raise ValueError(
                "MariaDB %s is only implemented for tables %s; %r is not supported "
                "(use service_db_backend=json for other tables)."
                % (operation, ", ".join(sorted(_MARIADB_WRITE_TABLES)), table_name)
            )

    def _bind_license_servers_insert_values(
        self, row_dict: Dict[str, Any]
    ) -> Tuple[List[str], List[Any]]:
        """Return (ordered column names, values) for parameterized INSERT."""
        if not isinstance(row_dict, dict):
            raise ValueError("row_dict must be a dict")
        clean = dict(row_dict)
        clean.pop("ID", None)
        missing = [c for c in _LICENSE_SERVERS_INSERT_COLUMNS if c not in clean]
        if missing:
            raise ValueError(
                "license_servers insert missing required columns: %s" % (", ".join(missing),)
            )
        extra = [k for k in clean if k not in _LICENSE_SERVERS_INSERT_COLUMNS]
        if extra:
            raise ValueError(
                "license_servers insert has unknown columns (not inserted): %s"
                % (", ".join(sorted(extra)),)
            )
        cols = list(_LICENSE_SERVERS_INSERT_COLUMNS)
        values = [clean[c] for c in cols]
        return cols, values

    def _bind_resources_ref_insert_values(
        self, row_dict: Dict[str, Any]
    ) -> Tuple[List[str], List[Any]]:
        """Return (ordered column names, values) for parameterized INSERT."""
        if not isinstance(row_dict, dict):
            raise ValueError("row_dict must be a dict")
        clean = dict(row_dict)
        clean.pop("ID", None)
        missing = [c for c in _RESOURCES_REF_INSERT_COLUMNS if c not in clean]
        if missing:
            raise ValueError(
                "resources_REF insert missing required columns: %s" % (", ".join(missing),)
            )
        extra = [k for k in clean if k not in _RESOURCES_REF_INSERT_COLUMNS]
        if extra:
            raise ValueError(
                "resources_REF insert has unknown columns (not inserted): %s"
                % (", ".join(sorted(extra)),)
            )
        cols = list(_RESOURCES_REF_INSERT_COLUMNS)
        values = [clean[c] for c in cols]
        return cols, values

    def insert_row(self, table_name: str, row_dict: Dict[str, Any]) -> int:
        """
        Insert one row; return new primary key.

        Assumes the table has an auto-increment (or equivalent) integer primary key column
        named ``ID`` so ``cursor.lastrowid`` is meaningful.
        """
        self._reject_unsupported_write_table(table_name, "insert_row")
        try:
            from pymysql.err import Error as PyMySQLError
        except ImportError as e:
            raise ImportError(
                "pymysql is required for service_db_backend=mariadb. "
                "Install with: pip install pymysql"
            ) from e

        if table_name == "license_servers":
            cols, values = self._bind_license_servers_insert_values(row_dict)
        elif table_name == "resources_REF":
            cols, values = self._bind_resources_ref_insert_values(row_dict)
        else:
            raise ValueError(
                "MariaDB insert_row: table %r has no column binding (adapter bug)."
                % (table_name,)
            )

        tq = _quote_ident(table_name, "table_name")
        col_sql = ", ".join(_quote_ident(c, "column") for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (tq, col_sql, placeholders)

        conn = self._connect()
        try:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, tuple(values))
                    new_id = int(cur.lastrowid)
            except PyMySQLError as e:
                raise ValueError(
                    "MariaDB insert into %s failed: %s" % (table_name, e)
                ) from e
        finally:
            conn.close()

        if new_id <= 0:
            raise ValueError(
                "MariaDB insert into %s returned no auto-increment ID (lastrowid=%s); "
                "confirm table uses an integer AUTO_INCREMENT primary key named ID."
                % (table_name, new_id)
            )
        return new_id

    def delete_row_by_id(self, table_name: str, row_id: int) -> bool:
        """Delete by ``ID`` column; allowlisted tables only."""
        self._reject_unsupported_write_table(table_name, "delete_row_by_id")
        try:
            target = int(row_id)
        except (TypeError, ValueError):
            return False
        if target <= 0:
            return False

        try:
            from pymysql.err import Error as PyMySQLError
        except ImportError as e:
            raise ImportError(
                "pymysql is required for service_db_backend=mariadb. "
                "Install with: pip install pymysql"
            ) from e

        tq = _quote_ident(table_name, "table_name")
        idq = _quote_ident("ID", "column_name")
        sql = "DELETE FROM %s WHERE %s = %%s LIMIT 1" % (tq, idq)

        conn = self._connect()
        try:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, (target,))
                    affected = int(cur.rowcount)
            except PyMySQLError as e:
                raise ValueError(
                    "MariaDB delete from %s failed: %s" % (table_name, e)
                ) from e
        finally:
            conn.close()

        return affected > 0
