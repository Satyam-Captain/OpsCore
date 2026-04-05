
import json
import os
from typing import Any, Dict, List, Optional, Union

from services.adapters.db_base import DbAdapterBase


class JsonFileDbAdapter(DbAdapterBase):
    """JSON list-of-rows files in db_dir, one file per table: tablename.json."""

    def __init__(self, db_dir: str) -> None:
        self._db_dir = db_dir

    def _path(self, table_name: str) -> str:
        safe = table_name.replace(os.sep, "").replace("/", "").replace("\\", "")
        if not safe or safe != table_name:
            raise ValueError("invalid table_name")
        return os.path.join(self._db_dir, f"{safe}.json")

    def _load_rows(self, table_name: str) -> List[Dict[str, Any]]:
        path = self._path(table_name)
        if not os.path.isfile(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict) and "rows" in data and isinstance(data["rows"], list):
            return [r for r in data["rows"] if isinstance(r, dict)]
        return []

    def _save_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        path = self._path(table_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)

    def get_unique_values(self, table_name: str, column_name: str) -> List[Any]:
        seen = set()
        out: List[Any] = []
        for row in self._load_rows(table_name):
            if column_name not in row:
                continue
            v = row[column_name]
            if v is None:
                continue
            key = repr(v)
            if key not in seen:
                seen.add(key)
                out.append(v)
        return out

    def get_all_rows(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        rows = self._load_rows(table_name)
        if not columns:
            return [dict(r) for r in rows]
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({c: r.get(c) for c in columns})
        return out

    def get_sample_row(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Return a shallow copy of the first data row, or None if the table is empty. Read-only."""
        rows = self._load_rows(table_name)
        if not rows:
            return None
        return dict(rows[0])

    def get_row_by_id(self, table_name: str, row_id: int) -> Optional[Dict[str, Any]]:
        try:
            target = int(row_id)
        except (TypeError, ValueError):
            return None
        for row in self._load_rows(table_name):
            if self._id_matches(row, target):
                return dict(row)
        return None

    @staticmethod
    def _id_matches(row: Dict[str, Any], target: int) -> bool:
        v: Union[int, str, None] = row.get("ID")
        if v is None:
            return False
        try:
            return int(v) == target
        except (TypeError, ValueError):
            return False

    def delete_row_by_id(self, table_name: str, row_id: int) -> bool:
        """
        Drop the first row whose ``ID`` matches ``row_id`` (coerced to int).
        Returns False if the row was not found or the table file is unusable.
        """
        try:
            target = int(row_id)
        except (TypeError, ValueError):
            return False
        try:
            rows = self._load_rows(table_name)
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        new_rows = [r for r in rows if not self._id_matches(r, target)]
        if len(new_rows) == len(rows):
            return False
        try:
            self._save_rows(table_name, new_rows)
        except OSError:
            return False
        return True

    def delete_cluster_resources_by_composite(
        self, cluster_id: int, resource_id: int, resource_value: str
    ) -> bool:
        """Remove the first row matching ``cluster_ID``, ``resource_ID``, ``resource_value``."""
        try:
            cid = int(cluster_id)
            rid = int(resource_id)
        except (TypeError, ValueError):
            return False
        val = resource_value if isinstance(resource_value, str) else str(resource_value)
        try:
            rows = self._load_rows("cluster_resources")
        except (OSError, ValueError, json.JSONDecodeError):
            return False
        new_rows: List[Dict[str, Any]] = []
        removed = False
        for row in rows:
            if not isinstance(row, dict):
                new_rows.append(row)
                continue
            if removed:
                new_rows.append(row)
                continue
            try:
                rc = int(row.get("cluster_ID"))
                rr = int(row.get("resource_ID"))
            except (TypeError, ValueError):
                new_rows.append(row)
                continue
            rv = row.get("resource_value")
            rs = rv if isinstance(rv, str) else ("" if rv is None else str(rv))
            if rc == cid and rr == rid and rs == val:
                removed = True
                continue
            new_rows.append(row)
        if not removed:
            return False
        try:
            self._save_rows("cluster_resources", new_rows)
        except OSError:
            return False
        return True

    def insert_row(self, table_name: str, row_dict: Dict[str, Any]) -> int:
        rows = self._load_rows(table_name)
        max_id = 0
        for r in rows:
            rid = r.get("ID")
            if isinstance(rid, int) and rid > max_id:
                max_id = rid
        new_id = max_id + 1
        new_row = dict(row_dict)
        new_row["ID"] = new_id
        rows.append(new_row)
        self._save_rows(table_name, rows)
        return new_id
