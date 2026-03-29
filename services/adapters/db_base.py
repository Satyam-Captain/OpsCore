"""Abstract DB adapter for service workflows (MariaDB later, JSON mock now)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DbAdapterBase(ABC):
    @abstractmethod
    def get_unique_values(self, table_name: str, column_name: str) -> List[Any]:
        """Distinct non-null values for column_name in table_name."""

    @abstractmethod
    def get_all_rows(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """All rows from table; if columns set, each row is restricted to those keys."""

    @abstractmethod
    def insert_row(self, table_name: str, row_dict: Dict[str, Any]) -> int:
        """Insert a row; return primary key (stub may no-op and return 0)."""

    @abstractmethod
    def get_row_by_id(self, table_name: str, row_id: int) -> Optional[Dict[str, Any]]:
        """Fetch row where conventional ID matches row_id."""

    @abstractmethod
    def delete_row_by_id(self, table_name: str, row_id: int) -> bool:
        """Remove row with matching ``ID``; return True if a row was removed."""
