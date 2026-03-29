
from abc import ABC, abstractmethod


class RequestDirAdapterBase(ABC):
    """Abstract adapter for ITSM or request directory layout."""

    @abstractmethod
    def request_exists(self, request_number: str) -> bool:
        """Return True if the request workspace exists on disk."""

    @abstractmethod
    def get_request_path(self, request_number: str) -> str:
        """Absolute path to the request folder (may not exist)."""
