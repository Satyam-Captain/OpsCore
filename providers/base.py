from abc import ABC, abstractmethod
from typing import List

from core.models import FileRecord, Source, Domain


class InventoryProvider(ABC):
    @abstractmethod
    def scan_domain(
        self,
        source: Source,
        domain: Domain,
        path_input: str = "",
        recursive: bool = False,
    ) -> List[FileRecord]:
        raise NotImplementedError

    @abstractmethod
    def read_file_content(
        self,
        source: Source,
        domain: Domain,
        relative_path: str,
    ) -> str:
        raise NotImplementedError