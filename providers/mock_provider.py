from typing import Dict, List

from core.models import Domain, FileRecord, Source
from providers.base import InventoryProvider


class MockInventoryProvider(InventoryProvider):
    def __init__(self) -> None:
        self.mock_inventory: Dict[str, List[Dict[str, str]]] = {
            "cluster_a": [
                {
                    "relative_path": "preexec.sh",
                    "checksum": "aaa111",
                    "size": 1200,
                    "mtime": "2026-03-27T10:00:00",
                },
                {
                    "relative_path": "profile.jobstarter",
                    "checksum": "bbb111",
                    "size": 800,
                    "mtime": "2026-03-20T09:00:00",
                },
            ],
            "cluster_b": [
                {
                    "relative_path": "preexec.sh",
                    "checksum": "aaa111",
                    "size": 1200,
                    "mtime": "2026-03-27T10:00:00",
                },
                {
                    "relative_path": "profile.jobstarter",
                    "checksum": "bbb222",
                    "size": 830,
                    "mtime": "2026-03-25T12:00:00",
                },
            ],
            "cluster_c": [
                {
                    "relative_path": "preexec.sh",
                    "checksum": "aaa999",
                    "size": 1400,
                    "mtime": "2026-03-28T08:15:00",
                }
            ],
            "gmc": [
                {
                    "relative_path": "preexec.sh",
                    "checksum": "aaa999",
                    "size": 1400,
                    "mtime": "2026-03-28T08:15:00",
                },
                {
                    "relative_path": "profile.jobstarter",
                    "checksum": "bbb222",
                    "size": 830,
                    "mtime": "2026-03-25T12:00:00",
                },
            ],
        }

        self.mock_contents: Dict[str, Dict[str, str]] = {
            "cluster_a": {
                "preexec.sh": "#!/bin/bash\necho cluster_a preexec\n",
                "profile.jobstarter": "export VAR=old\n",
            },
            "cluster_b": {
                "preexec.sh": "#!/bin/bash\necho cluster_a preexec\n",
                "profile.jobstarter": "export VAR=newer\n",
            },
            "cluster_c": {
                "preexec.sh": "#!/bin/bash\necho cluster_c improved preexec\n",
            },
            "gmc": {
                "preexec.sh": "#!/bin/bash\necho cluster_c improved preexec\n",
                "profile.jobstarter": "export VAR=newer\n",
            },
        }

    def scan_domain(
        self,
        source: Source,
        domain: Domain,
        path_input: str = "",
        recursive: bool = False,
    ) -> List[FileRecord]:
        rows = self.mock_inventory.get(source.id, [])
        records: List[FileRecord] = []

        for row in rows:
            relative_path = row["relative_path"]
            if path_input and path_input not in relative_path:
                continue

            root = domain.roots["gmc" if source.type == "gmc" else "cluster"]
            absolute_path = f"{root}/{relative_path}"

            records.append(
                FileRecord(
                    source_id=source.id,
                    domain_id=domain.id,
                    absolute_path=absolute_path,
                    relative_path=relative_path,
                    exists=True,
                    is_file=True,
                    checksum=row["checksum"],
                    size=row["size"],
                    mtime=row["mtime"],
                )
            )

        return records

    def read_file_content(
        self,
        source: Source,
        domain: Domain,
        relative_path: str,
    ) -> str:
        return self.mock_contents.get(source.id, {}).get(relative_path, "")