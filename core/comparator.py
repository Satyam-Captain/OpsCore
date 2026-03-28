from collections import defaultdict
from typing import Dict, List

from core.models import FileRecord, LogicalFileGroup


def group_records(records: List[FileRecord], selected_source_ids: List[str]) -> List[LogicalFileGroup]:
    grouped: Dict[str, List[FileRecord]] = defaultdict(list)

    for record in records:
        key = f"{record.domain_id}::{record.relative_path}"
        grouped[key].append(record)

    results: List[LogicalFileGroup] = []

    for key, file_records in grouped.items():
        domain_id, relative_path = key.split("::", 1)
        group = LogicalFileGroup(
            domain_id=domain_id,
            relative_path=relative_path,
            records=file_records,
        )

        present_sources = {r.source_id for r in file_records if r.exists}
        missing_sources = set(selected_source_ids) - present_sources
        checksums = {r.checksum for r in file_records if r.checksum}

        if missing_sources:
            group.status = "MISSING"
            group.notes.append("Missing on one or more selected sources.")
        elif len(checksums) == 1:
            group.status = "HARMONIZED"
            group.notes.append("All selected sources have identical content.")
        elif len(checksums) > 1:
            group.status = "DRIFTED"
            group.notes.append("Multiple content variants detected.")
        else:
            group.status = "UNCERTAIN"
            group.notes.append("Unable to determine status.")

        results.append(group)

    return sorted(results, key=lambda item: item.relative_path)