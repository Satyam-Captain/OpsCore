from collections import Counter
from typing import List

from core.models import LogicalFileGroup, Source


def recommend_candidates(group: LogicalFileGroup, sources: List[Source]) -> LogicalFileGroup:
    checksum_counter = Counter(
        record.checksum for record in group.records if record.checksum
    )

    trusted_sources = {source.id for source in sources if source.trusted_reference}

    if not checksum_counter:
        group.notes.append("No checksum data available for recommendation.")
        return group

    most_common_checksum, _ = checksum_counter.most_common(1)[0]
    candidates = [
        record for record in group.records
        if record.checksum == most_common_checksum
    ]

    trusted_candidates = [record for record in candidates if record.source_id in trusted_sources]

    if trusted_candidates:
        group.candidate_source_ids = [record.source_id for record in trusted_candidates]
        group.notes.append("Trusted reference source matches the dominant variant.")
    else:
        group.candidate_source_ids = [record.source_id for record in candidates]
        group.notes.append("Dominant content variant selected as recommendation.")

    return group