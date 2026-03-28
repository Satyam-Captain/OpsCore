import uuid
from typing import List

from core.comparator import group_records
from core.models import Domain, ScanResult, Source
from core.recommender import recommend_candidates
from providers.base import InventoryProvider


def run_scan(
    provider: InventoryProvider,
    domain: Domain,
    sources: List[Source],
    path_input: str = "",
    recursive: bool = False,
) -> ScanResult:
    all_records = []
    warnings: List[str] = []

    for source in sources:
        try:
            records = provider.scan_domain(
                source=source,
                domain=domain,
                path_input=path_input,
                recursive=recursive,
            )
            all_records.extend(records)
        except Exception as exc:
            warnings.append(f"{source.id}: scan failed: {exc}")

    result = ScanResult(
        scan_id=str(uuid.uuid4()),
        domain_id=domain.id,
        recursive=recursive,
        source_ids=[source.id for source in sources],
        path_input=path_input,
        warnings=warnings,
    )

    groups = group_records(all_records, result.source_ids)
    result.groups = [recommend_candidates(group, sources) for group in groups]

    return result