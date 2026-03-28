"""Build file × source matrix with baseline, variant labels, and similarity scores."""

from __future__ import annotations

import logging
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from core.gold_decisions import gold_source_for_file
from core.models import (
    Domain,
    FileRecord,
    LogicalFileGroup,
    MatrixRowView,
    MatrixView,
    ScanResult,
    Source,
    SourceCellInfo,
)
from providers.base import InventoryProvider
from providers.exceptions import SshReadError

logger = logging.getLogger(__name__)

BUCKET_EXACT = "exact"
BUCKET_SMALL = "small_diff"
BUCKET_MODERATE = "moderate_diff"
BUCKET_MAJOR = "major_diff"
BUCKET_MISSING = "missing"


def text_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    return float(SequenceMatcher(None, a, b).ratio())


def score_bucket(missing: bool, is_exact: bool, score: float) -> str:
    if missing:
        return BUCKET_MISSING
    if is_exact or score >= 1.0 - 1e-9:
        return BUCKET_EXACT
    if score >= 0.92:
        return BUCKET_SMALL
    if score >= 0.75:
        return BUCKET_MODERATE
    return BUCKET_MAJOR


def _checksum_variant_map(records: List[FileRecord]) -> Dict[Optional[str], str]:
    """Assign V1, V2, V3 … to distinct non-empty checksums (sorted for stability)."""
    distinct = sorted({r.checksum for r in records if r.exists and r.checksum})
    return {cs: f"V{i + 1}" for i, cs in enumerate(distinct)}


def _variant_label_for_record(
    record: Optional[FileRecord],
    checksum_to_label: Dict[Optional[str], str],
) -> str:
    if not record or not record.exists:
        return ""
    if record.checksum:
        return checksum_to_label.get(record.checksum, "?")
    return "?"


def _record_map(group: LogicalFileGroup) -> Dict[str, FileRecord]:
    return {r.source_id: r for r in group.records}


def _source_by_id(sources: List[Source], source_id: str) -> Optional[Source]:
    return next((s for s in sources if s.id == source_id), None)


def _format_gold_column(gold_source_id: Optional[str], catalog_sources: List[Source]) -> str:
    """Gold column reflects persisted JSON only; '-' when no decision exists."""
    if not gold_source_id:
        return "-"
    src = _source_by_id(catalog_sources, gold_source_id)
    if src:
        return f"{src.name} ({src.id})"
    return gold_source_id


def _resolve_baseline(
    group: LogicalFileGroup,
    selected_order: List[str],
    gold_source_id: Optional[str],
) -> Tuple[str, Optional[FileRecord], bool]:
    """Pick baseline source and whether it comes from a persisted gold decision.

    Gold JSON always wins for baseline when present. Otherwise use recommendation
    (dominant variant; trusted_reference may reorder candidates via recommender only).
    """
    rmap = _record_map(group)
    if gold_source_id:
        return gold_source_id, rmap.get(gold_source_id), True

    if group.candidate_source_ids:
        cid = group.candidate_source_ids[0]
        return cid, rmap.get(cid), False

    for r in group.records:
        if r.exists and r.checksum:
            return r.source_id, r, False
    for r in group.records:
        if r.exists:
            return r.source_id, r, False

    if selected_order:
        return selected_order[0], rmap.get(selected_order[0]), False
    return "", None, False


def _baseline_display(baseline_id: str, catalog_sources: List[Source]) -> str:
    src = _source_by_id(catalog_sources, baseline_id)
    if src:
        return f"{src.name} ({src.id})"
    return baseline_id or "-"


class _ContentCache:
    def __init__(self, provider: InventoryProvider, domain: Domain) -> None:
        self.provider = provider
        self.domain = domain
        self._cache: Dict[Tuple[str, str], str] = {}

    def get(self, source: Source, relative_path: str) -> str:
        key = (source.id, relative_path)
        if key not in self._cache:
            try:
                self._cache[key] = self.provider.read_file_content(
                    source, self.domain, relative_path
                )
            except SshReadError as exc:
                logger.warning(
                    "Skipping file %s for source %s: %s", relative_path, source.id, exc
                )
                self._cache[key] = ""
            except Exception as exc:
                logger.exception(
                    "Error reading file %s for source %s", relative_path, source.id
                )
                self._cache[key] = ""
        return self._cache[key]


def _build_row(
    group: LogicalFileGroup,
    catalog_sources: List[Source],
    selected_order: List[str],
    provider: InventoryProvider,
    domain: Domain,
    gold_map: Dict[str, str],
) -> MatrixRowView:
    rmap = _record_map(group)
    checksum_labels = _checksum_variant_map(group.records)
    gold_sid = gold_source_for_file(gold_map, group.domain_id, group.relative_path)
    baseline_id, baseline_record, baseline_from_gold_json = _resolve_baseline(
        group, selected_order, gold_sid
    )
    gold_display = _format_gold_column(gold_sid, catalog_sources)
    baseline_display = _baseline_display(baseline_id, catalog_sources)
    baseline_kind = "GOLD" if baseline_from_gold_json else "SUGGESTED"

    cache = _ContentCache(provider, domain)
    baseline_src = _source_by_id(catalog_sources, baseline_id) if baseline_id else None
    baseline_text = ""
    baseline_checksum: Optional[str] = None
    has_baseline = False
    if baseline_src and baseline_id:
        if baseline_record and baseline_record.exists:
            baseline_text = cache.get(baseline_src, group.relative_path)
            baseline_checksum = baseline_record.checksum
            has_baseline = True
        elif baseline_from_gold_json:
            baseline_text = cache.get(baseline_src, group.relative_path)
            baseline_checksum = baseline_record.checksum if baseline_record else None
            has_baseline = True

    cells: Dict[str, SourceCellInfo] = {}

    for sid in selected_order:
        src = _source_by_id(catalog_sources, sid)
        rec = rmap.get(sid)

        if not src or not rec or not rec.exists:
            cells[sid] = SourceCellInfo(
                variant_label="",
                score=None,
                score_display="-",
                bucket=BUCKET_MISSING,
                missing=True,
            )
            continue

        variant_label = _variant_label_for_record(rec, checksum_labels)

        if not has_baseline:
            cells[sid] = SourceCellInfo(
                variant_label=variant_label,
                score=None,
                score_display="-",
                bucket=BUCKET_MAJOR,
                missing=False,
            )
            continue

        if baseline_checksum and rec.checksum and rec.checksum == baseline_checksum:
            score = 1.0
            is_exact = True
        else:
            other_text = cache.get(src, group.relative_path)
            score = text_similarity(baseline_text, other_text)
            is_exact = score >= 1.0 - 1e-9

        bucket = score_bucket(False, is_exact, score)
        cells[sid] = SourceCellInfo(
            variant_label=variant_label,
            score=score,
            score_display=f"{score:.2f}",
            bucket=bucket,
            missing=False,
        )

    available_source_ids = [
        sid for sid in selected_order if sid in rmap and rmap[sid].exists
    ]

    return MatrixRowView(
        relative_path=group.relative_path,
        cells=cells,
        gold_display=gold_display,
        gold_source_id=gold_sid,
        available_source_ids=available_source_ids,
        baseline_display=baseline_display,
        baseline_kind=baseline_kind,
        status=group.status,
    )


def build_matrix_view(
    result: ScanResult,
    selected_sources: List[Source],
    provider: InventoryProvider,
    domain: Domain,
    catalog_sources: List[Source],
    gold_map: Dict[str, str],
) -> MatrixView:
    """Build matrix rows for each logical file in the scan."""
    selected_order = list(result.source_ids)
    rows = [
        _build_row(
            group,
            catalog_sources,
            selected_order,
            provider,
            domain,
            gold_map,
        )
        for group in result.groups
    ]
    column_sources = [s for s in selected_sources if s.id in set(selected_order)]
    order_index = {sid: i for i, sid in enumerate(selected_order)}
    column_sources.sort(key=lambda s: order_index.get(s.id, 999))
    return MatrixView(rows=rows, sources=column_sources)
