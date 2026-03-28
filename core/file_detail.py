"""Resolve scan + matrix row for the file detail page (from saved snapshot)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from core.models import (
    Domain,
    FileSourceDetailRow,
    LogicalFileGroup,
    MatrixRowView,
    ScanResult,
    Source,
)
from core.scan_snapshot import load_scan_snapshot
from providers.base import InventoryProvider


def _source_by_id(sources: List[Source], source_id: str) -> Optional[Source]:
    return next((s for s in sources if s.id == source_id), None)


def build_file_source_detail_rows(
    row: MatrixRowView,
    group: LogicalFileGroup,
    catalog_sources: List[Source],
    selected_order: List[str],
) -> List[FileSourceDetailRow]:
    rmap = {r.source_id: r for r in group.records}
    out: List[FileSourceDetailRow] = []
    for sid in selected_order:
        src = _source_by_id(catalog_sources, sid)
        label = f"{src.name} ({src.id})" if src else sid
        rec = rmap.get(sid)
        cell = row.cells[sid]

        if not rec or not rec.exists:
            out.append(
                FileSourceDetailRow(
                    source_id=sid,
                    source_label=label,
                    absolute_path="-",
                    checksum="-",
                    mtime="-",
                    size_display="-",
                    variant_label="-",
                    score_display=cell.score_display,
                    bucket=cell.bucket,
                    exists=False,
                    missing=True,
                )
            )
            continue

        size_display = str(rec.size) if rec.size is not None else "-"
        out.append(
            FileSourceDetailRow(
                source_id=sid,
                source_label=label,
                absolute_path=rec.absolute_path,
                checksum=rec.checksum or "-",
                mtime=rec.mtime or "-",
                size_display=size_display,
                variant_label=cell.variant_label or "-",
                score_display=cell.score_display,
                bucket=cell.bucket,
                exists=True,
                missing=False,
            )
        )
    return out


@dataclass
class ResolvedFilePage:
    result: ScanResult
    domain: Domain
    matrix_row: MatrixRowView
    group: LogicalFileGroup
    selected_sources: List[Source]
    provider: InventoryProvider
    detail_rows: List[FileSourceDetailRow]
    scan_id: str


def resolve_file_page(
    scan_id: str,
    domain: Domain,
    relative_path: str,
    catalog_sources: List[Source],
    provider: InventoryProvider,
) -> Optional[ResolvedFilePage]:
    """
    Build file detail from a persisted scan snapshot (no run_scan).

    Domain must match the snapshot's domain_id.
    """
    loaded = load_scan_snapshot(scan_id, catalog_sources)
    if not loaded:
        return None
    result, matrix = loaded
    if result.domain_id != domain.id:
        return None

    selected_sources = [s for s in catalog_sources if s.id in result.source_ids]
    if not selected_sources:
        return None

    row = next((r for r in matrix.rows if r.relative_path == relative_path), None)
    group = next((g for g in result.groups if g.relative_path == relative_path), None)
    if row is None or group is None:
        return None

    detail_rows = build_file_source_detail_rows(
        row, group, catalog_sources, result.source_ids
    )
    return ResolvedFilePage(
        result=result,
        domain=domain,
        matrix_row=row,
        group=group,
        selected_sources=selected_sources,
        provider=provider,
        detail_rows=detail_rows,
        scan_id=scan_id,
    )
