"""Resolve scan + matrix row for the file detail page."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from core.matrix import build_matrix_view
from core.models import (
    Domain,
    FileSourceDetailRow,
    LogicalFileGroup,
    MatrixRowView,
    ScanResult,
    Source,
)
from core.scanner import run_scan
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


def resolve_file_page(
    domain: Domain,
    relative_path: str,
    source_ids: List[str],
    path_input: str,
    recursive: bool,
    catalog_sources: List[Source],
    gold_map: Dict[str, str],
    provider: InventoryProvider,
) -> Optional[ResolvedFilePage]:
    selected_sources = [s for s in catalog_sources if s.id in source_ids]
    if not selected_sources:
        return None
    result = run_scan(
        provider=provider,
        domain=domain,
        sources=selected_sources,
        path_input=path_input,
        recursive=recursive,
    )
    matrix = build_matrix_view(
        result,
        selected_sources,
        provider,
        domain,
        catalog_sources,
        gold_map,
    )
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
    )
