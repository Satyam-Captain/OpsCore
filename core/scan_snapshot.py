"""
Persist and reload scan + matrix views as JSON under data/scans/<scan_id>.json.

Used so file detail and diff reuse one scan without calling run_scan again.
Gold/matrix cells reflect the state at save time (see product requirements).
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from core.models import (
    FileRecord,
    LogicalFileGroup,
    MatrixRowView,
    MatrixView,
    ScanResult,
    Source,
    SourceCellInfo,
)
from core.storage import load_json, save_json

SCAN_SNAPSHOT_VERSION = 1
SCAN_DIR = os.path.join("data", "scans")

# scan_id comes from uuid.uuid4() in scanner
_SCAN_ID_RE = re.compile(
    r"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
)


def is_valid_scan_id(scan_id: str) -> bool:
    if not scan_id or ".." in scan_id or "/" in scan_id or "\\" in scan_id:
        return False
    return bool(_SCAN_ID_RE.match(scan_id.strip()))


def snapshot_file_path(scan_id: str) -> str:
    if not is_valid_scan_id(scan_id):
        raise ValueError("invalid scan_id for snapshot path")
    return os.path.join(SCAN_DIR, f"{scan_id.strip()}.json")


def _source_by_id(catalog: List[Source], sid: str) -> Optional[Source]:
    return next((s for s in catalog if s.id == sid), None)


def _file_record_to_dict(r: FileRecord) -> Dict[str, Any]:
    return {
        "source_id": r.source_id,
        "domain_id": r.domain_id,
        "absolute_path": r.absolute_path,
        "relative_path": r.relative_path,
        "exists": r.exists,
        "is_file": r.is_file,
        "checksum": r.checksum,
        "size": r.size,
        "mtime": r.mtime,
        "warning": r.warning,
    }


def _file_record_from_dict(d: Dict[str, Any]) -> FileRecord:
    return FileRecord(
        source_id=str(d["source_id"]),
        domain_id=str(d["domain_id"]),
        absolute_path=str(d["absolute_path"]),
        relative_path=str(d["relative_path"]),
        exists=bool(d["exists"]),
        is_file=bool(d["is_file"]),
        checksum=d.get("checksum"),
        size=d.get("size"),
        mtime=d.get("mtime"),
        warning=d.get("warning"),
    )


def _group_to_dict(g: LogicalFileGroup) -> Dict[str, Any]:
    return {
        "domain_id": g.domain_id,
        "relative_path": g.relative_path,
        "records": [_file_record_to_dict(r) for r in g.records],
        "status": g.status,
        "candidate_source_ids": list(g.candidate_source_ids),
        "notes": list(g.notes),
    }


def _group_from_dict(d: Dict[str, Any]) -> LogicalFileGroup:
    return LogicalFileGroup(
        domain_id=str(d["domain_id"]),
        relative_path=str(d["relative_path"]),
        records=[_file_record_from_dict(x) for x in d.get("records", [])],
        status=str(d.get("status", "UNCERTAIN")),
        candidate_source_ids=[str(x) for x in d.get("candidate_source_ids", [])],
        notes=[str(x) for x in d.get("notes", [])],
    )


def _cell_to_dict(c: SourceCellInfo) -> Dict[str, Any]:
    return {
        "variant_label": c.variant_label,
        "score": c.score,
        "score_display": c.score_display,
        "bucket": c.bucket,
        "missing": c.missing,
    }


def _cell_from_dict(d: Dict[str, Any]) -> SourceCellInfo:
    return SourceCellInfo(
        variant_label=str(d.get("variant_label", "")),
        score=d.get("score"),
        score_display=str(d.get("score_display", "-")),
        bucket=str(d.get("bucket", "missing")),
        missing=bool(d.get("missing", False)),
    )


def _matrix_row_to_dict(row: MatrixRowView) -> Dict[str, Any]:
    return {
        "relative_path": row.relative_path,
        "cells": {sid: _cell_to_dict(c) for sid, c in row.cells.items()},
        "gold_display": row.gold_display,
        "gold_source_id": row.gold_source_id,
        "available_source_ids": list(row.available_source_ids),
        "baseline_display": row.baseline_display,
        "baseline_kind": row.baseline_kind,
        "status": row.status,
    }


def _matrix_row_from_dict(d: Dict[str, Any]) -> MatrixRowView:
    cells_raw = d.get("cells") or {}
    cells = {str(sid): _cell_from_dict(c) for sid, c in cells_raw.items()}
    return MatrixRowView(
        relative_path=str(d["relative_path"]),
        cells=cells,
        gold_display=str(d.get("gold_display", "-")),
        gold_source_id=d.get("gold_source_id"),
        available_source_ids=[str(x) for x in d.get("available_source_ids", [])],
        baseline_display=str(d.get("baseline_display", "-")),
        baseline_kind=str(d.get("baseline_kind", "SUGGESTED")),
        status=str(d.get("status", "UNCERTAIN")),
    )


def save_scan_snapshot(
    result: ScanResult,
    matrix: MatrixView,
    catalog_sources: List[Source],
) -> str:
    """
    Write data/scans/<scan_id>.json. Returns scan_id.

    Stores ScanResult.groups and full matrix rows so UI can reload without re-scanning.
    """
    scan_id = result.scan_id
    if not is_valid_scan_id(scan_id):
        # scanner always uses uuid4; normalize if ever needed
        raise ValueError(f"refusing to save snapshot for invalid scan_id: {scan_id!r}")

    path = snapshot_file_path(scan_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    column_ids = [s.id for s in matrix.sources]
    payload: Dict[str, Any] = {
        "version": SCAN_SNAPSHOT_VERSION,
        "scan_id": result.scan_id,
        "domain_id": result.domain_id,
        "path_input": result.path_input,
        "recursive": result.recursive,
        "source_ids": list(result.source_ids),
        "warnings": list(result.warnings),
        "groups": [_group_to_dict(g) for g in result.groups],
        "matrix_rows": [_matrix_row_to_dict(r) for r in matrix.rows],
        "column_source_ids": column_ids,
    }
    save_json(path, payload)
    return scan_id


def load_scan_snapshot(
    scan_id: str,
    catalog_sources: List[Source],
) -> Optional[Tuple[ScanResult, MatrixView]]:
    """
    Load snapshot from disk. Reattaches Source objects for matrix columns from catalog.

    Returns None if file missing, invalid id, or version mismatch.
    """
    if not is_valid_scan_id(scan_id):
        return None
    path = snapshot_file_path(scan_id)
    if not os.path.isfile(path):
        return None
    try:
        data = load_json(path)
    except (OSError, ValueError):
        return None

    if int(data.get("version", 0)) != SCAN_SNAPSHOT_VERSION:
        return None

    groups = [_group_from_dict(g) for g in data.get("groups", [])]
    result = ScanResult(
        scan_id=str(data["scan_id"]),
        domain_id=str(data["domain_id"]),
        recursive=bool(data.get("recursive", False)),
        source_ids=[str(x) for x in data.get("source_ids", [])],
        path_input=str(data.get("path_input", "")),
        warnings=[str(x) for x in data.get("warnings", [])],
        groups=groups,
    )

    rows = [_matrix_row_from_dict(r) for r in data.get("matrix_rows", [])]
    col_ids = [str(x) for x in data.get("column_source_ids", result.source_ids)]
    column_sources: List[Source] = []
    for sid in col_ids:
        src = _source_by_id(catalog_sources, sid)
        if src:
            column_sources.append(src)
    if not column_sources:
        column_sources = [s for s in catalog_sources if s.id in set(result.source_ids)]

    matrix = MatrixView(rows=rows, sources=column_sources)
    return result, matrix
