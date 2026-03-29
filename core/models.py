
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class SourceCellInfo:
    """One matrix cell: variant label, similarity to baseline, and bucket for styling."""

    variant_label: str
    score: Optional[float]
    score_display: str
    bucket: str
    missing: bool


@dataclass
class MatrixRowView:
    """One row in the comparison matrix (one logical file)."""

    relative_path: str
    cells: Dict[str, SourceCellInfo]
    gold_display: str
    gold_source_id: Optional[str]
    available_source_ids: List[str]
    baseline_display: str
    baseline_kind: str
    status: str


@dataclass
class FileSourceDetailRow:
    """Per-source row on the file detail page."""

    source_id: str
    source_label: str
    absolute_path: str
    checksum: str
    mtime: str
    size_display: str
    variant_label: str
    score_display: str
    bucket: str
    exists: bool
    missing: bool


@dataclass
class MatrixView:
    """Full matrix for a scan: columns follow selected source order."""

    rows: List[MatrixRowView]
    sources: List['Source']


@dataclass
class Source:
    id: str
    name: str
    type: str
    host: str
    ssh_user: str = ""
    enabled: bool = True
    trusted_reference: bool = False


@dataclass
class Domain:
    id: str
    label: str
    recursive_allowed: bool
    roots: Dict[str, str]


@dataclass
class FileRecord:
    source_id: str
    domain_id: str
    absolute_path: str
    relative_path: str
    exists: bool
    is_file: bool
    checksum: Optional[str] = None
    size: Optional[int] = None
    mtime: Optional[str] = None
    warning: Optional[str] = None


@dataclass
class LogicalFileGroup:
    domain_id: str
    relative_path: str
    records: List['FileRecord'] = field(default_factory=list)
    status: str = "UNCERTAIN"
    candidate_source_ids: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class ScanResult:
    scan_id: str
    domain_id: str
    recursive: bool
    source_ids: List[str]
    path_input: str
    warnings: List[str] = field(default_factory=list)
    groups: List['LogicalFileGroup'] = field(default_factory=list)
