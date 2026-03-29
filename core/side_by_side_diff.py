"""Line-aligned side-by-side rows for diff visualization (no unified diff)."""


from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import List


@dataclass(frozen=True)
class SideBySideRow:
    """One aligned row: left line, right line, and row CSS kind."""

    left: str
    right: str
    kind: str  # same | added | removed | modified


def build_side_by_side_rows(left: str, right: str) -> List[SideBySideRow]:
    """Split into lines and align with SequenceMatcher opcodes."""
    a = left.splitlines()
    b = right.splitlines()
    matcher = SequenceMatcher(a=a, b=b, autojunk=False)
    rows: List[SideBySideRow] = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                rows.append(SideBySideRow(a[i1 + k], b[j1 + k], "same"))
        elif tag == "delete":
            for k in range(i2 - i1):
                rows.append(SideBySideRow(a[i1 + k], "", "removed"))
        elif tag == "insert":
            for k in range(j2 - j1):
                rows.append(SideBySideRow("", b[j1 + k], "added"))
        elif tag == "replace":
            la, rb = i2 - i1, j2 - j1
            n = max(la, rb)
            for k in range(n):
                lv = a[i1 + k] if k < la else ""
                rv = b[j1 + k] if k < rb else ""
                if lv and rv:
                    kind = "modified"
                elif lv:
                    kind = "removed"
                else:
                    kind = "added"
                rows.append(SideBySideRow(lv, rv, kind))

    return rows
