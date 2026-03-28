"""Unified text diff for two file contents."""

from __future__ import annotations

from difflib import unified_diff


def unified_diff_text(
    left: str,
    right: str,
    from_name: str,
    to_name: str,
    context_lines: int = 3,
) -> str:
    left_lines = left.splitlines(keepends=True)
    right_lines = right.splitlines(keepends=True)
    lines = list(
        unified_diff(
            left_lines,
            right_lines,
            fromfile=from_name,
            tofile=to_name,
            n=context_lines,
        )
    )
    if not lines:
        return "No line differences (files match when compared line-by-line).\n"
    return "".join(lines)
