"""
Resolve and validate paths under a configured domain root (POSIX, for remote SSH targets).

All domain roots and file paths are treated as POSIX paths. This avoids Windows path
semantics leaking into remote commands.
"""

from __future__ import annotations

from pathlib import PurePosixPath


def _lexical_posix_norm(path: PurePosixPath) -> PurePosixPath:
    """Collapse ``.`` / ``..`` on a POSIX path without filesystem access."""
    stack: list[str] = []
    for part in path.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if stack:
                stack.pop()
            elif not path.is_absolute():
                stack.append("..")
        else:
            stack.append(part)
    if path.is_absolute():
        return PurePosixPath("/").joinpath(*stack) if stack else PurePosixPath("/")
    return PurePosixPath(*stack) if stack else PurePosixPath(".")


def normalize_user_relative_path(path_input: str) -> str:
    """
    Normalize a user-supplied path fragment (filter or relative path under the domain root).

    Empty / whitespace-only input returns "" (meaning "no extra fragment — use domain root only").

    Rejects absolute paths and any ".." segment to block traversal.
    """
    if path_input is None:
        return ""
    s = str(path_input).strip()
    if not s:
        return ""

    p = PurePosixPath(s)
    if p.is_absolute():
        raise ValueError("path must be relative (no leading /)")

    for part in p.parts:
        if part == "..":
            raise ValueError('path must not contain ".."')
        if part == "~":
            raise ValueError("tilde in path is not allowed")

    # Normalized string relative to logical domain (no leading ./ unless needed)
    out = str(p)
    if out.startswith("/"):
        raise ValueError("invalid path")
    return out


def posix_absolute_under_root(domain_root: str, relative_fragment: str) -> str:
    """
    Join an absolute domain root (from domains.json) with a normalized relative fragment.

    The result is the absolute POSIX path on the remote host. Raises if the combined
    path does not lie under domain_root (after lexical normalization).
    """
    root = PurePosixPath(domain_root.strip())
    if not root.is_absolute():
        raise ValueError("domain root must be absolute")

    rel_norm = normalize_user_relative_path(relative_fragment) if relative_fragment else ""
    rel_path = PurePosixPath(rel_norm) if rel_norm else PurePosixPath(".")
    root_n = _lexical_posix_norm(root)
    combined = _lexical_posix_norm(root_n / rel_path)

    rp = root_n.parts
    cp = combined.parts
    if len(cp) < len(rp) or cp[: len(rp)] != rp:
        raise ValueError("resolved path escapes domain root")

    return str(combined)


def domain_root_for_source(domain_roots: dict, source_type: str) -> str:
    """Pick the configured root key: GMC sources use roots['gmc'], else roots['cluster']."""
    key = "gmc" if source_type == "gmc" else "cluster"
    if key not in domain_roots:
        raise KeyError(f"domain.roots missing key {key!r}")
    root = domain_roots[key]
    if not root or not str(root).strip():
        raise ValueError("empty domain root in config")
    return str(root).strip()
