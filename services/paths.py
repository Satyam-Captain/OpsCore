"""Filesystem paths for the service layer (project root = OpsCore app directory)."""

from __future__ import annotations

import os


def project_root() -> str:
    """Directory containing app.py (parent of the ``services`` package)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
