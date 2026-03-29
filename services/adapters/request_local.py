"""
Sandbox request dirs: <sandbox>/requests/LSF10CFG/REQ-<request_number>.
"""

from __future__ import annotations

import os

from services.adapters.request_base import RequestDirAdapterBase


class LocalLsfRequestDirAdapter(RequestDirAdapterBase):
    """Local filesystem layout used for laptop mock and office parity."""

    def __init__(self, sandbox_root: str, cfg_subdir: str = "LSF10CFG") -> None:
        self._sandbox_root = sandbox_root
        self._cfg_subdir = cfg_subdir

    def get_request_path(self, request_number: str) -> str:
        req = str(request_number).strip()
        if not req or any(c in req for c in ("/", "\\", "..")):
            raise ValueError("invalid request_number")
        return os.path.join(
            self._sandbox_root, "requests", self._cfg_subdir, f"REQ-{req}"
        )

    def request_exists(self, request_number: str) -> bool:
        path = self.get_request_path(request_number)
        return os.path.isdir(path)
