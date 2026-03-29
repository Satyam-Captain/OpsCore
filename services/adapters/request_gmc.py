"""
GMC filesystem layout: <requests_root>/REQ-<request_number>.

``requests_root`` is typically the LSF10CFG segment, e.g.
``/opt/platform/gmc/modCFG/work/.requests/LSF10CFG`` — configurable via settings.
"""

from __future__ import annotations

import os

from services.adapters.request_base import RequestDirAdapterBase


class GmcRequestDirAdapter(RequestDirAdapterBase):
    """Resolve request dirs on the GMC host under the configured LSF10CFG root."""

    def __init__(self, requests_root: str) -> None:
        root = str(requests_root or "").strip()
        if not root:
            raise ValueError("GMC requests root is empty")
        self._requests_root = os.path.abspath(os.path.expanduser(root))

    def get_request_path(self, request_number: str) -> str:
        req = str(request_number).strip()
        if not req or any(c in req for c in ("/", "\\", "..")):
            raise ValueError("invalid request_number")
        return os.path.join(self._requests_root, f"REQ-{req}")

    def request_exists(self, request_number: str) -> bool:
        path = self.get_request_path(request_number)
        return os.path.isdir(path)
