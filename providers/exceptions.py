"""Inventory provider errors (SSH and future backends)."""


from typing import Optional


class SshReadError(RuntimeError):
    """
    Remote file read over SSH failed (non-zero exit, timeout, etc.).

    Not used by mock mode. Callers may catch and degrade gracefully (matrix scoring)
    or surface the message (diff page).
    """

    def __init__(
        self,
        message: str,
        *,
        source_id: str,
        path: str,
        returncode: Optional[int] = None,
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.source_id = source_id
        self.path = path
        self.returncode = returncode
        self.stderr = stderr
