"""
Workspace activity for global locking.

* If ``lifecycle.status`` is present on the operation, it is the source of truth.
* Legacy operations without ``lifecycle`` use older terminal heuristics so finalized /
  rolled-back jobs still release the lock.
"""


from typing import Any, Dict


def is_workspace_terminal(doc: Dict[str, Any]) -> bool:
    """
    Terminal workspaces are not "active" for lock purposes.

    Prototype rules:
    * ``rollback_executed`` → terminal (request treated as closed for locking).
    * ``step_results.finalization.status`` in ``done`` | ``already_present`` → terminal.
    """
    if bool(doc.get("rollback_executed")):
        return True
    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    fin = sr.get("finalization")
    if isinstance(fin, dict):
        st = str(fin.get("status") or "").strip()
        if st in ("done", "already_present"):
            return True
    return False


def effective_lifecycle_status(doc: Dict[str, Any]) -> str:
    """
    Effective lifecycle status for UI and locking.

    * Explicit ``lifecycle.status`` in ``active`` | ``completed`` | ``rolled_back`` wins.
    * Otherwise legacy docs: terminal heuristics map to ``completed``, else ``active``.
    """
    lc = doc.get("lifecycle")
    if isinstance(lc, dict):
        st = str(lc.get("status") or "").strip()
        if st in ("active", "completed", "rolled_back"):
            return st
    if is_workspace_terminal(doc):
        return "completed"
    return "active"


def lifecycle_closed_at(doc: Dict[str, Any]) -> str:
    lc = doc.get("lifecycle")
    if isinstance(lc, dict) and lc.get("closed_at"):
        return str(lc["closed_at"])
    return ""


def is_workspace_active(doc: Dict[str, Any]) -> bool:
    """True if this operation should participate in the global active-workspace lock."""
    return effective_lifecycle_status(doc) == "active"
