"""
Request lifecycle fields on operation JSON (explicit close via finalize / rollback-close).

Replace session-style checks with real IAM later; structure is intentionally small.
"""


from datetime import datetime, timezone
from typing import Any, Dict


def lifecycle_timestamp_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_lifecycle_dict() -> Dict[str, Any]:
    return {"status": "active", "closed_at": None}


def merge_lifecycle_status(doc: Dict[str, Any], status: str) -> Dict[str, Any]:
    """Return full ``lifecycle`` object to persist (preserves unknown keys)."""
    if status not in ("completed", "rolled_back"):
        raise ValueError("invalid lifecycle close status")
    lc = dict(doc.get("lifecycle") or {}) if isinstance(doc.get("lifecycle"), dict) else {}
    lc["status"] = status
    lc["closed_at"] = lifecycle_timestamp_iso()
    return lc
