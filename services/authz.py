"""
Prototype session role for service workspace control.

Replace this module with real auth/IAM later; keep call sites using these functions only.

SECURITY: The superadmin password below is for local prototype use only — not for production.
"""


from typing import Any, Optional

from flask import has_request_context, session

SESSION_ROLE_KEY = "opscore_service_role"
# Stored in Flask ``app.config`` — new value each time the server process starts.
SERVICE_ROLE_NONCE_CONFIG_KEY = "SERVICE_ROLE_NONCE"
# Client cookie must carry this nonce; if it mismatches (e.g. after server restart), role resets to admin.
SESSION_NONCE_KEY = "opscore_service_role_nonce"

# Prototype only — do not reuse in production; replace with IAM / password hashing / SSO.
PROTOTYPE_SUPERADMIN_PASSWORD = "satyam"

ROLE_ADMIN = "admin"
ROLE_SUPERADMIN = "superadmin"


def _current_server_nonce() -> Optional[str]:
    try:
        from flask import current_app

        raw = current_app.config.get(SERVICE_ROLE_NONCE_CONFIG_KEY)
        return str(raw) if raw else None
    except RuntimeError:
        return None


def _sync_role_session_to_server_process() -> None:
    """
    Drop superadmin if this session was created under a previous server process.

    Browser cookies survive Flask restarts; without this, ``opscore_service_role`` would
    still read as superadmin until the cookie expires.
    """
    if not has_request_context():
        return
    expected = _current_server_nonce()
    if not expected:
        return
    if session.get(SESSION_NONCE_KEY) == expected:
        return
    session.pop(SESSION_ROLE_KEY, None)
    session[SESSION_NONCE_KEY] = expected
    session.permanent = False
    session.modified = True


def _stamp_role_session_nonce() -> None:
    """Bind session to the current server process (call after successful superadmin login)."""
    if not has_request_context():
        return
    expected = _current_server_nonce()
    if not expected:
        return
    session[SESSION_NONCE_KEY] = expected
    session.modified = True


def get_current_role() -> str:
    """
    Default is **admin** until the user submits the superadmin password on Service access.

    Only the exact stored string ``superadmin`` counts; anything else is treated as admin
    (stale or tampered session values do not elevate).
    """
    _sync_role_session_to_server_process()
    raw = session.get(SESSION_ROLE_KEY)
    if raw is None:
        return ROLE_ADMIN
    if isinstance(raw, str) and raw.strip() == ROLE_SUPERADMIN:
        return ROLE_SUPERADMIN
    return ROLE_ADMIN


def is_superadmin() -> bool:
    return get_current_role() == ROLE_SUPERADMIN


def try_superadmin_login(password: str) -> bool:
    """
    Return True if password accepted and session upgraded to superadmin.

    Does **not** set ``session.permanent`` — elevation lasts for this browser session only
    (cookie expires when the browser session ends), not for weeks via a persistent cookie.
    """
    if (password or "").strip() == PROTOTYPE_SUPERADMIN_PASSWORD:
        session[SESSION_ROLE_KEY] = ROLE_SUPERADMIN
        _stamp_role_session_nonce()
        session.modified = True
        return True
    return False


def clear_role() -> None:
    """Drop back to normal admin (remove elevated session role)."""
    session.pop(SESSION_ROLE_KEY, None)
    session.permanent = False
    _stamp_role_session_nonce()
    session.modified = True
