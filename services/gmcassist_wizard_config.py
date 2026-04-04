"""
GMCAssist: metadata-driven allowlist for mock config types (generate / diff / publish).

Wizard JSON may set ``allowed_mock_config_types`` to a list of config id strings.
If absent, all types accepted by ``elim_lsb.ALL_MOCK_CONFIG_TYPES`` are allowed (legacy).
"""

from typing import Any, Dict, Optional, Tuple

from services.elim_lsb import ALL_MOCK_CONFIG_TYPES


def allowed_mock_config_types(wizard_def: Dict[str, Any]):
    """
    Return frozenset of allowed config_type strings, or None if the wizard omits the list
    (permissive: any known mock type).
    """
    raw = wizard_def.get("allowed_mock_config_types")
    if not isinstance(raw, list) or len(raw) == 0:
        return None
    out = frozenset(str(x).strip() for x in raw if str(x).strip())
    return out if out else None


def config_type_allowed_for_wizard(
    wizard_def: Dict[str, Any], config_type: str
) -> Tuple[bool, str]:
    """
    Return (ok, error_message). Empty error when ok.

    Unknown config types are rejected when an allowlist is present.
    When no allowlist, only types in ALL_MOCK_CONFIG_TYPES are accepted.
    """
    ct = str(config_type or "").strip()
    if not ct:
        return False, "Config type is missing for this step."
    allowed = allowed_mock_config_types(wizard_def)
    if allowed is not None:
        if ct not in allowed:
            return (
                False,
                "Config type %r is not allowed for this wizard (see allowed_mock_config_types)."
                % ct,
            )
        return True, ""
    if ct not in ALL_MOCK_CONFIG_TYPES:
        return False, "Unsupported config type %r for mock generation." % ct
    return True, ""
