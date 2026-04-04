"""
ELIM mock config helpers: paths, state merge, and UI row model.

Reference cluster is ``generation_context_cluster`` on the operation (not deployment).
"""


import os
from typing import Any, Dict, List, Optional, Tuple

from services.paths import project_root

CONFIG_LSB_RESOURCES = "lsb.resources"
CONFIG_LSF_SHARED = "lsf.shared"
CONFIG_LSF_CLUSTER = "lsf.cluster"
CONFIG_LSF_LICENSESCHEDULER = "lsf.licensescheduler"

ELIM_CONFIG_TYPES: Tuple[str, ...] = (
    CONFIG_LSB_RESOURCES,
    CONFIG_LSF_SHARED,
    CONFIG_LSF_CLUSTER,
)

LS_CONFIG_TYPES: Tuple[str, ...] = (
    CONFIG_LSB_RESOURCES,
    CONFIG_LSF_LICENSESCHEDULER,
)

# Union of all mock generation/publish config filenames (ELIM + LS).
ALL_MOCK_CONFIG_TYPES: Tuple[str, ...] = tuple(
    dict.fromkeys(ELIM_CONFIG_TYPES + (CONFIG_LSF_LICENSESCHEDULER,))
)


def is_allowed_elim_config_type(config_type: str) -> bool:
    """True if ``config_type`` is a known mock config id (ELIM or LS)."""
    return config_type in ALL_MOCK_CONFIG_TYPES


def sandbox_abs(settings: Dict[str, Any]) -> str:
    rel = settings.get("service_sandbox_root", "data/sandbox")
    return os.path.join(project_root(), rel)


def normalize_generation_cluster(raw: Any) -> Optional[str]:
    """Return safe cluster string or None if unset/invalid."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or any(c in s for c in ("/", "\\", "..")):
        return None
    return s


def _script_backend_gmc(settings: Dict[str, Any]) -> bool:
    return str(settings.get("service_script_backend", "mock") or "").strip().lower() == "gmc"


def current_config_path(
    settings: Dict[str, Any], cluster: str, config_type: str
) -> str:
    """Sandbox current file, or GMC OUTPUT baseline when ``service_script_backend`` is ``gmc``."""
    c = normalize_generation_cluster(cluster)
    if not c or config_type not in ALL_MOCK_CONFIG_TYPES:
        raise ValueError("invalid cluster or config type")
    if _script_backend_gmc(settings):
        from services.adapters.scripts_gmc import GmcScriptsAdapter

        return GmcScriptsAdapter.from_settings(settings).current_config_path(config_type, c)
    return os.path.join(sandbox_abs(settings), "current", c, config_type)


def generated_config_path(
    settings: Dict[str, Any], cluster: str, config_type: str
) -> str:
    """Sandbox ``*.new`` path; for ``gmc``, OUTPUT dir path preferring ``*.new`` then ``*.test`` if present."""
    c = normalize_generation_cluster(cluster)
    if not c or config_type not in ALL_MOCK_CONFIG_TYPES:
        raise ValueError("invalid cluster or config type")
    if _script_backend_gmc(settings):
        from services.adapters.scripts_gmc import GmcScriptsAdapter

        return GmcScriptsAdapter.from_settings(settings).generated_config_path(config_type, c)
    return os.path.join(sandbox_abs(settings), "generated", c, f"{config_type}.new")


def current_lsb_resources_path(settings: Dict[str, Any], cluster: str) -> str:
    """Backward-compatible alias."""
    return current_config_path(settings, cluster, CONFIG_LSB_RESOURCES)


def generated_lsb_resources_path(settings: Dict[str, Any], cluster: str) -> str:
    """Backward-compatible alias."""
    return generated_config_path(settings, cluster, CONFIG_LSB_RESOURCES)


def read_text_file(path: str) -> Tuple[str, bool]:
    """Return (content, True) if file exists, else ('', False)."""
    if not path or not os.path.isfile(path):
        return "", False
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read(), True


def normalize_generation_state(raw: Any) -> Dict[str, Dict[str, Any]]:
    """
    Map config_type -> generation payload.

    Supports legacy single dict: ``{"config_type": "lsb.resources", "generated_path": ...}``.
    """
    if not isinstance(raw, dict):
        return {}
    nested: Dict[str, Dict[str, Any]] = {}
    for ct in ALL_MOCK_CONFIG_TYPES:
        v = raw.get(ct)
        if isinstance(v, dict):
            nested[ct] = dict(v)
    if nested:
        return nested
    ct = raw.get("config_type")
    if isinstance(ct, str) and ct in ALL_MOCK_CONFIG_TYPES:
        return {ct: dict(raw)}
    return {}


def normalize_publish_state(raw: Any) -> Dict[str, Dict[str, Any]]:
    """Map config_type -> publish payload; migrates legacy flat shape."""
    if not isinstance(raw, dict):
        return {}
    nested: Dict[str, Dict[str, Any]] = {}
    for ct in ALL_MOCK_CONFIG_TYPES:
        v = raw.get(ct)
        if isinstance(v, dict):
            nested[ct] = dict(v)
    if nested:
        return nested
    ct = raw.get("config_type")
    if isinstance(ct, str) and ct in ALL_MOCK_CONFIG_TYPES:
        return {ct: dict(raw)}
    return {}


def merge_generation_step_result(
    step_results: Dict[str, Any],
    *,
    config_type: str,
    cluster: str,
    generated_path: str,
    status: str,
) -> Dict[str, Any]:
    out = dict(step_results)
    gen = normalize_generation_state(out.get("generation"))
    gen[config_type] = {
        "config_type": config_type,
        "cluster": cluster,
        "generated_path": generated_path,
        "status": status,
    }
    out["generation"] = gen
    return out


def merge_publish_step_result(
    step_results: Dict[str, Any],
    *,
    config_type: str,
    destination_path: str,
    status: str,
) -> Dict[str, Any]:
    out = dict(step_results)
    pub = normalize_publish_state(out.get("publish"))
    pub[config_type] = {
        "config_type": config_type,
        "destination_path": destination_path,
        "status": status,
    }
    out["publish"] = pub
    return out


def elim_config_ui_rows(
    settings: Dict[str, Any],
    sr: Dict[str, Any],
    elim_config_enabled: bool,
    gen_cluster: Optional[str],
    config_types: Optional[Tuple[str, ...]] = None,
) -> List[Dict[str, Any]]:
    """
    Per-config-type rows for the operation view (paths, flags, stored step metadata).

    ``config_types`` defaults to ELIM triple; pass blueprint generation ids for LS/other defs.
    """
    gen_map = normalize_generation_state(sr.get("generation"))
    pub_map = normalize_publish_state(sr.get("publish"))
    rows: List[Dict[str, Any]] = []
    types = config_types if config_types is not None else ELIM_CONFIG_TYPES
    for ct in types:
        cur_p, gen_p = "", ""
        if gen_cluster:
            try:
                cur_p = current_config_path(settings, gen_cluster, ct)
                gen_p = generated_config_path(settings, gen_cluster, ct)
            except ValueError:
                pass
        gen_entry = gen_map.get(ct) if isinstance(gen_map.get(ct), dict) else {}
        pub_entry = pub_map.get(ct) if isinstance(pub_map.get(ct), dict) else {}
        gen_exists = bool(gen_p and os.path.isfile(gen_p))
        _, cur_exists = read_text_file(cur_p) if cur_p else ("", False)
        rows.append(
            {
                "config_type": ct,
                "current_path": cur_p,
                "generated_path": gen_p,
                "generation": gen_entry,
                "publish": pub_entry,
                "cur_exists": cur_exists,
                "gen_exists": gen_exists,
                "can_generate": elim_config_enabled and bool(gen_cluster),
                "can_diff": bool(gen_cluster) and gen_exists,
                "can_publish": bool(gen_cluster) and gen_exists,
            }
        )
    return rows
