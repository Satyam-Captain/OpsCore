"""Pluggable backends for DB, request dirs, and scripts (mock vs office)."""


import os
from typing import Any, Dict, Tuple

from services.adapters.db_base import DbAdapterBase
from services.adapters.db_json import JsonFileDbAdapter
from services.adapters.db_mariadb import MariaDbAdapter
from services.adapters.request_base import RequestDirAdapterBase
from services.adapters.request_gmc import GmcRequestDirAdapter
from services.adapters.request_local import LocalLsfRequestDirAdapter
from services.adapters.scripts_base import ScriptsAdapterBase
from services.adapters.scripts_gmc import GmcScriptsAdapter
from services.adapters.scripts_mock import MockScriptsAdapter

# Office default: parent of REQ-<n> (override with service_gmc_requests_root).
_DEFAULT_GMC_REQUESTS_ROOT = "/opt/platform/gmc/modCFG/work/.requests/LSF10CFG"


def build_adapters(settings: Dict[str, Any]) -> Tuple[DbAdapterBase, RequestDirAdapterBase, ScriptsAdapterBase]:
    """
    Construct adapter triple (db, request, scripts) from ``settings``.

    Office swap: change ``service_*_backend`` keys and extend this factory.
    """
    from services.paths import project_root

    root = project_root()
    sandbox = settings.get("service_sandbox_root", "data/sandbox")
    sandbox_abs = os.path.join(root, sandbox)

    db_backend = settings.get("service_db_backend", "json")
    if db_backend == "json":
        db = JsonFileDbAdapter(os.path.join(sandbox_abs, "db"))
    elif db_backend == "mariadb":
        db = MariaDbAdapter.from_settings(settings)
    else:
        raise NotImplementedError(
            f"service_db_backend={db_backend!r} not implemented"
        )

    req_backend = settings.get("service_request_backend", "local")
    if req_backend == "local":
        request_adapter = LocalLsfRequestDirAdapter(sandbox_abs)
    elif req_backend == "gmc":
        raw_root = settings.get("service_gmc_requests_root")
        if isinstance(raw_root, str) and raw_root.strip():
            gmc_root = raw_root.strip()
        else:
            gmc_root = _DEFAULT_GMC_REQUESTS_ROOT
        request_adapter = GmcRequestDirAdapter(gmc_root)
    else:
        raise NotImplementedError(
            f"service_request_backend={req_backend!r} not implemented"
        )

    script_backend = str(settings.get("service_script_backend", "mock") or "mock").strip().lower()
    if script_backend == "mock":
        scripts = MockScriptsAdapter(sandbox_abs)
    elif script_backend == "gmc":
        scripts = GmcScriptsAdapter.from_settings(settings)
    else:
        raise NotImplementedError(
            "service_script_backend=%r not implemented" % (settings.get("service_script_backend"),)
        )

    return db, request_adapter, scripts
