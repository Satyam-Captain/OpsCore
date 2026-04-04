"""
GMC script backend: real ``lsf-*.pl`` generation/publish and OUTPUT-* directory layout.

Paths and commands follow site GMC layout; override via ``settings.json`` when needed.
"""

import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from services.adapters.scripts_base import ScriptsAdapterBase
from services.elim_lsb import normalize_generation_cluster

# config_type -> perl script basename, OUTPUT directory name under work root, file name prefix
_GMC_CONFIG_META: Dict[str, Dict[str, str]] = {
    "lsb.resources": {
        "script": "lsf-lsb.resources.pl",
        "output_dir": "lsf-lsb.resources-OUTPUT-10",
        "file_prefix": "lsb.resources",
    },
    "lsf.shared": {
        "script": "lsf-lsf.shared.pl",
        "output_dir": "lsf-lsf.shared-OUTPUT-10",
        "file_prefix": "lsf.shared",
    },
    "lsf.cluster": {
        "script": "lsf-lsf.cluster.pl",
        "output_dir": "lsf-lsf.cluster-OUTPUT-10",
        "file_prefix": "lsf.cluster",
    },
    "lsf.licensescheduler": {
        "script": "lsf-lsf.licensescheduler.pl",
        "output_dir": "lsf-lsf.licensescheduler-OUTPUT-10",
        "file_prefix": "lsf.licensescheduler",
    },
}

_DEFAULT_WORK_ROOT = "/opt/platform/gmc/modCFG/work"


def _safe_reqno(request_number: str) -> str:
    raw = str(request_number).strip()
    if raw.upper().startswith("REQ-"):
        raw = raw[4:].strip()
    if not raw or any(c in raw for c in ("/", "\\", "..", " ")):
        raise ValueError("invalid request_number for GMC publish")
    return raw


def _run_pl(
    argv: List[str],
    cwd: str,
    timeout: int,
) -> Tuple[int, str, str]:
    """Run command with no shell; return (exit_code, stdout, stderr)."""
    proc = subprocess.run(
        argv,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        timeout=timeout,
    )
    out = proc.stdout or ""
    err = proc.stderr or ""
    return int(proc.returncode), out, err


def _fmt_cmd_error(cmd: List[str], code: int, out: str, err: str) -> str:
    tail = (err or out or "").strip()
    if len(tail) > 800:
        tail = tail[:800] + "..."
    return "GMC command failed (exit %s): %s | %s" % (code, " ".join(cmd), tail)


class GmcScriptsAdapter(ScriptsAdapterBase):
    """
    Execute real GMC perl tools under ``service_gmc_work_root`` / ``service_gmc_scripts_dir``.

    Baseline (current) file: ``<OUTPUT-dir>/<prefix>.<cluster>`` (no .new/.test).
    Generated path for diff: existing ``.new`` then ``.test`` under OUTPUT; if neither exists yet,
    expected ``.new`` path (same rule as ``generate_config`` return value).
    """

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "GmcScriptsAdapter":
        work = str(settings.get("service_gmc_work_root") or _DEFAULT_WORK_ROOT).strip()
        if not work:
            raise ValueError("service_gmc_work_root is empty")
        work_abs = os.path.abspath(os.path.expanduser(work))
        scripts_raw = settings.get("service_gmc_scripts_dir")
        if isinstance(scripts_raw, str) and scripts_raw.strip():
            scripts_abs = os.path.abspath(os.path.expanduser(scripts_raw.strip()))
        else:
            scripts_abs = os.path.join(work_abs, "scripts")
        timeout = int(settings.get("service_gmc_script_timeout_seconds") or 600)
        if timeout < 10:
            timeout = 10
        requests_root = settings.get("service_gmc_requests_root")
        if isinstance(requests_root, str) and requests_root.strip():
            req_root_abs = os.path.abspath(os.path.expanduser(requests_root.strip()))
        else:
            req_root_abs = os.path.join(
                work_abs, ".requests", "LSF10CFG"
            )
        return cls(
            work_root=work_abs,
            scripts_dir=scripts_abs,
            requests_root=req_root_abs,
            timeout=timeout,
        )

    def __init__(
        self,
        work_root: str,
        scripts_dir: str,
        requests_root: str,
        timeout: int,
    ) -> None:
        self._work_root = work_root
        self._scripts_dir = scripts_dir
        self._requests_root = requests_root
        self._timeout = timeout

    def _meta(self, config_type: str) -> Dict[str, str]:
        ct = str(config_type or "").strip()
        if ct not in _GMC_CONFIG_META:
            raise ValueError("unsupported config_type for GMC scripts: %r" % (ct,))
        return _GMC_CONFIG_META[ct]

    def _script_path(self, config_type: str) -> str:
        meta = self._meta(config_type)
        name = meta["script"]
        path = os.path.join(self._scripts_dir, name)
        return path

    def _output_dir(self, config_type: str) -> str:
        meta = self._meta(config_type)
        return os.path.join(self._work_root, meta["output_dir"])

    @staticmethod
    def _safe_cluster(cluster: str) -> str:
        c = normalize_generation_cluster(cluster)
        if not c:
            raise ValueError("invalid cluster")
        return c

    def current_config_path(self, config_type: str, cluster: str) -> str:
        """Baseline file in GMC OUTPUT directory (no .new / .test)."""
        c = self._safe_cluster(cluster)
        meta = self._meta(config_type)
        prefix = meta["file_prefix"]
        out_dir = self._output_dir(config_type)
        return os.path.join(out_dir, "%s.%s" % (prefix, c))

    def generated_config_path(self, config_type: str, cluster: str) -> str:
        """
        Canonical generated path (primary artifact name).

        Prefer ``<prefix>.<cluster>.new``; if only ``.test`` exists (as on disk), return that
        so diff matches what the perl tool produced.
        """
        return self.resolved_generated_path_for_diff(config_type, cluster)

    def resolved_generated_path_for_diff(self, config_type: str, cluster: str) -> str:
        """
        Existing ``.new`` or ``.test`` under OUTPUT, else expected ``.new`` path.
        """
        return self._resolve_generated_after_run(config_type, cluster)

    def _resolve_generated_after_run(self, config_type: str, cluster: str) -> str:
        """Return existing generated file path (.new preferred, else .test)."""
        c = self._safe_cluster(cluster)
        meta = self._meta(config_type)
        prefix = meta["file_prefix"]
        out_dir = self._output_dir(config_type)
        for suffix in (".new", ".test"):
            p = os.path.join(out_dir, "%s.%s%s" % (prefix, c, suffix))
            if os.path.isfile(p):
                return p
        return os.path.join(out_dir, "%s.%s.new" % (prefix, c))

    def generate_config(
        self, config_type: str, cluster: str, context: Dict[str, Any]
    ) -> str:
        _ = context
        c = self._safe_cluster(cluster)
        script = self._script_path(config_type)
        if not os.path.isfile(script):
            raise ValueError("GMC script not found: %s" % script)
        cmd = [script, "-c", c]
        code, out, err = _run_pl(cmd, cwd=self._work_root, timeout=self._timeout)
        if code != 0:
            raise ValueError(_fmt_cmd_error(cmd, code, out, err))
        resolved = self._resolve_generated_after_run(config_type, c)
        if not os.path.isfile(resolved):
            raise ValueError(
                "GMC generate exited 0 but no .new/.test output found under %s (stdout=%r)"
                % (self._output_dir(config_type), (out or "")[:400])
            )
        return resolved

    def publish_to_request(
        self, config_type: str, cluster: str, request_number: str, generated_path: str
    ) -> str:
        _ = generated_path
        c = self._safe_cluster(cluster)
        req = _safe_reqno(request_number)
        script = self._script_path(config_type)
        if not os.path.isfile(script):
            raise ValueError("GMC script not found: %s" % script)
        cmd = [script, "-c", c, "-p", "-f", req, "-nu"]
        code, out, err = _run_pl(cmd, cwd=self._work_root, timeout=self._timeout)
        if code != 0:
            raise ValueError(_fmt_cmd_error(cmd, code, out, err))
        dest = os.path.join(self._requests_root, "REQ-%s" % req, config_type)
        return dest
