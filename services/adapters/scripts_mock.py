"""
Mock scripts: generated configs under ``<sandbox>/generated/<cluster>/`` and publish into request dir.
"""


import os
import shutil
import uuid
from typing import Any, Callable, Dict, List, Optional

from services.adapters.request_local import LocalLsfRequestDirAdapter
from services.adapters.scripts_base import ScriptsAdapterBase
from services.elim_db import LS_SERVICE_ID

LSB_RESOURCES = "lsb.resources"
LSF_SHARED = "lsf.shared"
LSF_CLUSTER = "lsf.cluster"
LSF_LICENSESCHEDULER = "lsf.licensescheduler"

ELIM_MOCK_CONFIG_TYPES = frozenset({LSB_RESOURCES, LSF_SHARED, LSF_CLUSTER})
# Filenames published flat into REQ-<n>/ (no extra extension mangling).
REQUEST_TREE_MOCK_CONFIG_TYPES = frozenset(
    {LSB_RESOURCES, LSF_SHARED, LSF_CLUSTER, LSF_LICENSESCHEDULER}
)

MARKER_START = "--- BEGIN OpsCore ELIM generated block ---"
MARKER_END = "--- END OpsCore ELIM generated block ---"
MARKER_START_LS = "--- BEGIN OpsCore LS generated block ---"
MARKER_END_LS = "--- END OpsCore LS generated block ---"


class MockScriptsAdapter(ScriptsAdapterBase):
    def __init__(self, sandbox_root: str) -> None:
        self._sandbox_root = sandbox_root
        self._requests = LocalLsfRequestDirAdapter(sandbox_root)

    @staticmethod
    def _safe_cluster(cluster: str) -> str:
        c = str(cluster).strip()
        if not c or any(x in c for x in ("/", "\\", "..")):
            raise ValueError("invalid cluster")
        return c

    def generate_config(
        self, config_type: str, cluster: str, context: Dict[str, Any]
    ) -> str:
        if config_type == LSB_RESOURCES:
            return self._generate_elim_file(
                cluster,
                context,
                LSB_RESOURCES,
                "# (place file at data/sandbox/current/<cluster>/lsb.resources)\n",
                self._block_lsb_resources,
            )
        if config_type == LSF_SHARED:
            return self._generate_elim_file(
                cluster,
                context,
                LSF_SHARED,
                "# (place file at data/sandbox/current/<cluster>/lsf.shared)\n",
                self._block_lsf_shared,
            )
        if config_type == LSF_CLUSTER:
            return self._generate_elim_file(
                cluster,
                context,
                LSF_CLUSTER,
                "# (place file at data/sandbox/current/<cluster>/lsf.cluster)\n",
                self._block_lsf_cluster,
            )
        if config_type == LSF_LICENSESCHEDULER:
            return self._generate_elim_file(
                cluster,
                context,
                LSF_LICENSESCHEDULER,
                "# (place file at data/sandbox/current/<cluster>/lsf.licensescheduler)\n",
                self._block_lsf_licensescheduler,
            )
        safe_cluster = self._safe_cluster(cluster)
        out_dir = os.path.join(self._sandbox_root, "generated", safe_cluster)
        os.makedirs(out_dir, exist_ok=True)
        name = f"{config_type}_{uuid.uuid4().hex[:8]}.mock.txt"
        path = os.path.join(out_dir, name)
        lines = [
            f"# mock generated {config_type}",
            f"cluster={cluster}",
            f"context_keys={sorted(context.keys())!r}",
        ]
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        return path

    def _read_file_or_empty(self, path: str) -> str:
        if not os.path.isfile(path):
            return ""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

    @staticmethod
    def _strip_prior_generated_block(content: str) -> str:
        """Remove appended mock blocks (ELIM or LS) before re-generation."""
        while True:
            stripped = False
            for start in (MARKER_START_LS + "\n", MARKER_START + "\n"):
                idx = content.find(start)
                if idx != -1:
                    content = content[:idx].rstrip()
                    stripped = True
                    break
            if not stripped:
                return content

    def _generate_elim_file(
        self,
        cluster: str,
        context: Dict[str, Any],
        config_type: str,
        empty_hint_line: str,
        block_fn: Callable[..., str],
    ) -> str:
        safe = self._safe_cluster(cluster)
        cur_dir = os.path.join(self._sandbox_root, "current", safe)
        out_dir = os.path.join(self._sandbox_root, "generated", safe)
        os.makedirs(cur_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)
        current_path = os.path.join(cur_dir, config_type)
        out_path = os.path.join(out_dir, f"{config_type}.new")
        base = self._read_file_or_empty(current_path)
        if not base.strip():
            base = (
                f"# OpsCore sandbox: no current {config_type} for this cluster yet\n"
                f"{empty_hint_line}"
            )
        base = self._strip_prior_generated_block(base)
        si = context.get("service_inputs") if isinstance(context.get("service_inputs"), dict) else {}
        req = str(context.get("request_number") or "")
        gen_cluster = str(context.get("generation_cluster") or cluster)
        block = block_fn(si, req, gen_cluster, context)
        content = base.rstrip() + "\n\n" + block + "\n"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(content)
        return out_path

    @staticmethod
    def _block_lsb_resources(
        si: Dict[str, Any],
        request_number: str,
        _cluster: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        res = str(si.get("resource") or "").strip()
        desc = str(si.get("description") or "").strip()
        lic = str(si.get("licservers") or "").strip()
        feat = str(si.get("features") or "").strip()
        sid = str((context or {}).get("service_id") or "")
        lines: List[str] = [
            MARKER_START,
            "# OpsCore mock: lsb.resources (simplified, not full LSF syntax)",
            f"# request_number={request_number}",
        ]
        if sid == LS_SERVICE_ID:
            uls = str(si.get("useLS") or "").strip()
            wld = str(si.get("wlDistLS") or "").strip()
            lines.append("# LS path: License Scheduler–oriented resource (see lsf.licensescheduler mock)")
            lines.append(f"# useLS {uls}")
            lines.append(f"# wlDistLS {wld}")
        lines.extend(
            [
                f"resource {res}",
                f"description {desc}",
                f"# licservers {lic}",
                f"# features {feat}",
                MARKER_END,
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def _block_lsf_shared(
        si: Dict[str, Any],
        request_number: str,
        _cluster: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        _ = context
        res = str(si.get("resource") or "").strip()
        lic = str(si.get("licservers") or "").strip()
        feat = str(si.get("features") or "").strip()
        lines = [
            MARKER_START,
            "# OpsCore mock: lsf.shared (not real LSF syntax)",
            f"# request_number={request_number}",
            f"# resource_name {res}",
            f"# license_server_info {lic}",
            f"# features {feat}",
            MARKER_END,
        ]
        return "\n".join(lines)

    @staticmethod
    def _block_lsf_cluster(
        si: Dict[str, Any],
        request_number: str,
        cluster_ref: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        _ = context
        res = str(si.get("resource") or "").strip()
        lines = [
            MARKER_START,
            "# OpsCore mock: lsf.cluster (not real LSF syntax)",
            f"# request_number={request_number}",
            f"# generation_context_cluster {cluster_ref}",
            f"# resource_name {res}",
            MARKER_END,
        ]
        return "\n".join(lines)

    @staticmethod
    def _block_lsf_licensescheduler(
        si: Dict[str, Any],
        request_number: str,
        cluster_ref: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        _ = context
        res = str(si.get("resource") or "").strip()
        uls = str(si.get("useLS") or "").strip()
        wld = str(si.get("wlDistLS") or "").strip()
        lines = [
            MARKER_START_LS,
            "# OpsCore mock: lsf.licensescheduler (LS path; not production LSF syntax)",
            f"# request_number={request_number}",
            f"# generation_context_cluster {cluster_ref}",
            f"# resource_name {res}",
            f"useLS {uls}",
            f"wlDistLS {wld}",
            MARKER_END_LS,
        ]
        return "\n".join(lines)

    def publish_to_request(
        self, config_type: str, cluster: str, request_number: str, generated_path: str
    ) -> str:
        req_dir = self._requests.get_request_path(request_number)
        os.makedirs(req_dir, exist_ok=True)
        if config_type in REQUEST_TREE_MOCK_CONFIG_TYPES:
            dest = os.path.join(req_dir, config_type)
        else:
            dest_name = os.path.basename(generated_path)
            dest = os.path.join(req_dir, dest_name)
        shutil.copy2(generated_path, dest)
        return dest
