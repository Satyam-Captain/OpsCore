"""
SSH-backed inventory using the system ``ssh`` binary (subprocess).

**Scan targeting (path_input):**
- Empty → scan the configured domain root (files only; depth depends on ``recursive``).
- Non-empty → treated as a **relative path under the domain root** (validated via
  ``core.path_safety``). Remote probe decides:
  * **file** → one ``FileRecord`` for that path.
  * **directory** → ``find`` only under that directory; ``recursive`` controls depth.
  * **missing** → empty record list (no substring matching).

**Assumptions (office/GMC):**
- Linux + GNU coreutils: ``find``, ``stat``, ``sha256sum``, ``bash``.
- Domain roots in ``domains.json`` are absolute POSIX paths on the remote host.
- Non-interactive SSH (``BatchMode=yes``).

**Not used:** Paramiko.
"""

from __future__ import annotations

import getpass
import logging
import shlex
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from core.models import Domain, FileRecord, Source
from core.path_safety import domain_root_for_source, normalize_user_relative_path, posix_absolute_under_root
from providers.base import InventoryProvider
from providers.exceptions import SshReadError

logger = logging.getLogger(__name__)


class SshInventoryProvider(InventoryProvider):
    """Collect file metadata and contents over SSH using ``ssh user@host`` + remote bash."""

    def __init__(self, settings: Dict[str, Any]) -> None:
        self._timeout = int(settings.get("scan_timeout_seconds", 120))
        self._max_files = int(settings.get("max_files_per_scan", 500))
        extra = settings.get("ssh_extra_args")
        self._ssh_extra: List[str] = list(extra) if isinstance(extra, list) else []
        self._strict_host_key = str(
            settings.get("ssh_strict_host_key_checking", "accept-new")
        ).strip() or "accept-new"

    def _ssh_user(self, source: Source) -> str:
        u = (source.ssh_user or "").strip()
        if u:
            return u
        return getpass.getuser()

    def _ssh_cmd_prefix(self, source: Source) -> List[str]:
        host = (source.host or "").strip()
        if not host:
            raise ValueError(f"source {source.id!r} has empty host (required for ssh mode)")

        user = self._ssh_user(source)
        target = f"{user}@{host}"
        cmd: List[str] = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={int(self._settings_connect_timeout())}",
            "-o",
            f"StrictHostKeyChecking={self._strict_host_key}",
        ]
        cmd.extend(self._ssh_extra)
        cmd.append(target)
        return cmd

    def _settings_connect_timeout(self) -> int:
        return min(30, max(5, self._timeout // 4 or 15))

    def _run_remote_script(self, source: Source, script: str) -> subprocess.CompletedProcess[bytes]:
        cmd = self._ssh_cmd_prefix(source) + ["bash", "-s", "--"]
        return subprocess.run(
            cmd,
            input=script.encode("utf-8"),
            capture_output=True,
            timeout=self._timeout,
        )

    def _run_remote_argv(self, source: Source, argv: List[str]) -> subprocess.CompletedProcess[bytes]:
        cmd = self._ssh_cmd_prefix(source) + argv
        return subprocess.run(cmd, capture_output=True, timeout=self._timeout)

    def _resolve_scan_target(self, domain: Domain, source: Source, path_input: str) -> Tuple[str, str]:
        """
        Return (domain_root, target_absolute_path).

        ``path_input`` empty → target is domain root.
        Otherwise normalize as relative under root and join (raises ValueError if unsafe).
        """
        root = domain_root_for_source(domain.roots, source.type)
        posix_absolute_under_root(root, "")
        frag = (path_input or "").strip()
        if not frag:
            target_abs = posix_absolute_under_root(root, "")
        else:
            rel = normalize_user_relative_path(frag)
            target_abs = posix_absolute_under_root(root, rel)
        return root, target_abs

    def scan_domain(
        self,
        source: Source,
        domain: Domain,
        path_input: str = "",
        recursive: bool = False,
    ) -> List[FileRecord]:
        root, target_abs = self._resolve_scan_target(domain, source, path_input)
        root_q = shlex.quote(root)
        target_q = shlex.quote(target_abs)
        max_files = max(1, self._max_files)
        rec_flag = "1" if recursive else "0"

        # One remote script: validate TARGET under ROOT, probe type, emit TSV or status line.
        script = f"""set -uo pipefail
ROOT={root_q}
TARGET={target_q}
ROOT="${{ROOT%/}}"
MAX={max_files}
REC={rec_flag}

_remote_under_root() {{
  case "$TARGET" in
    "$ROOT"|"$ROOT"/*) return 0 ;;
    *) return 1 ;;
  esac
}}

_rel_from_root() {{
  local t="$1"
  if [ "$t" = "$ROOT" ]; then printf '.'
  elif [[ "$t" == "$ROOT"/* ]]; then printf '%s' "${{t#"$ROOT"/}}"
  else echo "OPSCORE_SCAN_RESULT|badroot|$t" >&2; return 1
  fi
}}

if ! _remote_under_root; then
  printf 'OPSCORE_SCAN_RESULT|badroot\\n'
  exit 3
fi

if [ ! -e "$TARGET" ]; then
  printf 'OPSCORE_SCAN_RESULT|missing\\n'
  exit 0
fi

# Regular file (including symlink to file)
if [ -f "$TARGET" ]; then
  rel=$(_rel_from_root "$TARGET") || exit 3
  sz=$(stat -c '%s' "$TARGET" 2>/dev/null) || {{ printf 'OPSCORE_SCAN_RESULT|statfail|%s\\n' "$rel"; exit 0; }}
  mt=$(stat -c '%Y' "$TARGET" 2>/dev/null) || {{ printf 'OPSCORE_SCAN_RESULT|statfail|%s\\n' "$rel"; exit 0; }}
  sum=$(sha256sum "$TARGET" 2>/dev/null | awk '{{print $1}}') || true
  if [ -z "$sum" ]; then
    printf '%s\\t%s\\t%s\\t\\n' "$rel" "$sz" "$mt"
  else
    printf '%s\\t%s\\t%s\\t%s\\n' "$rel" "$sz" "$mt" "$sum"
  fi
  exit 0
fi

# Directory: scan only inside TARGET
if [ -d "$TARGET" ]; then
  PREFIX=$(_rel_from_root "$TARGET") || exit 3
  cd "$TARGET" || {{ printf 'OPSCORE_SCAN_RESULT|cdfail\\n'; exit 0; }}
  count=0
  if [ "$REC" = "1" ]; then
    FINDSPEC=(find . -type f -print0)
  else
    FINDSPEC=(find . -maxdepth 1 -mindepth 1 -type f -print0)
  fi
  while IFS= read -r -d '' f; do
    inner="${{f#./}}"
    if [ -z "$inner" ]; then
      continue
    fi
    if [ "$PREFIX" = "." ]; then
      rel_out="$inner"
    else
      rel_out="$PREFIX/$inner"
    fi
    sz=$(stat -c '%s' "$f" 2>/dev/null) || continue
    mt=$(stat -c '%Y' "$f" 2>/dev/null) || continue
    sum=$(sha256sum "$f" 2>/dev/null | awk '{{print $1}}') || true
    if [ -z "$sum" ]; then
      printf '%s\\t%s\\t%s\\t\\n' "$rel_out" "$sz" "$mt"
    else
      printf '%s\\t%s\\t%s\\t%s\\n' "$rel_out" "$sz" "$mt" "$sum"
    fi
    count=$((count + 1))
    if [ "$count" -ge "$MAX" ]; then
      break
    fi
  done < <("${{FINDSPEC[@]}}")
  exit 0
fi

printf 'OPSCORE_SCAN_RESULT|unsupported|not a regular file or directory\\n'
exit 0
"""

        proc = self._run_remote_script(source, script)
        if proc.returncode not in (0, 3):
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"ssh scan failed for {source.id}: {err or proc.returncode}")

        text = proc.stdout.decode("utf-8", errors="replace")
        if proc.returncode == 3:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            raise ValueError(
                f"{source.id}: SSH scan aborted (remote exit 3 — path validation or shell error). "
                f"{err or text.strip() or 'no stderr'}"
            )

        return self._parse_scan_lines(
            source=source,
            domain=domain,
            domain_root=root,
            stdout=text,
        )

    def _parse_scan_lines(
        self,
        *,
        source: Source,
        domain: Domain,
        domain_root: str,
        stdout: str,
    ) -> List[FileRecord]:
        records: List[FileRecord] = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("OPSCORE_SCAN_RESULT|"):
                parts = line.split("|", 2)
                kind = parts[1] if len(parts) > 1 else ""
                if kind == "missing":
                    logger.info("ssh scan %s: target path does not exist on host", source.id)
                    return []
                if kind == "statfail":
                    rel_hint = parts[2] if len(parts) > 2 else "?"
                    logger.warning("ssh scan %s: stat/checksum failed for %s", source.id, rel_hint)
                    return []
                if kind == "cdfail":
                    logger.warning("ssh scan %s: could not cd into directory target", source.id)
                    return []
                if kind == "unsupported":
                    detail = parts[2] if len(parts) > 2 else kind
                    logger.warning("ssh scan %s: unsupported target (%s)", source.id, detail)
                    return []
                continue

            parts = line.split("\t")
            if len(parts) < 3:
                continue
            rel, sz_s, mt_s = parts[0], parts[1], parts[2]
            sha = parts[3] if len(parts) > 3 else ""

            try:
                abs_path = posix_absolute_under_root(domain_root, rel)
            except ValueError as exc:
                logger.warning("ssh scan %s: skip line (path safety): %s", source.id, exc)
                continue

            try:
                size = int(sz_s)
            except ValueError:
                size = None
            try:
                mt_epoch = int(float(mt_s))
                mtime = datetime.fromtimestamp(mt_epoch, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
            except (ValueError, OSError, OverflowError):
                mtime = ""

            warning = None
            if not sha:
                warning = "checksum unavailable"

            records.append(
                FileRecord(
                    source_id=source.id,
                    domain_id=domain.id,
                    absolute_path=abs_path,
                    relative_path=rel,
                    exists=True,
                    is_file=True,
                    checksum=sha or None,
                    size=size,
                    mtime=mtime or None,
                    warning=warning,
                )
            )

        return records

    def read_file_content(
        self,
        source: Source,
        domain: Domain,
        relative_path: str,
    ) -> str:
        root = domain_root_for_source(domain.roots, source.type)
        try:
            rel = normalize_user_relative_path(relative_path)
        except ValueError as exc:
            raise ValueError(f"{source.id}: invalid relative_path {relative_path!r}: {exc}") from exc
        if not rel:
            return ""
        try:
            abs_path = posix_absolute_under_root(root, rel)
        except ValueError as exc:
            raise ValueError(
                f"{source.id}: path {relative_path!r} escapes domain root: {exc}"
            ) from exc

        try:
            proc = self._run_remote_argv(source, ["cat", "--", abs_path])
        except subprocess.TimeoutExpired as exc:
            raise SshReadError(
                f"{source.id}: ssh cat timed out for {abs_path}",
                source_id=source.id,
                path=abs_path,
                returncode=None,
                stderr="",
            ) from exc

        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            raise SshReadError(
                f"{source.id}: cannot read {abs_path} (ssh exit {proc.returncode}): {err or 'no stderr'}",
                source_id=source.id,
                path=abs_path,
                returncode=proc.returncode,
                stderr=err,
            )

        return proc.stdout.decode("utf-8", errors="replace")
