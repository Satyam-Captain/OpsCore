#!/usr/bin/env python3
"""
CLI smoke test for the configured inventory provider (mock or ssh).

Run from the OpsCore application directory (where config/ and app.py live):

  python test_provider.py --domain scripts_bin --sources cluster_a
  python test_provider.py --domain scripts_bin --path preexec.sh --sources cluster_a,cluster_b
  python test_provider.py --domain scripts_bin --sources gmc --recursive

On office/GMC, set provider to \"ssh\" in config/settings.json first, then verify SSH works here
before starting Flask (BatchMode SSH, keys, host names).

``--path``: with ``provider=ssh``, this is a relative path under the domain root (file or
directory target). With ``provider=mock``, it remains a substring filter (unchanged).
"""


import argparse
import os
import sys
from pathlib import Path
from typing import List

# Ensure config/*.json paths resolve when cwd is not the app root
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

from core.models import Domain, Source  # noqa: E402
from core.scanner import run_scan  # noqa: E402
from core.storage import load_json  # noqa: E402
from providers.factory import create_inventory_provider  # noqa: E402


def _load_settings():
    return load_json("config/settings.json")


def _load_domains() -> List[Domain]:
    raw = load_json("config/domains.json")
    return [Domain(**item) for item in raw["domains"]]


def _load_sources() -> List[Source]:
    raw = load_json("config/sources.json")
    items = raw.get("sources") or []
    return [Source(**item) for item in items if item.get("enabled", True)]


def main() -> int:
    parser = argparse.ArgumentParser(description="OpsCore inventory provider smoke test")
    parser.add_argument("--domain", required=True, help="Domain id from domains.json")
    parser.add_argument(
        "--path",
        default="",
        help="Scan target: ssh = relative path under domain root (file/dir); mock = substring filter",
    )
    parser.add_argument(
        "--sources",
        required=True,
        help="Comma-separated source ids (must exist in sources.json)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursive directory scan (provider-dependent)",
    )
    args = parser.parse_args()

    settings = _load_settings()
    domains = {d.id: d for d in _load_domains()}
    domain = domains.get(args.domain.strip())
    if not domain:
        print(f"Unknown domain {args.domain!r}", file=sys.stderr)
        return 2

    want_ids = [x.strip() for x in args.sources.split(",") if x.strip()]
    by_id = {s.id: s for s in _load_sources()}
    selected = [by_id[i] for i in want_ids if i in by_id]
    missing = set(want_ids) - {s.id for s in selected}
    if missing:
        print(f"Unknown or disabled sources: {sorted(missing)}", file=sys.stderr)
        return 2
    if not selected:
        print("No sources selected.", file=sys.stderr)
        return 2

    provider = create_inventory_provider(settings)
    mode = settings.get("provider", "mock")
    print(f"Provider mode: {mode}")
    print(f"Domain: {domain.id} ({domain.label})")
    print(f"Sources: {', '.join(s.id for s in selected)}")
    print(f"path filter: {args.path!r}  recursive={args.recursive}")
    print("---")

    result = run_scan(
        provider=provider,
        domain=domain,
        sources=selected,
        path_input=args.path,
        recursive=args.recursive,
    )

    for w in result.warnings:
        print(f"WARNING: {w}")

    # Flatten records from logical groups
    n = 0
    for group in result.groups:
        for rec in group.records:
            n += 1
            warn = f"  warn={rec.warning}" if rec.warning else ""
            print(
                f"{rec.source_id}\t{rec.relative_path}\t"
                f"exists={rec.exists}\tsize={rec.size}\t"
                f"mtime={rec.mtime}\tchecksum={rec.checksum}{warn}"
            )
            print(f"  abs: {rec.absolute_path}")

    print("---")
    print(f"Total file records: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
