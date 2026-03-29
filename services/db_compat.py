"""
Read-only DB/schema compatibility diagnostics for ELIM/LS before MariaDB writes are enabled.

Run:
    python -m services.db_compat
"""


import json
import os
from typing import Any, Dict, List, Optional, Set, Tuple

from core.storage import load_json
from services.adapters import build_adapters
from services.elim_db import LICENSE_SERVER_KEYS, RESOURCE_REF_KEYS_BODY
from services.paths import project_root
from services.registry import load_all_definitions

TARGET_TABLES: Tuple[str, ...] = (
    "license_servers",
    "resources_REF",
    "resourcegroups",
    "clusters",
)


def _load_settings() -> Dict[str, Any]:
    return load_json(os.path.join(project_root(), "config", "settings.json"))


def _expected_columns_from_defs(defs: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for svc in defs:
        for field in svc.get("guided_inputs", []):
            if not isinstance(field, dict):
                continue
            table = field.get("table")
            col = field.get("column")
            if isinstance(table, str) and isinstance(col, str) and table and col:
                out.setdefault(table, set()).add(col)
            help_table = field.get("help_table")
            help_col = field.get("help_column")
            if isinstance(help_table, str) and isinstance(help_col, str):
                out.setdefault(help_table, set()).add(help_col)
            help_cols = field.get("help_columns")
            if isinstance(help_table, str) and isinstance(help_cols, list):
                for c in help_cols:
                    if isinstance(c, str) and c:
                        out.setdefault(help_table, set()).add(c)
    return out


def _workflow_insert_columns() -> Dict[str, Set[str]]:
    return {
        "license_servers": set(LICENSE_SERVER_KEYS),
        "resources_REF": set(RESOURCE_REF_KEYS_BODY) | {"license_server_ID"},
    }


def _required_columns_not_covered(
    schema_rows: List[Dict[str, Any]], covered: Set[str]
) -> List[str]:
    missing: List[str] = []
    for row in schema_rows:
        name = str(row.get("column_name") or "")
        if not name:
            continue
        nullable = str(row.get("is_nullable") or "").upper()
        default = row.get("column_default")
        extra = str(row.get("extra") or "").lower()
        if "auto_increment" in extra:
            continue
        if nullable == "YES":
            continue
        if default is not None:
            continue
        if name in covered:
            continue
        missing.append(name)
    return missing


def run_mariadb_compat_diagnostics(settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    s = settings or _load_settings()
    defs_root = str(s.get("service_defs_root") or "services/defs")
    defs = load_all_definitions(defs_root)
    expected_by_table = _expected_columns_from_defs(defs)
    insert_by_table = _workflow_insert_columns()

    db, _, _ = build_adapters(s)
    needed_methods = ("get_table_columns", "get_primary_key_columns", "get_table_schema")
    for m in needed_methods:
        if not hasattr(db, m):
            raise RuntimeError(
                f"configured DB adapter {type(db).__name__} does not expose schema diagnostics "
                f"method {m} (set service_db_backend=mariadb)"
            )

    confirmed: List[str] = []
    uncertain: List[str] = []
    incompatible: List[str] = []
    tables: Dict[str, Any] = {}

    for table in TARGET_TABLES:
        cols = list(getattr(db, "get_table_columns")(table))
        pks = list(getattr(db, "get_primary_key_columns")(table))
        schema_rows = list(getattr(db, "get_table_schema")(table))
        sample = getattr(db, "get_sample_row")(table) if hasattr(db, "get_sample_row") else None

        col_set = set(cols)
        expected_cols = sorted(expected_by_table.get(table, set()))
        missing_expected = sorted(c for c in expected_cols if c not in col_set)

        insert_cols = sorted(insert_by_table.get(table, set()))
        missing_insert = sorted(c for c in insert_cols if c not in col_set)

        pk_ok_for_current_flow = table not in ("license_servers", "resources_REF") or ("ID" in pks)
        if table in ("license_servers", "resources_REF"):
            covered_for_write = set(insert_cols) | {"ID"}
            required_not_covered = _required_columns_not_covered(schema_rows, covered_for_write)
        else:
            required_not_covered = []

        if missing_expected:
            incompatible.append(f"{table}: missing expected columns {missing_expected}")
        else:
            confirmed.append(f"{table}: all guided/help columns present")

        if missing_insert:
            incompatible.append(f"{table}: workflow insert columns missing {missing_insert}")
        elif table in insert_by_table:
            confirmed.append(f"{table}: all workflow insert columns exist")

        if pk_ok_for_current_flow:
            confirmed.append(f"{table}: PK columns = {pks or ['(none)']}")
        else:
            incompatible.append(f"{table}: PK does not include 'ID' (found {pks})")

        if required_not_covered:
            uncertain.append(
                f"{table}: DB has non-null/no-default columns not in workflow inputs {required_not_covered}"
            )

        if sample is None:
            uncertain.append(f"{table}: no sample row available for prefill checks")
        elif isinstance(sample, dict):
            sample_missing = sorted(c for c in expected_cols if c not in sample)
            if sample_missing:
                uncertain.append(
                    f"{table}: sample row missing some expected keys {sample_missing}"
                )

        tables[table] = {
            "primary_key_columns": pks,
            "expected_columns_from_defs": expected_cols,
            "missing_expected_columns": missing_expected,
            "workflow_insert_columns": insert_cols,
            "missing_workflow_insert_columns": missing_insert,
            "required_columns_not_covered_by_workflow": required_not_covered,
            "sample_row_present": isinstance(sample, dict),
            "sample_row_keys": sorted(sample.keys()) if isinstance(sample, dict) else [],
        }

    return {
        "db_adapter": type(db).__name__,
        "database_backend": s.get("service_db_backend"),
        "definitions_analyzed": [str(d.get("id")) for d in defs],
        "tables": tables,
        "confirmed_compatible": confirmed,
        "uncertain_or_mapping_needed": uncertain,
        "incompatible_or_missing": incompatible,
    }


def main() -> int:
    try:
        report = run_mariadb_compat_diagnostics()
    except Exception as e:  # pragma: no cover - diagnostic entrypoint
        print(json.dumps({"ok": False, "error": str(e)}, indent=2))
        return 1
    print(json.dumps({"ok": True, "report": report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
