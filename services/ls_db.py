"""
Add License Resource (LS): database preview, apply, and rollback use the same mechanics as ELIM.

LS-specific code should import from here when you want an explicit LS entry point; the
implementation is shared in ``elim_db`` (same tables and steps).
"""

from __future__ import annotations

from services.elim_db import (
    LS_SERVICE_ID,
    TABLE_LICENSE_SERVERS,
    TABLE_RESOURCES_REF,
    apply_elim_inserts as apply_ls_inserts,
    apply_elim_license_server_only as apply_ls_license_server_only,
    apply_elim_resource_only as apply_ls_resource_only,
    build_elim_preview as build_ls_db_preview,
    build_rollback_stack as build_ls_rollback_stack,
    build_rollback_stack_license_only as build_ls_rollback_stack_license_only,
    elim_db_already_applied as ls_db_already_applied,
    elim_db_fully_applied as ls_db_fully_applied,
    validate_elim_service_inputs as validate_ls_service_inputs,
)

__all__ = [
    "LS_SERVICE_ID",
    "TABLE_LICENSE_SERVERS",
    "TABLE_RESOURCES_REF",
    "apply_ls_inserts",
    "apply_ls_license_server_only",
    "apply_ls_resource_only",
    "build_ls_db_preview",
    "build_ls_rollback_stack",
    "build_ls_rollback_stack_license_only",
    "ls_db_already_applied",
    "ls_db_fully_applied",
    "validate_ls_service_inputs",
]
