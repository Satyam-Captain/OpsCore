"""Flask routes for the service layer (registry, start form, operation view)."""


import os
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from core.storage import load_json
from core.text_diff import unified_diff_text
from services.adapters import build_adapters
from services.adapters.db_base import DbAdapterBase
from services.adapters.scripts_base import ScriptsAdapterBase
from services.elim_lsb import (
    ELIM_CONFIG_TYPES,
    LS_CONFIG_TYPES,
    current_config_path,
    elim_config_ui_rows,
    generated_config_path,
    is_allowed_elim_config_type,
    merge_generation_step_result,
    merge_publish_step_result,
    normalize_generation_cluster,
    read_text_file,
)
from services.elim_db import (
    TABLE_LICENSE_SERVERS,
    TABLE_RESOURCES_REF,
    apply_elim_inserts,
    apply_elim_license_server_only,
    apply_elim_resource_only,
    build_db_apply_step_license_server,
    build_db_apply_step_resource,
    build_db_apply_step_results,
    build_elim_preview,
    build_resource_ref_row,
    build_rollback_preview_items,
    build_rollback_stack,
    build_rollback_stack_license_only,
    elim_can_rollback,
    elim_db_already_applied,
    elim_db_fully_applied,
    elim_db_step_done,
    elim_inserted_id_for_step,
    elim_rollback_executed,
    execute_elim_rollback,
    get_db_apply_steps,
    is_elim_operation,
    is_license_resource_db_operation,
    is_ls_operation,
    merge_db_apply_steps,
    validate_elim_service_inputs,
)
from services.elim_finalize import (
    compute_license_resource_readiness,
    create_finalized_marker,
    finalize_timestamp_iso,
    merge_finalization_step_result,
    marker_path_for_storage,
)
from services.authz import (
    clear_role,
    get_current_role,
    is_superadmin,
    try_superadmin_login,
)
from services.blueprint import (
    build_config_workspace_view_model,
    build_table_workspace_view_model,
    build_workspace_view_model,
    get_blueprint_finalization_config_ids,
    get_blueprint_generation_config_ids,
)
from services.engine import start_service_operation
from services.lifecycle_ops import merge_lifecycle_status
from services.input_help import build_help_payload, find_guided_field, iter_guided_fields
from services.paths import project_root
from services.registry import ServiceRegistryError, get_definition_by_id, load_all_definitions
from services.state_store import (
    delete_operation,
    find_all_active_workspaces,
    find_operations,
    list_operations,
    load_operation,
    update_operation,
)
from services.workspace_lifecycle import effective_lifecycle_status, lifecycle_closed_at

services_bp = Blueprint("services", __name__, url_prefix="/services")


def _load_settings() -> Dict[str, Any]:
    path = os.path.join(project_root(), "config", "settings.json")
    return load_json(path)


def _db_adapter(settings: Dict[str, Any]) -> DbAdapterBase:
    db, _, _ = build_adapters(settings)
    return db


def _optional_db_adapter(settings: Dict[str, Any]) -> Optional[DbAdapterBase]:
    """For blueprint prefill; None if the configured DB backend cannot be constructed."""
    try:
        return _db_adapter(settings)
    except NotImplementedError:
        return None


def _scripts_adapter(settings: Dict[str, Any]) -> ScriptsAdapterBase:
    _, _, scripts = build_adapters(settings)
    return scripts


def _split_guided_groups(
    guided: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ls = [f for f in guided if f.get("group") == "license_server"]
    rr = [f for f in guided if f.get("group") == "resource_ref"]
    return ls, rr


def _readiness_for_license_resource_service(
    settings: Dict[str, Any],
    doc: Dict[str, Any],
    svc: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    ids = get_blueprint_finalization_config_ids(svc)
    if not ids:
        ids = LS_CONFIG_TYPES if is_ls_operation(doc) else ELIM_CONFIG_TYPES
    return compute_license_resource_readiness(settings, doc, ids)


def _elim_config_rows_for_operation(
    settings: Dict[str, Any],
    doc: Dict[str, Any],
    svc: Optional[Dict[str, Any]],
    sr: Dict[str, Any],
    elim_config_enabled: bool,
    gen_cluster: Optional[str],
) -> List[Dict[str, Any]]:
    ids = get_blueprint_generation_config_ids(svc)
    if not ids:
        ids = LS_CONFIG_TYPES if is_ls_operation(doc) else ELIM_CONFIG_TYPES
    return elim_config_ui_rows(
        settings,
        sr,
        elim_config_enabled,
        gen_cluster,
        config_types=ids,
    )


def _config_type_allowed_for_operation(
    config_type: str, svc: Optional[Dict[str, Any]]
) -> bool:
    ids = get_blueprint_generation_config_ids(svc)
    if ids:
        return config_type in ids
    return is_allowed_elim_config_type(config_type)


def _has_license_resource_inputs(doc: Dict[str, Any]) -> bool:
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    return is_license_resource_db_operation(doc) and len(si) > 0


def _flash_global_start_blocked(active: List[Dict[str, Any]]) -> None:
    """Operator-facing flashes when POST create is blocked by the global active-workspace lock."""
    flash("Cannot create a new request right now.", "error")
    if len(active) == 1:
        rn = str(active[0].get("request_number") or "").strip() or "—"
        flash(
            f"Request workflow `{rn}` is already open and active. "
            "Please finish it, finalize it, or roll it back before starting a new request.",
            "error",
        )
    else:
        flash(
            "There are active request workflows in progress. "
            "Please finish, finalize, or roll back each one before starting a new request.",
            "error",
        )
    flash("Only superadmin may override this.", "error")


def _guard_lifecycle_mutations(doc: Dict[str, Any], operation_id: str):
    """Block DB / generation / publish / inputs writes when request lifecycle is closed."""
    if effective_lifecycle_status(doc) != "active":
        flash(
            "This request is closed. No further database, generation, or publish actions are allowed.",
            "error",
        )
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    return None


def _elim_run_finalize_marker(
    settings: Dict[str, Any],
    operation_id: str,
    doc: Dict[str, Any],
    svc: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    ELIM/LS: create/readiness-validated .finalized marker and merged step_results.
    Flash and return None on failure. Call only for license-resource DB operations.
    """
    if not is_license_resource_db_operation(doc):
        return None
    readiness = _readiness_for_license_resource_service(settings, doc, svc)
    if not readiness["ready"]:
        flash(
            "Request is not ready to finalize. Complete all required config generation and publish steps.",
            "error",
        )
        return None
    req_no = str(doc.get("request_number") or "").strip()
    if not req_no:
        flash("Operation has no request number.", "error")
        return None
    try:
        abs_path, mk_status = create_finalized_marker(settings, req_no)
    except ValueError as e:
        flash(str(e), "error")
        return None
    stored_path = marker_path_for_storage(abs_path)
    ts = finalize_timestamp_iso()
    raw_sr = doc.get("step_results")
    sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
    fin_status = "already_present" if mk_status == "already_present" else "done"
    return merge_finalization_step_result(
        sr,
        status=fin_status,
        marker_path=stored_path,
        created_at=ts,
    )


def _operation_display_row(settings: Dict[str, Any], doc: Dict[str, Any]) -> Dict[str, Any]:
    """Stable display fields for list templates (tolerates older JSON)."""
    root = settings.get("service_defs_root", "services/defs")
    sid = str(doc.get("service_id") or "")
    service_name = sid or "—"
    try:
        svc = get_definition_by_id(sid, root)
        if svc and svc.get("name"):
            service_name = str(svc["name"])
    except ServiceRegistryError:
        pass
    oid = str(doc.get("operation_id") or "")
    wc = doc.get("workspace_control")
    if isinstance(wc, dict):
        role_at_creation = str(wc.get("role_at_creation") or "—")
        is_override = bool(wc.get("is_superadmin_override"))
    else:
        role_at_creation = "—"
        is_override = False
    return {
        "operation_id": oid,
        "service_id": sid or "—",
        "service_name": service_name,
        "request_number": str(doc.get("request_number") or "") or "—",
        "status": str(doc.get("status") or "") or "—",
        "updated_at": str(doc.get("updated_at") or doc.get("created_at") or "") or "—",
        "role_at_creation": role_at_creation,
        "is_superadmin_override": is_override,
        "lifecycle_status": effective_lifecycle_status(doc),
    }


@services_bp.before_request
def _require_service_enabled() -> None:
    if not _load_settings().get("service_enabled", True):
        abort(404)


@services_bp.route("/access", methods=["GET", "POST"])
def access():
    """Prototype role control: superadmin password in session (replace with real auth later)."""
    if request.method == "POST":
        pwd = request.form.get("superadmin_password") or ""
        if try_superadmin_login(pwd):
            flash("Session role elevated to superadmin (prototype).", "ok")
        elif pwd.strip():
            flash("Incorrect password.", "error")
        return redirect(url_for("services.access"))
    return render_template(
        "services/access.html",
        role=get_current_role(),
    )


@services_bp.route("/logout-role", methods=["POST"])
def logout_role():
    clear_role()
    flash("Session role reset to admin.", "ok")
    return redirect(url_for("services.access"))


@services_bp.route("/admin/dashboard")
def admin_dashboard():
    """All operations — superadmin only (prototype visibility)."""
    settings = _load_settings()
    if not is_superadmin():
        flash("Superadmin role is required. Use Service access to sign in.", "error")
        return redirect(url_for("services.access"))
    docs = list_operations(settings)
    rows = [_operation_display_row(settings, d) for d in docs]
    return render_template("services/admin_dashboard.html", operations=rows)


@services_bp.route("/")
def index():
    settings = _load_settings()
    root = settings.get("service_defs_root", "services/defs")
    try:
        services = load_all_definitions(root)
    except ServiceRegistryError as e:
        flash(str(e), "error")
        services = []
    return render_template("services/index.html", services=services)


@services_bp.route("/operations")
def operations_list():
    """List persisted operations; optional ``request_number`` / ``service_id`` query filters."""
    settings = _load_settings()
    rn = (request.args.get("request_number") or "").strip() or None
    sid = (request.args.get("service_id") or "").strip() or None
    docs = find_operations(settings, service_id=sid, request_number=rn)
    rows = [_operation_display_row(settings, d) for d in docs]
    filter_request_number = request.args.get("request_number", "").strip()
    filter_service_id = request.args.get("service_id", "").strip()
    return render_template(
        "services/operations_index.html",
        operations=rows,
        filter_request_number=filter_request_number,
        filter_service_id=filter_service_id,
    )


@services_bp.route("/operations/<operation_id>")
def operation_view(operation_id: str):
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    service_name = (svc or {}).get("name") or doc.get("service_id")
    guided = iter_guided_fields(svc or {})
    has_guided_inputs = len(guided) > 0
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    is_elim = is_elim_operation(doc)
    is_ls = is_ls_operation(doc)
    has_elim_inputs = _has_license_resource_inputs(doc)
    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    db_apply_steps = sr.get("db_apply")
    has_db_apply = isinstance(db_apply_steps, list) and len(db_apply_steps) > 0
    rb = doc.get("rollback_stack") if isinstance(doc.get("rollback_stack"), list) else []
    has_rollback_stack = len(rb) > 0
    rollback_executed = elim_rollback_executed(doc)
    rollback_results: List[Any] = []
    if rollback_executed:
        rr = sr.get("rollback")
        if isinstance(rr, list):
            rollback_results = rr
    show_rollback_button = elim_can_rollback(doc)
    elim_config_enabled = is_license_resource_db_operation(doc) and has_db_apply
    gen_cluster = normalize_generation_cluster(doc.get("generation_context_cluster"))
    can_set_generation_context = elim_config_enabled
    elim_config_rows = _elim_config_rows_for_operation(
        settings, doc, svc, sr, elim_config_enabled, gen_cluster
    )
    elim_readiness_summary = None
    elim_finalization = None
    if is_license_resource_db_operation(doc):
        rr = _readiness_for_license_resource_service(settings, doc, svc)
        elim_readiness_summary = {
            "ready": rr["ready"],
            "finalized_exists": rr["finalized_marker_exists"],
            "missing_count": len(rr["missing_items"]),
        }
        fin = sr.get("finalization")
        elim_finalization = fin if isinstance(fin, dict) else None
    workspace_summary = None
    has_workspace_blueprint = False
    if svc is not None:
        wm = build_workspace_view_model(svc, doc, settings)
        has_workspace_blueprint = bool(wm.get("has_blueprint"))
        if has_workspace_blueprint:
            workspace_summary = wm.get("workspace_summary")
    lc_status = effective_lifecycle_status(doc)
    lc_at = lifecycle_closed_at(doc)
    lifecycle_execution_enabled = lc_status == "active"
    return render_template(
        "services/operation_view.html",
        operation=doc,
        service_name=service_name,
        has_guided_inputs=has_guided_inputs,
        is_elim=is_elim,
        is_ls=is_ls,
        has_elim_inputs=has_elim_inputs,
        db_apply_steps=db_apply_steps if has_db_apply else None,
        has_db_apply=has_db_apply,
        rollback_stack_entries=rb,
        has_rollback_stack=has_rollback_stack,
        rollback_executed=rollback_executed,
        rollback_results=rollback_results,
        show_rollback_button=show_rollback_button,
        elim_config_enabled=elim_config_enabled,
        generation_cluster=gen_cluster,
        can_set_generation_context=can_set_generation_context,
        elim_config_rows=elim_config_rows,
        elim_readiness_summary=elim_readiness_summary,
        elim_finalization=elim_finalization,
        has_workspace_blueprint=has_workspace_blueprint,
        workspace_summary=workspace_summary,
        lifecycle_status=lc_status,
        lifecycle_closed_at=lc_at,
        lifecycle_execution_enabled=lifecycle_execution_enabled,
        can_delete_closed_record=(
            is_superadmin()
            and lc_status in ("completed", "rolled_back")
        ),
    )


@services_bp.route("/operations/<operation_id>/delete", methods=["POST"])
def operation_delete(operation_id: str):
    """Remove operation JSON only; superadmin + closed lifecycle only."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_superadmin():
        flash("Only superadmin may delete a closed operation record.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    st = effective_lifecycle_status(doc)
    if st == "active":
        flash("Cannot delete an active request record.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if st not in ("completed", "rolled_back"):
        flash(
            "Delete is only allowed for completed or rolled-back requests.",
            "error",
        )
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if delete_operation(settings, operation_id):
        flash("Closed operation record removed from disk (JSON file only).", "ok")
        return redirect(url_for("services.operations_list"))
    flash("Operation file could not be deleted.", "error")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/workspace")
def operation_workspace(operation_id: str):
    """Blueprint-driven request workspace overview (config strip + table deps)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    service_name = (svc or {}).get("name") or doc.get("service_id")
    guided = iter_guided_fields(svc or {})
    has_guided_inputs = len(guided) > 0
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    is_elim = is_elim_operation(doc)
    is_ls = is_ls_operation(doc)
    has_elim_inputs = _has_license_resource_inputs(doc)
    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    db_apply_steps = sr.get("db_apply")
    has_db_apply = isinstance(db_apply_steps, list) and len(db_apply_steps) > 0
    workspace = build_workspace_view_model(svc, doc, settings)
    return render_template(
        "services/workspace.html",
        operation=doc,
        service_name=service_name,
        service_def=svc,
        workspace=workspace,
        has_guided_inputs=has_guided_inputs,
        has_elim_inputs=has_elim_inputs,
        is_elim=is_elim,
        is_ls=is_ls,
        has_db_apply=has_db_apply,
        lifecycle_execution_enabled=effective_lifecycle_status(doc) == "active",
    )


@services_bp.route("/operations/<operation_id>/workspace/config/<config_id>")
def operation_config_workspace(operation_id: str, config_id: str):
    """Single config file focus: deps, actions, and links to existing ELIM routes."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    service_name = (svc or {}).get("name") or doc.get("service_id")
    guided = iter_guided_fields(svc or {})
    has_guided_inputs = len(guided) > 0
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    is_elim = is_elim_operation(doc)
    is_ls = is_ls_operation(doc)
    has_elim_inputs = _has_license_resource_inputs(doc)
    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    db_apply_steps = sr.get("db_apply")
    has_db_apply = isinstance(db_apply_steps, list) and len(db_apply_steps) > 0

    cw = build_config_workspace_view_model(svc, doc, settings, config_id)
    if cw is None:
        flash("Unknown config for this service blueprint.", "error")
        return redirect(url_for("services.operation_workspace", operation_id=operation_id))

    return render_template(
        "services/config_workspace.html",
        operation=doc,
        service_name=service_name,
        service_def=svc,
        cw=cw,
        has_guided_inputs=has_guided_inputs,
        has_elim_inputs=has_elim_inputs,
        is_elim=is_elim,
        is_ls=is_ls,
        has_db_apply=has_db_apply,
        lifecycle_execution_enabled=effective_lifecycle_status(doc) == "active",
    )


@services_bp.route(
    "/operations/<operation_id>/workspace/config/<config_id>/table/<table_id>"
)
def operation_table_workspace(operation_id: str, config_id: str, table_id: str):
    """Single manual table focus: fields, reuse context, next-step hints."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    service_name = (svc or {}).get("name") or doc.get("service_id")
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    is_elim = is_elim_operation(doc)
    is_ls = is_ls_operation(doc)
    has_elim_inputs = _has_license_resource_inputs(doc)

    tw = build_table_workspace_view_model(
        svc,
        doc,
        settings,
        config_id,
        table_id,
        db_adapter=_optional_db_adapter(settings),
    )
    if tw is None:
        flash("Unknown table for this config or blueprint.", "error")
        return redirect(
            url_for(
                "services.operation_config_workspace",
                operation_id=operation_id,
                config_id=config_id,
            )
        )

    return render_template(
        "services/table_workspace.html",
        operation=doc,
        service_name=service_name,
        service_def=svc,
        tw=tw,
        config_id=config_id,
        has_elim_inputs=has_elim_inputs,
    )


@services_bp.route(
    "/operations/<operation_id>/workspace/config/<config_id>/table/<table_id>/db-preview"
)
def operation_table_db_preview(
    operation_id: str, config_id: str, table_id: str
):
    """ELIM: filtered DB preview for one blueprint table (license_servers or resources_REF)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Table DB preview is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    tw = build_table_workspace_view_model(
        svc,
        doc,
        settings,
        config_id,
        table_id,
        db_adapter=_optional_db_adapter(settings),
    )
    if tw is None:
        flash("Unknown table for this config or blueprint.", "error")
        return redirect(
            url_for(
                "services.operation_config_workspace",
                operation_id=operation_id,
                config_id=config_id,
            )
        )
    if table_id not in (TABLE_LICENSE_SERVERS, TABLE_RESOURCES_REF):
        flash("Table-scoped DB preview is only defined for ELIM database tables.", "error")
        return redirect(
            url_for(
                "services.operation_table_workspace",
                operation_id=operation_id,
                config_id=config_id,
                table_id=table_id,
            )
        )

    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    preview_ls, preview_rr, validation_errors = build_elim_preview(si)
    ls_id = elim_inserted_id_for_step(doc, "insert_license_server")
    dependency_blocked = table_id == TABLE_RESOURCES_REF and ls_id is None
    if table_id == TABLE_RESOURCES_REF and ls_id is not None:
        preview_rr = build_resource_ref_row(si, ls_id, [], preview=False)

    if table_id == TABLE_LICENSE_SERVERS:
        this_applied = elim_db_step_done(doc, "insert_license_server")
    else:
        this_applied = elim_db_step_done(doc, "insert_resource")

    table_back_url = url_for(
        "services.operation_table_workspace",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    db_apply_action = url_for(
        "services.operation_table_db_apply",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    lc_active = effective_lifecycle_status(doc) == "active"
    can_apply = (
        not validation_errors
        and not this_applied
        and not dependency_blocked
        and lc_active
    )
    db_confirm_url = (
        url_for(
            "services.operation_table_db_apply_confirm",
            operation_id=operation_id,
            config_id=config_id,
            table_id=table_id,
        )
        if can_apply
        else ""
    )

    return render_template(
        "services/db_preview.html",
        operation=doc,
        preview_license_servers=preview_ls,
        preview_resources_ref=preview_rr,
        validation_errors=validation_errors,
        already_applied=this_applied,
        can_apply=can_apply,
        db_confirm_url=db_confirm_url,
        table_scope=table_id,
        table_back_url=table_back_url,
        db_apply_action=db_apply_action,
        dependency_blocked=dependency_blocked,
        license_server_id_for_preview=ls_id,
        lifecycle_execution_enabled=lc_active,
    )


@services_bp.route(
    "/operations/<operation_id>/workspace/config/<config_id>/table/<table_id>/db-apply-confirm"
)
def operation_table_db_apply_confirm(
    operation_id: str, config_id: str, table_id: str
):
    """Second step before table-scoped DB apply."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Table DB apply is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    tw = build_table_workspace_view_model(
        svc,
        doc,
        settings,
        config_id,
        table_id,
        db_adapter=_optional_db_adapter(settings),
    )
    if tw is None or table_id not in (TABLE_LICENSE_SERVERS, TABLE_RESOURCES_REF):
        flash("Invalid table for confirmation.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))

    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    preview_ls, preview_rr, validation_errors = build_elim_preview(si)
    ls_id = elim_inserted_id_for_step(doc, "insert_license_server")
    dependency_blocked = table_id == TABLE_RESOURCES_REF and ls_id is None
    if table_id == TABLE_RESOURCES_REF and ls_id is not None:
        preview_rr = build_resource_ref_row(si, ls_id, [], preview=False)

    if table_id == TABLE_LICENSE_SERVERS:
        this_applied = elim_db_step_done(doc, "insert_license_server")
    else:
        this_applied = elim_db_step_done(doc, "insert_resource")

    table_back_url = url_for(
        "services.operation_table_workspace",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    prev_url = url_for(
        "services.operation_table_db_preview",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    db_apply_action = url_for(
        "services.operation_table_db_apply",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    lc_active = effective_lifecycle_status(doc) == "active"
    can_apply = (
        not validation_errors
        and not this_applied
        and not dependency_blocked
        and lc_active
    )
    if not can_apply:
        flash(
            "Apply is not available. Return to the table preview page.",
            "error",
        )
        return redirect(prev_url)

    return render_template(
        "services/db_apply_confirm.html",
        operation=doc,
        preview_license_servers=preview_ls,
        preview_resources_ref=preview_rr,
        validation_errors=validation_errors,
        partial_apply_note=None,
        table_scope=table_id,
        table_back_url=table_back_url,
        db_apply_action=db_apply_action,
        preview_back_url=prev_url,
        dependency_blocked=dependency_blocked,
        license_server_id_for_preview=ls_id,
        lifecycle_execution_enabled=lc_active,
    )


@services_bp.route(
    "/operations/<operation_id>/workspace/config/<config_id>/table/<table_id>/db-apply",
    methods=["POST"],
)
def operation_table_db_apply(operation_id: str, config_id: str, table_id: str):
    """ELIM: apply insert for a single table after table-scoped preview."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Table DB apply is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    tw = build_table_workspace_view_model(
        svc,
        doc,
        settings,
        config_id,
        table_id,
        db_adapter=_optional_db_adapter(settings),
    )
    prev_url = url_for(
        "services.operation_table_db_preview",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    back_table = url_for(
        "services.operation_table_workspace",
        operation_id=operation_id,
        config_id=config_id,
        table_id=table_id,
    )
    if tw is None:
        flash("Unknown table for this config or blueprint.", "error")
        return redirect(
            url_for(
                "services.operation_config_workspace",
                operation_id=operation_id,
                config_id=config_id,
            )
        )
    if table_id not in (TABLE_LICENSE_SERVERS, TABLE_RESOURCES_REF):
        flash("Table-scoped DB apply is only defined for ELIM database tables.", "error")
        return redirect(back_table)

    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    ve = validate_elim_service_inputs(si)
    if ve:
        flash("Cannot apply: " + "; ".join(ve[:5]), "error")
        return redirect(prev_url)

    try:
        db = _db_adapter(settings)
    except (NotImplementedError, RuntimeError) as e:
        flash(str(e), "error")
        return redirect(prev_url)

    raw_sr = doc.get("step_results")
    sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
    prior = get_db_apply_steps(doc)

    if table_id == TABLE_LICENSE_SERVERS:
        if elim_db_step_done(doc, "insert_license_server"):
            flash("license_servers was already applied for this operation.", "error")
            return redirect(prev_url)
        ok, ls_id, apply_errors = apply_elim_license_server_only(db, si)
        if not ok or ls_id is None:
            msg = apply_errors[0] if apply_errors else "Apply failed"
            flash(msg, "error")
            return redirect(prev_url)
        sr["db_apply"] = merge_db_apply_steps(
            prior, build_db_apply_step_license_server(ls_id)
        )
        update_operation(
            settings,
            operation_id,
            {
                "step_results": sr,
                "rollback_stack": build_rollback_stack_license_only(ls_id),
            },
        )
        flash("license_servers insert applied for this request.", "ok")
        return redirect(back_table)

    if elim_db_step_done(doc, "insert_resource"):
        flash("resources_REF was already applied for this operation.", "error")
        return redirect(prev_url)
    ls_id = elim_inserted_id_for_step(doc, "insert_license_server")
    if ls_id is None:
        flash(
            "Requires license_servers to be applied first.",
            "error",
        )
        return redirect(back_table)
    ok, rr_id, apply_errors = apply_elim_resource_only(db, si, ls_id)
    if not ok or rr_id is None:
        msg = apply_errors[0] if apply_errors else "Apply failed"
        flash(msg, "error")
        return redirect(prev_url)
    sr["db_apply"] = merge_db_apply_steps(prior, build_db_apply_step_resource(rr_id))
    update_operation(
        settings,
        operation_id,
        {
            "step_results": sr,
            "rollback_stack": build_rollback_stack(ls_id, rr_id),
        },
    )
    flash("resources_REF insert applied; rollback stack updated.", "ok")
    return redirect(back_table)


@services_bp.route("/operations/<operation_id>/inputs", methods=["GET", "POST"])
def operation_inputs(operation_id: str):
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))

    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    if not svc:
        flash("Service definition not found.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))

    guided = iter_guided_fields(svc)
    fields_ls, fields_rr = _split_guided_groups(guided)
    service_inputs = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}

    if not guided:
        return render_template(
            "services/operation_inputs.html",
            operation=doc,
            service=svc,
            fields_license_server=[],
            fields_resource_ref=[],
            service_inputs=service_inputs,
            no_schema=True,
        )

    if request.method == "POST":
        gl = _guard_lifecycle_mutations(doc, operation_id)
        if gl:
            return gl
        allowed = {f["key"] for f in guided}
        merged = dict(service_inputs)
        for key in allowed:
            if key in request.form:
                merged[key] = (request.form.get(key) or "").strip()
        update_operation(settings, operation_id, {"service_inputs": merged})
        rc = (request.form.get("return_config_id") or "").strip()
        rt = (request.form.get("return_table_id") or "").strip()
        if rc and rt:
            flash(
                "Insert operation saved fields to this request. Use table DB preview when ready.",
                "ok",
            )
            return redirect(
                url_for(
                    "services.operation_table_workspace",
                    operation_id=operation_id,
                    config_id=rc,
                    table_id=rt,
                )
            )
        flash("Service inputs saved.", "ok")
        return redirect(url_for("services.operation_view", operation_id=operation_id))

    return render_template(
        "services/operation_inputs.html",
        operation=doc,
        service=svc,
        fields_license_server=fields_ls,
        fields_resource_ref=fields_rr,
        service_inputs=service_inputs,
        no_schema=False,
    )


@services_bp.route("/operations/<operation_id>/db-preview")
def operation_db_preview(operation_id: str):
    """ELIM/LS: show planned JSON DB inserts from ``service_inputs``."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("DB preview is only for ELIM/LS add-license-resource services.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    preview_ls, preview_rr, validation_errors = build_elim_preview(si)
    ls_id = elim_inserted_id_for_step(doc, "insert_license_server")
    rr_done = elim_db_step_done(doc, "insert_resource")
    if ls_id is not None and not rr_done:
        preview_rr = build_resource_ref_row(si, ls_id, [], preview=False)
    fully = elim_db_fully_applied(doc)
    partial_note = None
    if elim_db_already_applied(doc) and not fully:
        partial_note = (
            "Part of this request is already applied. "
            "Complete the remaining insert below when validation passes."
        )
    lc_active = effective_lifecycle_status(doc) == "active"
    can_apply = not validation_errors and not fully and lc_active
    db_confirm_url = (
        url_for("services.operation_db_apply_confirm", operation_id=operation_id)
        if can_apply
        else ""
    )
    return render_template(
        "services/db_preview.html",
        operation=doc,
        preview_license_servers=preview_ls,
        preview_resources_ref=preview_rr,
        validation_errors=validation_errors,
        already_applied=fully,
        partial_apply_note=partial_note,
        can_apply=can_apply,
        db_confirm_url=db_confirm_url,
        lifecycle_execution_enabled=lc_active,
    )


@services_bp.route("/operations/<operation_id>/db-apply-confirm")
def operation_db_apply_confirm(operation_id: str):
    """Second step before full ELIM DB apply: explicit confirm after preview."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("DB apply confirmation is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    preview_ls, preview_rr, validation_errors = build_elim_preview(si)
    ls_id = elim_inserted_id_for_step(doc, "insert_license_server")
    rr_done = elim_db_step_done(doc, "insert_resource")
    if ls_id is not None and not rr_done:
        preview_rr = build_resource_ref_row(si, ls_id, [], preview=False)
    fully = elim_db_fully_applied(doc)
    partial_note = None
    if elim_db_already_applied(doc) and not fully:
        partial_note = (
            "Part of this request is already applied. "
            "Complete the remaining insert below when validation passes."
        )
    lc_active = effective_lifecycle_status(doc) == "active"
    can_apply = not validation_errors and not fully and lc_active
    if not can_apply:
        flash(
            "Apply is not available (validation, lifecycle, or already applied). "
            "Return to the preview page.",
            "error",
        )
        return redirect(
            url_for("services.operation_db_preview", operation_id=operation_id)
        )
    back_url = url_for("services.operation_db_preview", operation_id=operation_id)
    apply_url = url_for("services.operation_db_apply", operation_id=operation_id)
    return render_template(
        "services/db_apply_confirm.html",
        operation=doc,
        preview_license_servers=preview_ls,
        preview_resources_ref=preview_rr,
        validation_errors=validation_errors,
        partial_apply_note=partial_note,
        table_scope="",
        table_back_url=back_url,
        preview_back_url=back_url,
        db_apply_action=apply_url,
        lifecycle_execution_enabled=lc_active,
    )


@services_bp.route("/operations/<operation_id>/db-apply", methods=["POST"])
def operation_db_apply(operation_id: str):
    """ELIM/LS: insert license_servers then resources_REF; record steps and rollback stack."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("DB apply is only for ELIM/LS license-resource services.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    if elim_db_fully_applied(doc):
        flash("Database changes were already fully applied for this operation.", "error")
        return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    ve = validate_elim_service_inputs(si)
    if ve:
        flash("Cannot apply: " + "; ".join(ve[:5]), "error")
        return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
    try:
        db = _db_adapter(settings)
    except (NotImplementedError, RuntimeError) as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
    raw_sr = doc.get("step_results")
    sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
    ls_done = elim_db_step_done(doc, "insert_license_server")
    rr_done = elim_db_step_done(doc, "insert_resource")
    if rr_done:
        flash("Resource row was already applied for this operation.", "error")
        return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
    if ls_done:
        ls_id = elim_inserted_id_for_step(doc, "insert_license_server")
        if ls_id is None:
            flash("Missing license server insert id; cannot apply resources_REF.", "error")
            return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
        ok, rr_id, apply_errors = apply_elim_resource_only(db, si, ls_id)
        if not ok or rr_id is None:
            msg = apply_errors[0] if apply_errors else "Apply failed"
            flash(msg, "error")
            return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
        prior = get_db_apply_steps(doc)
        sr["db_apply"] = merge_db_apply_steps(
            prior, build_db_apply_step_resource(rr_id)
        )
        update_operation(
            settings,
            operation_id,
            {
                "step_results": sr,
                "rollback_stack": build_rollback_stack(ls_id, rr_id),
            },
        )
        flash("resources_REF insert applied; rollback stack updated.", "ok")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    ok, ls_id, rr_id, apply_errors = apply_elim_inserts(db, si)
    if not ok or ls_id is None or rr_id is None:
        msg = apply_errors[0] if apply_errors else "Apply failed"
        flash(msg, "error")
        return redirect(url_for("services.operation_db_preview", operation_id=operation_id))
    sr["db_apply"] = build_db_apply_step_results(ls_id, rr_id)
    update_operation(
        settings,
        operation_id,
        {
            "step_results": sr,
            "rollback_stack": build_rollback_stack(ls_id, rr_id),
        },
    )
    flash("Database inserts applied; rollback stack recorded.", "ok")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/rollback-preview")
def operation_rollback_preview(operation_id: str):
    """ELIM only: show rows targeted by ``rollback_stack`` before delete."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Rollback is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    if elim_rollback_executed(doc):
        flash("Rollback was already executed for this operation.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not elim_db_already_applied(doc):
        flash("Apply database inserts first; nothing to roll back yet.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    rb = doc.get("rollback_stack")
    if not isinstance(rb, list) or len(rb) == 0:
        flash("No rollback stack is recorded for this operation.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    try:
        db = _db_adapter(settings)
    except (NotImplementedError, RuntimeError) as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    preview_items = build_rollback_preview_items(db, rb)
    confirm_url = url_for(
        "services.operation_rollback_confirm", operation_id=operation_id
    )
    return render_template(
        "services/rollback_preview.html",
        operation=doc,
        preview_items=preview_items,
        rollback_confirm_url=confirm_url,
    )


@services_bp.route("/operations/<operation_id>/rollback-confirm")
def operation_rollback_confirm(operation_id: str):
    """Second step before rollback deletes: explicit confirm."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Rollback is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    if elim_rollback_executed(doc):
        flash("Rollback was already executed for this operation.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not elim_db_already_applied(doc):
        flash("Apply database inserts first; nothing to roll back yet.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    rb = doc.get("rollback_stack")
    if not isinstance(rb, list) or len(rb) == 0:
        flash("No rollback stack is recorded for this operation.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    try:
        db = _db_adapter(settings)
    except (NotImplementedError, RuntimeError) as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    preview_items = build_rollback_preview_items(db, rb)
    if not preview_items:
        flash("No valid rollback steps to confirm.", "error")
        return redirect(
            url_for("services.operation_rollback_preview", operation_id=operation_id)
        )
    return render_template(
        "services/rollback_confirm.html",
        operation=doc,
        preview_items=preview_items,
        preview_back_url=url_for(
            "services.operation_rollback_preview", operation_id=operation_id
        ),
    )


@services_bp.route("/operations/<operation_id>/rollback-apply", methods=["POST"])
def operation_rollback_apply(operation_id: str):
    """ELIM/LS: delete rows per ``rollback_stack``; record results (stack kept for audit)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Rollback is only for ELIM/LS license-resource services.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    if elim_rollback_executed(doc):
        flash("Rollback was already executed; cannot run again.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not elim_can_rollback(doc):
        flash("Rollback is not available (apply inserts first, or rollback stack is empty).", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    rb = doc.get("rollback_stack")
    if not isinstance(rb, list):
        flash("Invalid rollback stack.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    try:
        db = _db_adapter(settings)
    except (NotImplementedError, RuntimeError) as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_rollback_preview", operation_id=operation_id))
    results = execute_elim_rollback(db, rb)
    raw_sr = doc.get("step_results")
    sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
    sr["rollback"] = results
    update_operation(
        settings,
        operation_id,
        {
            "step_results": sr,
            "rollback_executed": True,
        },
    )
    flash("Rollback finished; results stored under step_results.rollback.", "ok")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/generation-context", methods=["GET", "POST"])
def operation_generation_context(operation_id: str):
    """ELIM: set ``generation_context_cluster`` (reference cluster for mock generation, not deployment)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Generation context is used for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not elim_db_already_applied(doc):
        flash("Apply database inserts first, then set generation context.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    current = normalize_generation_cluster(doc.get("generation_context_cluster"))
    if request.method == "POST":
        gl = _guard_lifecycle_mutations(doc, operation_id)
        if gl:
            return gl
        raw = (request.form.get("generation_context_cluster") or "").strip()
        norm = normalize_generation_cluster(raw) if raw else None
        if raw and norm is None:
            flash("Invalid cluster value.", "error")
            return redirect(
                url_for("services.operation_generation_context", operation_id=operation_id)
            )
        update_operation(
            settings,
            operation_id,
            {"generation_context_cluster": norm},
        )
        flash("Generation context cluster saved.", "ok")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    return render_template(
        "services/generation_context.html",
        operation=doc,
        current_cluster=current or "",
    )


@services_bp.route(
    "/operations/<operation_id>/generate/<config_type>", methods=["POST"]
)
def operation_generate_config(operation_id: str, config_type: str):
    """Mock generate: blueprint config ids for ELIM or LS."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    if not _config_type_allowed_for_operation(config_type, svc):
        flash("Unsupported config type for this service.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not is_license_resource_db_operation(doc) or not elim_db_already_applied(doc):
        flash("Generate is only available after database apply.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    gen_cluster = normalize_generation_cluster(doc.get("generation_context_cluster"))
    if not gen_cluster:
        flash("Set generation context cluster first.", "error")
        return redirect(
            url_for("services.operation_generation_context", operation_id=operation_id)
        )
    try:
        scripts = _scripts_adapter(settings)
    except RuntimeError as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    si = doc.get("service_inputs") if isinstance(doc.get("service_inputs"), dict) else {}
    ctx: Dict[str, Any] = {
        "service_inputs": si,
        "request_number": str(doc.get("request_number") or ""),
        "generation_cluster": gen_cluster,
        "service_id": str(doc.get("service_id") or ""),
    }
    try:
        out_path = scripts.generate_config(config_type, gen_cluster, ctx)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    raw_sr = doc.get("step_results")
    sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
    sr_merged = merge_generation_step_result(
        sr,
        config_type=config_type,
        cluster=gen_cluster,
        generated_path=out_path,
        status="done",
    )
    update_operation(settings, operation_id, {"step_results": sr_merged})
    flash(f"Generated {config_type} (mock).", "ok")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/config-diff/<config_type>")
def operation_config_diff(operation_id: str, config_type: str):
    """Unified diff: current vs generated file for the generation context cluster."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    if not _config_type_allowed_for_operation(config_type, svc):
        flash("Unsupported config type for this service.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not is_license_resource_db_operation(doc):
        flash("Config diff is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gen_cluster = normalize_generation_cluster(doc.get("generation_context_cluster"))
    if not gen_cluster:
        flash("Set generation context cluster first.", "error")
        return redirect(
            url_for("services.operation_generation_context", operation_id=operation_id)
        )
    try:
        cur_p = current_config_path(settings, gen_cluster, config_type)
        gen_p = generated_config_path(settings, gen_cluster, config_type)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    left, _ = read_text_file(cur_p)
    right, gen_ok = read_text_file(gen_p)
    if not gen_ok:
        flash("Generated file not found; run Generate first.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    left_label = f"current ({gen_cluster})"
    right_label = f"generated ({gen_cluster})"
    diff_text = unified_diff_text(left, right, left_label, right_label)
    return render_template(
        "services/config_diff.html",
        operation=doc,
        config_type=config_type,
        cluster=gen_cluster,
        current_path=cur_p,
        generated_path=gen_p,
        current_missing=not os.path.isfile(cur_p),
        diff_text=diff_text,
    )


@services_bp.route(
    "/operations/<operation_id>/publish/<config_type>", methods=["POST"]
)
def operation_publish_config(operation_id: str, config_type: str):
    """Copy generated file into mock request directory."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    if not _config_type_allowed_for_operation(config_type, svc):
        flash("Unsupported config type for this service.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not is_license_resource_db_operation(doc):
        flash("Publish is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    gl = _guard_lifecycle_mutations(doc, operation_id)
    if gl:
        return gl
    gen_cluster = normalize_generation_cluster(doc.get("generation_context_cluster"))
    if not gen_cluster:
        flash("Set generation context cluster first.", "error")
        return redirect(
            url_for("services.operation_generation_context", operation_id=operation_id)
        )
    try:
        gen_p = generated_config_path(settings, gen_cluster, config_type)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if not os.path.isfile(gen_p):
        flash("Generated file missing; run Generate first.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    req_no = str(doc.get("request_number") or "").strip()
    if not req_no:
        flash("Operation has no request number.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    try:
        scripts = _scripts_adapter(settings)
        dest = scripts.publish_to_request(config_type, gen_cluster, req_no, gen_p)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    raw_sr = doc.get("step_results")
    sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
    sr_merged = merge_publish_step_result(
        sr,
        config_type=config_type,
        destination_path=dest,
        status="done",
    )
    update_operation(settings, operation_id, {"step_results": sr_merged})
    flash(f"Published {config_type} into request directory (mock).", "ok")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/readiness")
def operation_readiness(operation_id: str):
    """ELIM: show request finalization readiness (mock)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Readiness is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    readiness = _readiness_for_license_resource_service(settings, doc, svc)
    return render_template(
        "services/readiness.html",
        operation=doc,
        readiness=readiness,
        lifecycle_execution_enabled=effective_lifecycle_status(doc) == "active",
    )


@services_bp.route("/operations/<operation_id>/finalize", methods=["POST"])
def operation_lifecycle_finalize(operation_id: str):
    """Explicit lifecycle close: finalize (ELIM runs marker + readiness; others lifecycle-only)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if effective_lifecycle_status(doc) != "active":
        flash("This request is already closed; finalize is not available.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    if is_license_resource_db_operation(doc):
        sr_merged = _elim_run_finalize_marker(settings, operation_id, doc, svc)
        if sr_merged is None:
            return redirect(url_for("services.operation_view", operation_id=operation_id))
        lc = merge_lifecycle_status(doc, "completed")
        update_operation(
            settings,
            operation_id,
            {"step_results": sr_merged, "lifecycle": lc},
        )
    else:
        lc = merge_lifecycle_status(doc, "completed")
        update_operation(settings, operation_id, {"lifecycle": lc})
    flash("Request finalized successfully. Workspace is now closed.", "ok")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/rollback-close", methods=["POST"])
def operation_lifecycle_rollback_close(operation_id: str):
    """Rollback DB (if needed) then set lifecycle to rolled_back."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if effective_lifecycle_status(doc) != "active":
        flash("This request is already closed; rollback-close is not available.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))

    extra: Dict[str, Any] = {}
    if is_license_resource_db_operation(doc):
        if elim_rollback_executed(doc):
            pass
        elif elim_can_rollback(doc):
            rb = doc.get("rollback_stack")
            if not isinstance(rb, list):
                flash("Invalid rollback stack.", "error")
                return redirect(url_for("services.operation_view", operation_id=operation_id))
            try:
                db = _db_adapter(settings)
            except (NotImplementedError, RuntimeError) as e:
                flash(str(e), "error")
                return redirect(
                    url_for("services.operation_rollback_preview", operation_id=operation_id)
                )
            results = execute_elim_rollback(db, rb)
            raw_sr = doc.get("step_results")
            sr = dict(raw_sr) if isinstance(raw_sr, dict) else {}
            sr["rollback"] = results
            extra = {"step_results": sr, "rollback_executed": True}
        else:
            flash(
                "Cannot roll back and close: apply database changes first so there is something "
                "to roll back, or use Finalize if the changes should be kept.",
                "error",
            )
            return redirect(url_for("services.operation_view", operation_id=operation_id))

    virtual = {**doc, **extra}
    lc = merge_lifecycle_status(virtual, "rolled_back")
    update_operation(settings, operation_id, {**extra, "lifecycle": lc})
    flash("Request rolled back and closed successfully.", "ok")
    return redirect(url_for("services.operation_view", operation_id=operation_id))


@services_bp.route(
    "/operations/<operation_id>/finalize-request", methods=["POST"]
)
def operation_finalize_request(operation_id: str):
    """ELIM: legacy entry point; same as lifecycle finalize for ELIM (redirects to readiness)."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        flash("Operation not found.", "error")
        return redirect(url_for("services.index"))
    if not is_license_resource_db_operation(doc):
        flash("Finalization is only for ELIM/LS license-resource requests.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))
    if effective_lifecycle_status(doc) != "active":
        flash("This request is already closed; finalization is not available.", "error")
        return redirect(url_for("services.operation_view", operation_id=operation_id))

    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    sr_merged = _elim_run_finalize_marker(settings, operation_id, doc, svc)
    if sr_merged is None:
        return redirect(url_for("services.operation_readiness", operation_id=operation_id))

    lc = merge_lifecycle_status(doc, "completed")
    update_operation(
        settings,
        operation_id,
        {"step_results": sr_merged, "lifecycle": lc},
    )

    fin = sr_merged.get("finalization") if isinstance(sr_merged.get("finalization"), dict) else {}
    if str(fin.get("status") or "") == "already_present":
        flash(
            ".finalized marker was already present; operation state updated.",
            "ok",
        )
    else:
        flash("Created .finalized marker and recorded finalization.", "ok")
    return redirect(url_for("services.operation_readiness", operation_id=operation_id))


@services_bp.route("/operations/<operation_id>/help/<field_key>")
def operation_field_help(operation_id: str, field_key: str):
    """JSON help for one guided field; table/column come only from service metadata."""
    settings = _load_settings()
    doc = load_operation(settings, operation_id)
    if not doc:
        return jsonify({"error": "operation not found"}), 404

    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(str(doc.get("service_id", "")), root)
    except ServiceRegistryError:
        svc = None
    if not svc:
        return jsonify({"error": "service not found"}), 404

    fdef = find_guided_field(svc, field_key)
    if not fdef:
        return jsonify({"error": "unknown field"}), 404

    try:
        db = _db_adapter(settings)
    except (NotImplementedError, RuntimeError) as e:
        return jsonify({"error": str(e)}), 503

    payload, err = build_help_payload(db, fdef)
    if err:
        return jsonify({"error": err}), 400
    return jsonify(payload)


@services_bp.route("/<service_id>/start", methods=["GET", "POST"])
def start(service_id: str):
    settings = _load_settings()
    root = settings.get("service_defs_root", "services/defs")
    try:
        svc = get_definition_by_id(service_id, root)
    except ServiceRegistryError as e:
        flash(str(e), "error")
        return redirect(url_for("services.index"))
    if not svc:
        flash("Unknown service.", "error")
        return redirect(url_for("services.index"))

    if request.method == "GET":
        search_rn = (request.args.get("request_number") or "").strip()
        existing_docs: List[Dict[str, Any]] = []
        if search_rn:
            existing_docs = find_operations(
                settings, service_id=service_id, request_number=search_rn
            )
        existing_rows = [_operation_display_row(settings, d) for d in existing_docs]
        active_docs = find_all_active_workspaces(settings)
        active_rows = [_operation_display_row(settings, d) for d in active_docs]
        return render_template(
            "services/service_start.html",
            service=svc,
            search_request_number=search_rn,
            existing_operations=existing_rows,
            active_workspaces=active_rows,
            current_role=get_current_role(),
            is_superadmin_session=is_superadmin(),
            start_blocked=False,
        )

    req_no = (request.form.get("request_number") or "").strip()
    if not req_no:
        flash("Request number is required.", "error")
        return redirect(url_for("services.start", service_id=service_id))

    active = find_all_active_workspaces(settings)
    if active and not is_superadmin():
        _flash_global_start_blocked(active)
        all_matching = find_operations(
            settings, service_id=service_id, request_number=req_no
        )
        return render_template(
            "services/service_start.html",
            service=svc,
            search_request_number=req_no,
            existing_operations=[_operation_display_row(settings, d) for d in all_matching],
            active_workspaces=[_operation_display_row(settings, d) for d in active],
            current_role=get_current_role(),
            is_superadmin_session=is_superadmin(),
            start_blocked=True,
        )

    wc = {
        "role_at_creation": get_current_role(),
        "is_superadmin_override": bool(active and is_superadmin()),
        "override_reason": None,
    }
    try:
        doc, _meta = start_service_operation(
            settings, svc, request_number=req_no, workspace_control=wc
        )
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("services.start", service_id=service_id))

    oid = doc.get("operation_id")
    if not oid:
        flash("Operation could not be created.", "error")
        return redirect(url_for("services.start", service_id=service_id))

    if doc.get("status") == "blocked_request_missing":
        flash(
            "Request directory was not found; operation recorded as blocked.",
            "error",
        )
    elif active and is_superadmin():
        flash("Workspace created (superadmin override: global lock bypassed).", "ok")
    else:
        flash("Operation created.", "ok")

    return redirect(url_for("services.operation_view", operation_id=oid))
