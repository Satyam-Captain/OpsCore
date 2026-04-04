"""

GMCAssist: linear service wizards (Flask + Jinja). Additive to the legacy Services workspace.

"""



from typing import Any, Dict, List, Optional, Tuple



from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from services.gmcassist_cluster_resources import (
    apply_cluster_resources_batch,
    build_cluster_resources_rows,
    cluster_help_rows_from_db,
    parse_cluster_resources_form,
    pending_cluster_resources_from_run,
    validate_cluster_resources_prereq,
)



from core.storage import load_json

from services.adapters import build_adapters

from services.blueprint import build_table_default_values

from services.elim_db import (

    TABLE_CLUSTER_RESOURCES,

    TABLE_LICENSE_SERVERS,

    TABLE_RESOURCES_REF,

    apply_elim_license_server_only,

    apply_elim_resource_only,

    build_license_server_row,

    build_resource_ref_row,

    validate_license_server_inputs_only,

    validate_resource_ref_inputs_only,

)

from services.gmcassist_db_rollback import maybe_rollback_before_navigate_back

from services.gmcassist_sql_preview import format_insert_preview
from services.gmcassist_superadmin import (
    SESSION_KEY_SUPERADMIN,
    apply_superadmin_db_rollback_from_run,
    build_superadmin_reset_audit,
    build_superadmin_reset_preview,
    session_is_gmcassist_superadmin,
    verify_superadmin_password,
)

from services.input_help import MODE_UNIQUE, build_help_payload, find_guided_field

from services.gmcassist_wizard_steps import (

    apply_cluster_list_to_context,

    back_allowed,

    ensure_wizard_context,

    merge_get_template_extras,

    post_diff,

    post_finalize_check,

    post_generate,

    post_handoff,

    post_manual_note,

    post_publish,

)

from services.registry import ServiceRegistryError, get_definition_by_id

from services.wizard_loader import get_step, list_wizard_definitions, load_wizard_definition, step_title

from services.wizard_run_store import (
    abandon_wizard_run,
    build_step_advance_updates,
    create_wizard_run,
    find_any_blocking_wizard_run,
    find_blocking_run_for_normalized_request,
    is_wizard_run_blocking,
    list_blocking_wizard_runs,
    load_wizard_run,
    mark_wizard_run_superadmin_reset,
    normalize_wizard_request_number,
    raw_request_number_from_run,
    reconcile_wizard_run_for_blocking,
    update_wizard_run,
)



gmcassist_bp = Blueprint("gmcassist", __name__)


@gmcassist_bp.context_processor
def _inject_gmcassist_superadmin_flag() -> Dict[str, Any]:
    return {"gmcassist_superadmin_session": session_is_gmcassist_superadmin(session)}


def _require_gmcassist_superadmin_redirect():
    if not session_is_gmcassist_superadmin(session):
        flash("GMCAssist superadmin mode is required.", "error")
        return redirect(url_for("gmcassist.superadmin_elevate"))
    return None


def _load_settings() -> Dict[str, Any]:

    return load_json("config/settings.json")





@gmcassist_bp.before_request

def _require_service_enabled() -> None:

    if not _load_settings().get("service_enabled", True):

        abort(404)





def _service_defs_root(settings: Dict[str, Any]) -> str:

    return settings.get("service_defs_root", "services/defs")





def _guided_fields_for_group(service_def: Dict[str, Any], group: str) -> List[Dict[str, Any]]:

    out: List[Dict[str, Any]] = []

    guided = service_def.get("guided_inputs")

    if not isinstance(guided, list):

        return out

    g = (group or "").strip()

    for item in guided:

        if not isinstance(item, dict):

            continue

        if str(item.get("group") or "").strip() == g:

            out.append(item)

    return out





def _wizard_field_value_empty(val: Any) -> bool:

    """True if the wizard should treat ``val`` as unset for display prefill."""

    if val is None:

        return True

    if isinstance(val, str) and not str(val).strip():

        return True

    return False





def _table_id_for_form_group(service_def: Dict[str, Any], form_group: str) -> Optional[str]:

    fg = (form_group or "").strip()

    tables = service_def.get("tables")

    if not isinstance(tables, dict):

        return None

    for tid, tdef in tables.items():

        if not isinstance(tdef, dict):

            continue

        if str(tdef.get("guided_group") or "").strip() == fg:

            return str(tid)

    return None





def _merge_table_defaults_for_group(

    service_def: Dict[str, Any],

    form_group: str,

    service_inputs: Dict[str, Any],

    settings: Dict[str, Any],

) -> Dict[str, Any]:

    """

    Display-only prefill: keep user-entered values; then apply table metadata defaults

    and (when ``prefill_mode`` is ``sample_row``) the first DB row via ``build_table_default_values``.

    """

    out = dict(service_inputs) if isinstance(service_inputs, dict) else {}

    tid = _table_id_for_form_group(service_def, form_group)

    if not tid:

        return out

    try:

        db = build_adapters(settings)[0]

    except NotImplementedError:

        db = None

    merged = build_table_default_values(service_def, {}, db, tid)

    if not isinstance(merged, dict):

        return out

    for key, val in merged.items():

        if _wizard_field_value_empty(out.get(key)):

            out[key] = val

    return out





def _collect_service_inputs_from_form(form: Any) -> Dict[str, Any]:

    out: Dict[str, Any] = {}

    for k in form.keys():

        if not k.startswith("si_"):

            continue

        key = k[3:]

        out[key] = form.get(k) or ""

    return out





def _request_dir_gate(settings: Dict[str, Any], request_number: str) -> Tuple[bool, str]:

    """Return (ok, error_message). Mirrors ``services.engine`` request-dir rules."""

    _, request_adapter, _ = build_adapters(settings)

    rn = (request_number or "").strip()

    if not rn:

        return False, "Request number is required."

    exists = request_adapter.request_exists(rn)

    require_dir = bool(settings.get("service_require_request_dir", True))

    allow_bypass = bool(settings.get("service_allow_request_bypass", True))

    req_backend = str(settings.get("service_request_backend") or "local").strip().lower()

    if req_backend == "gmc" and require_dir and not allow_bypass and not exists:

        return (

            False,

            "Request directory was not found on GMC (.requests/LSF10CFG/REQ-<n>).",

        )

    if require_dir and not allow_bypass and not exists:

        return False, "Request directory is missing or not reachable."

    return True, ""





def _confirm_preview(

    settings: Dict[str, Any],

    run: Dict[str, Any],

    wizard_def: Dict[str, Any],

    step_def: Dict[str, Any],

) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], List[str]]:

    """

    For confirm steps: return (license_servers_row or None, resources_REF row or None, errors).

    """

    si = run.get("service_inputs")

    if not isinstance(si, dict):

        si = {}

    scope = str(step_def.get("confirm_scope") or "").strip()

    errors: List[str] = []

    if scope == "license_servers":

        errors = validate_license_server_inputs_only(si)

        ls_row = build_license_server_row(si, [])

        return ls_row, None, errors

    if scope in ("resources_REF", "resources_ref"):

        ctx = run.get("context")

        if not isinstance(ctx, dict):

            ctx = {}

        raw_ls = ctx.get("license_server_id")

        if raw_ls is None:

            return None, None, ["license_server_id is missing from wizard context."]

        try:

            ls_id = int(raw_ls)

        except (TypeError, ValueError):

            return None, None, ["license_server_id in context is invalid."]

        errors = validate_resource_ref_inputs_only(si, ls_id)

        rr_row = build_resource_ref_row(si, ls_id, [], preview=False)

        return None, rr_row, errors

    return None, None, ["Unknown confirm_scope for this wizard."]





def _db_insert_dispatch(

    settings: Dict[str, Any],

    run: Dict[str, Any],

    step_def: Dict[str, Any],

) -> Tuple[bool, str]:

    """Run db_insert for ELIM; update ``run`` in place. Returns (ok, message)."""

    target = str(step_def.get("insert_target") or "").strip()

    si = run.get("service_inputs")

    if not isinstance(si, dict):

        si = {}

    db, _, _ = build_adapters(settings)



    if target in ("license_servers", TABLE_LICENSE_SERVERS):

        ok, ls_id, errs = apply_elim_license_server_only(db, si)

        if not ok or ls_id is None:

            return False, "; ".join(errs) if errs else "Insert failed."

        ctx = run.setdefault("context", {})

        if not isinstance(ctx, dict):

            ctx = {}

            run["context"] = ctx

        ctx["license_server_id"] = int(ls_id)

        sid = str(step_def.get("id") or "license_servers_apply")

        sd = run.setdefault("step_data", {})

        if isinstance(sd, dict):

            sd[sid] = {

                "insert_target": TABLE_LICENSE_SERVERS,

                "inserted_id": int(ls_id),

            }

        return True, "Inserted license_servers row (ID %s)." % ls_id



    if target in ("resources_REF", "resources_ref", TABLE_RESOURCES_REF):

        ctx = run.get("context")

        if not isinstance(ctx, dict):

            ctx = {}

        raw_ls = ctx.get("license_server_id")

        if raw_ls is None:

            return False, "license_server_id missing; complete the license server step first."

        try:

            ls_id = int(raw_ls)

        except (TypeError, ValueError):

            return False, "Invalid license_server_id in context."

        ok, rr_id, errs = apply_elim_resource_only(db, si, ls_id)

        if not ok or rr_id is None:

            return False, "; ".join(errs) if errs else "Insert failed."

        ctx["resource_id"] = int(rr_id)

        sid = str(step_def.get("id") or "resources_ref_apply")

        sd = run.setdefault("step_data", {})

        if isinstance(sd, dict):

            sd[sid] = {

                "insert_target": TABLE_RESOURCES_REF,

                "inserted_id": int(rr_id),

            }

        return True, "Inserted resources_REF row (ID %s)." % rr_id



    return False, "Unknown insert_target for this wizard."





@gmcassist_bp.route("/superadmin/elevate", methods=["GET", "POST"])

def superadmin_elevate():

    settings = _load_settings()

    if not settings.get("service_enabled", True):

        abort(404)

    if request.method == "POST":

        pw = (request.form.get("password") or "").strip()

        if verify_superadmin_password(pw):

            session[SESSION_KEY_SUPERADMIN] = True

            flash("GMCAssist superadmin mode enabled for this browser session.", "ok")

            return redirect(url_for("gmcassist.catalog"))

        flash("Invalid password.", "error")

    return render_template("gmcassist/superadmin_elevate.html")





@gmcassist_bp.route("/superadmin/clear", methods=["POST"])

def superadmin_clear():

    settings = _load_settings()

    if not settings.get("service_enabled", True):

        abort(404)

    session.pop(SESSION_KEY_SUPERADMIN, None)

    flash("GMCAssist superadmin mode cleared.", "info")

    return redirect(url_for("gmcassist.catalog"))




@gmcassist_bp.route("/superadmin/reset-active", methods=["GET"])

def superadmin_reset_active():

    gl = _require_gmcassist_superadmin_redirect()

    if gl:

        return gl

    settings = _load_settings()

    blk = find_any_blocking_wizard_run(settings)

    if not blk:

        flash("There is no active blocking wizard run to reset.", "info")

        return redirect(url_for("gmcassist.catalog"))

    run_id, doc = blk

    try:

        db = build_adapters(settings)[0]

    except (NotImplementedError, RuntimeError) as e:

        flash(str(e), "error")

        return redirect(url_for("gmcassist.catalog"))

    preview = build_superadmin_reset_preview(db, doc)

    return render_template(

        "gmcassist/superadmin_reset_preview.html",

        run_id=run_id,

        run=doc,

        reset_preview=preview,

        cancel_url=url_for("gmcassist.catalog"),

    )





@gmcassist_bp.route("/superadmin/reset-active/confirm", methods=["POST"])

def superadmin_reset_active_confirm():

    gl = _require_gmcassist_superadmin_redirect()

    if gl:

        return gl

    settings = _load_settings()

    run_id = str(request.form.get("run_id") or "").strip()

    blk = find_any_blocking_wizard_run(settings)

    if not blk or blk[0] != run_id:

        flash("That run is not the current active blocking wizard run; nothing was changed.", "error")

        return redirect(url_for("gmcassist.catalog"))

    # Use reconciled doc from blocking scan (same snapshot as exclusivity check).
    doc = blk[1]

    if not is_wizard_run_blocking(doc):

        flash("That wizard run is no longer active; nothing was changed.", "error")

        return redirect(url_for("gmcassist.catalog"))

    try:

        db = build_adapters(settings)[0]

    except (NotImplementedError, RuntimeError) as e:

        flash(str(e), "error")

        return redirect(url_for("gmcassist.superadmin_reset_active"))

    ok, err_msg = apply_superadmin_db_rollback_from_run(db, doc)

    if not ok:

        flash("Database rollback failed: %s" % err_msg, "error")

        return redirect(url_for("gmcassist.superadmin_reset_active"))

    audit = build_superadmin_reset_audit(doc)

    updated = mark_wizard_run_superadmin_reset(settings, run_id, audit)

    if not updated:

        flash("Run file could not be updated after rollback; check DB and run JSON manually.", "error")

        return redirect(url_for("gmcassist.catalog"))

    flash(

        "Database rollback completed and the wizard run was marked abandoned (superadmin reset).",

        "ok",

    )

    return redirect(url_for("gmcassist.catalog"))





@gmcassist_bp.route("/")

def catalog():

    settings = _load_settings()

    wizards = list_wizard_definitions(settings)

    active_blocking_run = None

    blk = find_any_blocking_wizard_run(settings)

    if blk:

        bid, bdoc = blk

        raw_rn = raw_request_number_from_run(bdoc)

        label = raw_rn if raw_rn else "(request not confirmed yet)"

        active_blocking_run = {

            "run_id": bid,

            "request_label": label,

            "resume_url": url_for("gmcassist.wizard_run", run_id=bid),

        }

    return render_template(

        "gmcassist/catalog.html",

        wizards=wizards,

        active_blocking_run=active_blocking_run,

    )





@gmcassist_bp.route("/services/<service_id>/help/<field_key>")

def service_field_help(service_id: str, field_key: str):

    """JSON field help for GMCAssist wizard forms; same payload shape as legacy Services help."""

    settings = _load_settings()

    root = _service_defs_root(settings)

    try:

        svc = get_definition_by_id(str(service_id), root)

    except ServiceRegistryError:

        svc = None

    if not svc:

        return jsonify({"error": "service not found"}), 404

    fdef = find_guided_field(svc, str(field_key))

    if not fdef:

        return jsonify({"error": "unknown field"}), 404

    try:

        db = build_adapters(settings)[0]

    except (NotImplementedError, RuntimeError) as e:

        return jsonify({"error": str(e)}), 503

    payload, err = build_help_payload(db, fdef)

    if err:

        return jsonify({"error": err}), 400

    return jsonify(payload)





@gmcassist_bp.route("/help/cluster-resources/clusters")

def help_cluster_resources_clusters():

    """JSON rows for cluster_ID picker: cluster_ID + label (name + id)."""

    settings = _load_settings()

    try:

        db = build_adapters(settings)[0]

    except (NotImplementedError, RuntimeError) as e:

        return jsonify({"error": str(e), "rows": []}), 503

    rows = cluster_help_rows_from_db(db)

    return jsonify({"rows": rows})





# Same metadata shape as guided_inputs unique_values (cluster_resources.resource_value).
_CLUSTER_RESOURCES_RESOURCE_VALUE_HELP_FIELD = {
    "key": "resource_value",
    "help_mode": MODE_UNIQUE,
    "help_table": "cluster_resources",
    "help_column": "resource_value",
    "column": "resource_value",
}


@gmcassist_bp.route("/help/cluster-resources/resource-values")

def help_cluster_resources_values():

    """Distinct ``resource_value`` values from ``cluster_resources`` (shared input_help path)."""

    settings = _load_settings()

    try:

        db = build_adapters(settings)[0]

    except (NotImplementedError, RuntimeError) as e:

        return jsonify({"mode": MODE_UNIQUE, "values": [], "error": str(e)}), 503

    payload, err = build_help_payload(db, _CLUSTER_RESOURCES_RESOURCE_VALUE_HELP_FIELD)

    if err:

        return jsonify({"mode": MODE_UNIQUE, "values": [], "error": err}), 400

    return jsonify(payload)





@gmcassist_bp.route("/wizard/<wizard_id>/start", methods=["POST"])

def wizard_start(wizard_id: str):

    settings = _load_settings()

    wdef = load_wizard_definition(settings, wizard_id)

    if not wdef:

        flash("Unknown wizard.", "error")

        return redirect(url_for("gmcassist.catalog"))

    blk = find_any_blocking_wizard_run(settings)

    if blk:

        bid, bdoc = blk

        raw_rn = raw_request_number_from_run(bdoc)

        disp = raw_rn if raw_rn else "(not confirmed yet)"

        flash(

            "Request %s is still in progress. Resuming that wizard — complete it before starting a new one."

            % disp,

            "info",

        )

        return redirect(url_for("gmcassist.wizard_run", run_id=bid))

    sid = str(wdef.get("service_id") or "").strip()

    label = str(wdef.get("label") or sid)

    run = create_wizard_run(

        settings,

        wizard_id=str(wdef.get("wizard_id")),

        service_id=sid,

        service_label=label,

    )

    return redirect(url_for("gmcassist.wizard_run", run_id=run["wizard_run_id"]))





@gmcassist_bp.route("/run/<run_id>", methods=["GET", "POST"])

def wizard_run(run_id: str):

    settings = _load_settings()

    defs_root = _service_defs_root(settings)

    run = load_wizard_run(settings, run_id)

    if not run:

        flash("Wizard run not found.", "error")

        return redirect(url_for("gmcassist.catalog"))

    run = reconcile_wizard_run_for_blocking(settings, run_id, run)

    _st = str(run.get("status") or "").strip().lower()

    if _st == "completed":

        flash("This wizard run is already completed.", "info")

        return redirect(url_for("gmcassist.catalog"))

    if _st == "abandoned":

        flash("This wizard run is no longer active.", "info")

        return redirect(url_for("gmcassist.catalog"))



    ensure_wizard_context(run)



    wid = str(run.get("wizard_id") or "").strip()

    wizard_def = load_wizard_definition(settings, wid)

    if not wizard_def:

        flash("Wizard definition missing.", "error")

        return redirect(url_for("gmcassist.catalog"))



    steps = wizard_def.get("steps")

    if not isinstance(steps, list) or not steps:

        flash("Invalid wizard definition.", "error")

        return redirect(url_for("gmcassist.catalog"))



    idx = int(run.get("current_step_index") or 0)

    if idx < 0:

        idx = 0

    if idx >= len(steps):

        idx = len(steps) - 1



    step_def = get_step(wizard_def, idx)

    if not step_def:

        flash("Invalid step.", "error")

        return redirect(url_for("gmcassist.catalog"))



    service_id = str(run.get("service_id") or "").strip()

    service_def = get_definition_by_id(service_id, defs_root)

    if not service_def:

        flash("Service definition not found for this wizard.", "error")

        return redirect(url_for("gmcassist.catalog"))



    total = len(steps)

    next_title = step_title(wizard_def, idx + 1) if idx + 1 < total else ""

    prev_title = step_title(wizard_def, idx - 1) if idx > 0 else ""



    if request.method == "POST":

        action = (request.form.get("action") or "").strip().lower()

        if action == "back":

            if idx > 0:

                run_back = load_wizard_run(settings, run_id)

                if not run_back:

                    flash("Wizard run not found.", "error")

                    return redirect(url_for("gmcassist.catalog"))

                ensure_wizard_context(run_back)

                target = idx - 1

                ok_back, back_msg = back_allowed(run_back, idx, target)

                if not ok_back:

                    flash(back_msg, "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                ok_rb, rb_msg = maybe_rollback_before_navigate_back(

                    settings, run_back, wizard_def, target

                )

                if not ok_rb:

                    flash(rb_msg, "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                upd_back: Dict[str, Any] = {

                    "current_step_index": target,

                    "context": run_back.get("context"),

                    "step_data": run_back.get("step_data"),

                }

                update_wizard_run(settings, run_id, upd_back)

                if rb_msg:

                    flash(rb_msg, "ok")

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        stype = str(step_def.get("type") or "").strip()



        if stype == "request_check":

            rn = (request.form.get("request_number") or "").strip()

            ok_gate, err_gate = _request_dir_gate(settings, rn)

            if not ok_gate:

                flash(err_gate, "error")

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

            rn_norm = normalize_wizard_request_number(rn)

            dup = find_blocking_run_for_normalized_request(settings, rn_norm)

            if dup and dup[0] != run_id:

                abandon_wizard_run(

                    settings,

                    run_id,

                    "superseded_by_resume_same_request",

                    superseded_by_run_id=dup[0],

                )

                flash("Resuming the existing wizard run for this request.", "info")

                return redirect(url_for("gmcassist.wizard_run", run_id=dup[0]))

            for oid, odoc in list_blocking_wizard_runs(settings):

                if oid == run_id:

                    continue

                o_raw = raw_request_number_from_run(odoc)

                o_norm = normalize_wizard_request_number(o_raw)

                if o_norm and o_norm != rn_norm:

                    flash(

                        (

                            "Request %s is still in progress. Resume or complete that request "

                            "before starting request %s."

                        )

                        % (o_raw or o_norm, rn),

                        "error",

                    )

                    return redirect(url_for("gmcassist.wizard_run", run_id=oid))

                if not o_norm:

                    flash(

                        "Another wizard run is still in progress (request not confirmed yet). "

                        "Resume it before entering a different request.",

                        "error",

                    )

                    return redirect(url_for("gmcassist.wizard_run", run_id=oid))

            si = run.get("service_inputs")

            if not isinstance(si, dict):

                si = {}

            si["request_number"] = rn

            update_wizard_run(

                settings,

                run_id,

                build_step_advance_updates(

                    idx,

                    len(steps),

                    {"request_number": rn, "service_inputs": si},

                ),

            )

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "cluster_selection":

            run_cs = load_wizard_run(settings, run_id)

            if not run_cs:

                flash("Wizard run not found.", "error")

                return redirect(url_for("gmcassist.catalog"))

            ctx = ensure_wizard_context(run_cs)

            raw = request.form.get("wizard_cluster_list") or ""

            sel_err = apply_cluster_list_to_context(ctx, raw)

            if sel_err:

                flash(sel_err, "error")

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

            update_wizard_run(

                settings,

                run_id,

                build_step_advance_updates(idx, len(steps), {"context": ctx}),

            )

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "form":

            merged = _collect_service_inputs_from_form(request.form)

            si = run.get("service_inputs")

            if not isinstance(si, dict):

                si = {}

            si.update(merged)

            update_wizard_run(

                settings,

                run_id,

                build_step_advance_updates(idx, len(steps), {"service_inputs": si}),

            )

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "cluster_resources_form":

            if action == "next":

                run_cr = load_wizard_run(settings, run_id)

                if not run_cr:

                    flash("Wizard run not found.", "error")

                    return redirect(url_for("gmcassist.catalog"))

                ctx = ensure_wizard_context(run_cr)

                pre_err = validate_cluster_resources_prereq(ctx.get("resource_id"))

                if pre_err:

                    flash(pre_err, "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                try:

                    rid = int(ctx.get("resource_id"))

                except (TypeError, ValueError):

                    flash("resource_id in context is invalid.", "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                ids_c, vals = parse_cluster_resources_form(request.form)

                rows, row_errs = build_cluster_resources_rows(ids_c, vals, rid)

                if row_errs:

                    flash("; ".join(row_errs), "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                update_wizard_run(

                    settings,

                    run_id,

                    build_step_advance_updates(

                        idx,

                        len(steps),

                        {"pending_cluster_resources": rows},

                    ),

                )

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "cluster_resources_apply":

            if action == "next":

                run_apply = load_wizard_run(settings, run_id)

                if not run_apply:

                    flash("Wizard run not found.", "error")

                    return redirect(url_for("gmcassist.catalog"))

                pending = pending_cluster_resources_from_run(run_apply)

                if not pending:

                    flash("No cluster_resources rows to apply; go back and add rows.", "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                try:

                    db = build_adapters(settings)[0]

                except (NotImplementedError, RuntimeError) as e:

                    flash(str(e), "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                ok, inserted_ids, batch_err = apply_cluster_resources_batch(db, pending)

                if not ok:

                    flash(batch_err or "cluster_resources insert failed.", "error")

                    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

                ctx = ensure_wizard_context(run_apply)

                ctx["cluster_resource_row_ids"] = [int(x) for x in inserted_ids]

                sid = str(step_def.get("id") or "cluster_resources_apply")

                sd = run_apply.setdefault("step_data", {})

                if isinstance(sd, dict):

                    sd[sid] = {

                        "insert_target": TABLE_CLUSTER_RESOURCES,

                        "inserted_ids": list(ctx["cluster_resource_row_ids"]),

                    }

                updates = build_step_advance_updates(

                    idx,

                    len(steps),

                    {

                        "context": ctx,

                        "pending_cluster_resources": [],

                        "step_data": run_apply.get("step_data"),

                    },

                )

                update_wizard_run(settings, run_id, updates)

                if int(updates["current_step_index"]) >= len(steps):

                    flash(

                        "Inserted %s cluster_resources row(s). Wizard completed." % len(inserted_ids),

                        "ok",

                    )

                    return redirect(url_for("gmcassist.catalog"))

                flash(

                    "Inserted %s cluster_resources row(s)." % len(inserted_ids),

                    "ok",

                )

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "confirm":

            ls_row, rr_row, cerr = _confirm_preview(settings, run, wizard_def, step_def)

            if cerr:

                flash("; ".join(cerr), "error")

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

            update_wizard_run(

                settings,

                run_id,

                build_step_advance_updates(idx, len(steps), {}),

            )

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "db_insert":

            run_fresh = load_wizard_run(settings, run_id)

            if not run_fresh:

                flash("Wizard run not found.", "error")

                return redirect(url_for("gmcassist.catalog"))

            ok, msg = _db_insert_dispatch(settings, run_fresh, step_def)

            if not ok:

                flash(msg, "error")

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

            updates = build_step_advance_updates(

                idx,

                len(steps),

                {

                    "service_inputs": run_fresh.get("service_inputs"),

                    "context": run_fresh.get("context"),

                    "step_data": run_fresh.get("step_data"),

                },

            )

            update_wizard_run(settings, run_id, updates)

            if int(updates["current_step_index"]) >= len(steps):

                flash("%s Wizard completed." % msg, "ok")

                return redirect(url_for("gmcassist.catalog"))

            flash(msg, "ok")

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "generate" and action == "generate":

            run_fresh = load_wizard_run(settings, run_id)

            if not run_fresh:

                flash("Wizard run not found.", "error")

                return redirect(url_for("gmcassist.catalog"))

            return post_generate(

                settings, run_id, run_fresh, step_def, wizard_def, idx, request.form

            )



        if stype == "diff" and action == "next":

            run_fresh = load_wizard_run(settings, run_id)

            if not run_fresh:

                flash("Wizard run not found.", "error")

                return redirect(url_for("gmcassist.catalog"))

            return post_diff(

                settings, run_id, run_fresh, step_def, wizard_def, idx, len(steps)

            )



        if stype == "publish" and action == "publish":

            run_fresh = load_wizard_run(settings, run_id)

            if not run_fresh:

                flash("Wizard run not found.", "error")

                return redirect(url_for("gmcassist.catalog"))

            return post_publish(

                settings,

                run_id,

                run_fresh,

                step_def,

                wizard_def,

                idx,

                request.form,

                len(steps),

            )



        if stype == "finalize_check" and action == "next":

            run_fresh = load_wizard_run(settings, run_id)

            if not run_fresh:

                flash("Wizard run not found.", "error")

                return redirect(url_for("gmcassist.catalog"))

            return post_finalize_check(settings, run_id, run_fresh, idx, len(steps))



        if stype == "handoff" and action == "next":

            return post_handoff(settings, run_id, idx, len(steps))



        if stype == "manual_note" and action == "next":

            return post_manual_note(settings, run_id, idx, len(steps))



        flash("Unsupported action for this step.", "error")

        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



    # GET

    stype = str(step_def.get("type") or "").strip()

    fields: List[Dict[str, Any]] = []

    if stype == "form":

        fg = str(step_def.get("form_group") or "").strip()

        fields = _guided_fields_for_group(service_def, fg)



    ls_preview = None

    rr_preview = None

    confirm_errors: List[str] = []

    ls_sql_preview = ""

    rr_sql_preview = ""

    if stype == "confirm":

        ls_preview, rr_preview, confirm_errors = _confirm_preview(

            settings, run, wizard_def, step_def

        )

        if ls_preview:

            ls_sql_preview = format_insert_preview("license_servers", ls_preview)

        if rr_preview:

            rr_sql_preview = format_insert_preview("resources_REF", rr_preview)



    si = run.get("service_inputs")

    if not isinstance(si, dict):

        si = {}



    insert_ls_preview = None

    insert_rr_preview = None

    insert_sql_preview = ""

    if stype == "db_insert":

        target = str(step_def.get("insert_target") or "").strip()

        if target in ("license_servers", TABLE_LICENSE_SERVERS):

            insert_ls_preview = build_license_server_row(si, [])

            if insert_ls_preview:

                insert_sql_preview = format_insert_preview("license_servers", insert_ls_preview)

        elif target in ("resources_REF", "resources_ref", TABLE_RESOURCES_REF):

            ctx = run.get("context")

            if not isinstance(ctx, dict):

                ctx = {}

            raw_ls = ctx.get("license_server_id")

            if raw_ls is not None:

                try:

                    insert_rr_preview = build_resource_ref_row(

                        si, int(raw_ls), [], preview=False

                    )

                except (TypeError, ValueError):

                    insert_rr_preview = None

            if insert_rr_preview:

                insert_sql_preview = format_insert_preview("resources_REF", insert_rr_preview)



    gmcassist_extras = merge_get_template_extras(settings, run, step_def, wizard_def)



    form_display_inputs = si

    if stype == "form":

        fg_disp = str(step_def.get("form_group") or "").strip()

        form_display_inputs = _merge_table_defaults_for_group(service_def, fg_disp, si, settings)



    return render_template(

        "gmcassist/wizard_step.html",

        run=run,

        wizard_def=wizard_def,

        step_def=step_def,

        step_index=idx,

        step_total=total,

        step_display=idx + 1,

        service_def=service_def,

        service_label=str(run.get("service_label") or ""),

        fields=fields,

        service_inputs=si,

        form_display_inputs=form_display_inputs,

        request_number=str(run.get("request_number") or ""),

        next_step_title=next_title,

        prev_step_title=prev_title,

        ls_preview=ls_preview,

        rr_preview=rr_preview,

        confirm_errors=confirm_errors,

        insert_ls_preview=insert_ls_preview,

        insert_rr_preview=insert_rr_preview,

        insert_sql_preview=insert_sql_preview,

        ls_sql_preview=ls_sql_preview,

        rr_sql_preview=rr_sql_preview,

        gmcassist_extras=gmcassist_extras,

    )


