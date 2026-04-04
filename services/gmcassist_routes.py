"""

GMCAssist: linear service wizards (Flask + Jinja). Additive to the legacy Services workspace.

"""



from typing import Any, Dict, List, Optional, Tuple



from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, url_for



from core.storage import load_json

from services.adapters import build_adapters

from services.blueprint import build_table_default_values

from services.elim_db import (

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

from services.input_help import build_help_payload, find_guided_field

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

from services.wizard_run_store import create_wizard_run, load_wizard_run, update_wizard_run



gmcassist_bp = Blueprint("gmcassist", __name__)





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





@gmcassist_bp.route("/")

def catalog():

    settings = _load_settings()

    wizards = list_wizard_definitions(settings)

    return render_template("gmcassist/catalog.html", wizards=wizards)





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





@gmcassist_bp.route("/wizard/<wizard_id>/start", methods=["POST"])

def wizard_start(wizard_id: str):

    settings = _load_settings()

    wdef = load_wizard_definition(settings, wizard_id)

    if not wdef:

        flash("Unknown wizard.", "error")

        return redirect(url_for("gmcassist.catalog"))

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

    if str(run.get("status") or "") == "completed":

        flash("This wizard run is already completed.", "info")

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

            si = run.get("service_inputs")

            if not isinstance(si, dict):

                si = {}

            si["request_number"] = rn

            update_wizard_run(

                settings,

                run_id,

                {

                    "request_number": rn,

                    "service_inputs": si,

                    "current_step_index": idx + 1,

                },

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

                {"context": ctx, "current_step_index": idx + 1},

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

                {"service_inputs": si, "current_step_index": idx + 1},

            )

            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))



        if stype == "confirm":

            ls_row, rr_row, cerr = _confirm_preview(settings, run, wizard_def, step_def)

            if cerr:

                flash("; ".join(cerr), "error")

                return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

            update_wizard_run(settings, run_id, {"current_step_index": idx + 1})

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

            new_idx = idx + 1

            updates: Dict[str, Any] = {

                "service_inputs": run_fresh.get("service_inputs"),

                "context": run_fresh.get("context"),

                "step_data": run_fresh.get("step_data"),

                "current_step_index": new_idx,

            }

            if new_idx >= len(steps):

                updates["status"] = "completed"

            update_wizard_run(settings, run_id, updates)

            if new_idx >= len(steps):

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


