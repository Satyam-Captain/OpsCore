"""
GMCAssist wizard step handlers (generate, diff, publish, finalize_check, handoff, manual_note).

Keeps ``gmcassist_routes`` thin; reuses sandbox adapters and ELIM helpers.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

from flask import flash, redirect, url_for

from core.side_by_side_diff import build_side_by_side_rows
from core.text_diff import unified_diff_text
from services.adapters import build_adapters
from services.elim_finalize import finalized_marker_path, request_has_finalized_marker
from services.gmcassist_wizard_config import config_type_allowed_for_wizard
from services.elim_lsb import (
    current_config_path,
    generated_config_path,
    normalize_generation_cluster,
    read_text_file,
)
from services.wizard_run_store import update_wizard_run


def ensure_wizard_context(run: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure ``run['context']`` exists with keys used by post-DB steps."""
    ctx = run.setdefault("context", {})
    if not isinstance(ctx, dict):
        ctx = {}
        run["context"] = ctx
    if "generation_cluster" not in ctx:
        ctx["generation_cluster"] = None
    if "cannot_go_before_index" not in ctx:
        ctx["cannot_go_before_index"] = 0
    if "license_server_id" not in ctx:
        ctx["license_server_id"] = None
    if "resource_id" not in ctx:
        ctx["resource_id"] = None
    if "cluster_list" not in ctx:
        ctx["cluster_list"] = []
    if "cluster_queue_index" not in ctx:
        ctx["cluster_queue_index"] = {}
    if "published_clusters_log" not in ctx:
        ctx["published_clusters_log"] = []
    return ctx


def back_allowed(run: Dict[str, Any], current_index: int, target_index: int) -> Tuple[bool, str]:
    """
    Enforce simplest safe back navigation after publish (no rollback of published files).

    ``cannot_go_before_index`` is the minimum step index the user may visit; indices below
    it are blocked when navigating backward.
    """
    ctx = ensure_wizard_context(run)
    cannot_before = int(ctx.get("cannot_go_before_index") or 0)
    if target_index < cannot_before:
        return (
            False,
            "Back is disabled before a completed publish step (published files are not rolled back).",
        )
    return True, ""


def _config_type(step_def: Dict[str, Any]) -> str:
    return str(step_def.get("config_type") or "").strip()


def find_generate_step_index(wizard_def: Dict[str, Any], config_type: str) -> Optional[int]:
    """Step index of the first ``generate`` step for ``config_type`` (for multi-cluster loopback)."""
    steps = wizard_def.get("steps")
    if not isinstance(steps, list):
        return None
    ct = (config_type or "").strip()
    for i, s in enumerate(steps):
        if not isinstance(s, dict):
            continue
        if str(s.get("type") or "").strip() != "generate":
            continue
        if str(s.get("config_type") or "").strip() == ct:
            return i
    return None


def _parse_cluster_list(raw: str) -> List[str]:
    """Split comma- or newline-separated cluster names (non-empty strips)."""
    out: List[str] = []
    if not raw or not str(raw).strip():
        return out
    for chunk in str(raw).replace("\r", "").split("\n"):
        for part in chunk.split(","):
            t = part.strip()
            if t:
                out.append(t)
    return out


def _normalize_cluster_list(parts: List[str]) -> Tuple[Optional[List[str]], Optional[str]]:
    """Return (clusters, error_message)."""
    seen = set()
    out: List[str] = []
    for p in parts:
        n = normalize_generation_cluster(p)
        if not n:
            return None, "Invalid cluster name in list: %r" % (p,)
        if n not in seen:
            seen.add(n)
            out.append(n)
    if not out:
        return None, "Enter at least one cluster."
    return out, None


def apply_cluster_list_to_context(ctx: Dict[str, Any], raw: str) -> Optional[str]:
    """
    Parse operator cluster list (comma/newline), store on context for the whole wizard run.

    Resets per-config-type queue indices so each config type starts at the first cluster again.
    Returns an error message, or None on success.
    """
    parts = _parse_cluster_list(raw)
    validated, err = _normalize_cluster_list(parts)
    if err:
        return err
    assert validated is not None
    ctx["cluster_list"] = validated
    ctx["cluster_queue_index"] = {}
    ctx["generation_cluster"] = validated[0] if validated else None
    return None


def get_cluster_selection_extras(run: Dict[str, Any]) -> Dict[str, Any]:
    """Prefill cluster selection textarea from saved context."""
    ctx = ensure_wizard_context(run)
    cl = ctx.get("cluster_list")
    text = ""
    if isinstance(cl, list) and cl:
        text = ", ".join(str(x) for x in cl)
    return {"wizard_cluster_list_text": text}


def _config_policy_error(wizard_def: Dict[str, Any], config_type: str) -> str:
    ok, msg = config_type_allowed_for_wizard(wizard_def, config_type)
    return "" if ok else msg


def _scripts_adapter(settings: Dict[str, Any]):
    _, _, scripts = build_adapters(settings)
    return scripts


def _generation_ctx_payload(run: Dict[str, Any], cluster: str) -> Dict[str, Any]:
    si = run.get("service_inputs") if isinstance(run.get("service_inputs"), dict) else {}
    return {
        "service_inputs": si,
        "request_number": str(run.get("request_number") or ""),
        "generation_cluster": cluster,
        "service_id": str(run.get("service_id") or ""),
    }


def get_generate_extras(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
) -> Dict[str, Any]:
    ctx = ensure_wizard_context(run)
    ct = _config_type(step_def)
    sid = str(step_def.get("id") or "")
    sd = run.get("step_data") if isinstance(run.get("step_data"), dict) else {}
    last = sd.get(sid) if isinstance(sd.get(sid), dict) else {}
    cluster_list = ctx.get("cluster_list") if isinstance(ctx.get("cluster_list"), list) else []
    idxs = ctx.get("cluster_queue_index") if isinstance(ctx.get("cluster_queue_index"), dict) else {}
    pos = int(idxs.get(ct) or 0)
    queue_label = ""
    if cluster_list:
        cur = cluster_list[pos] if 0 <= pos < len(cluster_list) else ""
        queue_label = "Cluster %s — %s of %s for %s" % (cur, pos + 1, len(cluster_list), ct)
    return {
        "config_type": ct,
        "last_generate": last,
        "config_policy_error": _config_policy_error(wizard_def, ct),
        "cluster_queue_label": queue_label,
        "cluster_queue_count": len(cluster_list),
    }


def post_generate(
    settings: Dict[str, Any],
    run_id: str,
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
    idx: int,
    form: Any,
) -> Any:
    """Run mock/real generate; advance on success."""
    _ = form
    ctx = ensure_wizard_context(run)
    ct = _config_type(step_def)
    cluster_list = ctx.get("cluster_list")
    if not isinstance(cluster_list, list) or len(cluster_list) == 0:
        flash("Set your cluster list on the Clusters step first.", "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))
    idx_map = ctx.setdefault("cluster_queue_index", {})
    if not isinstance(idx_map, dict):
        idx_map = {}
        ctx["cluster_queue_index"] = idx_map
    pos = int(idx_map.get(ct) or 0)
    if pos < 0 or pos >= len(cluster_list):
        flash("Cluster queue state is invalid; refresh this step.", "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))
    nc = cluster_list[pos]
    ctx["generation_cluster"] = nc

    if not ct:
        flash("Wizard step is missing config_type.", "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    pol_err = _config_policy_error(wizard_def, ct)
    if pol_err:
        flash(pol_err, "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    scripts = _scripts_adapter(settings)
    gctx = _generation_ctx_payload(run, nc)
    try:
        out_path = scripts.generate_config(ct, nc, gctx)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    sid = str(step_def.get("id") or "generate")
    sd = run.setdefault("step_data", {})
    if not isinstance(sd, dict):
        sd = {}
        run["step_data"] = sd
    sd[sid] = {
        "kind": "generate",
        "config_type": ct,
        "cluster": nc,
        "generated_path": out_path,
        "status": "done",
    }
    update_wizard_run(
        settings,
        run_id,
        {
            "context": ctx,
            "step_data": run.get("step_data"),
            "current_step_index": idx + 1,
        },
    )
    flash("Generated %s for cluster %s." % (ct, nc), "ok")
    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))


def get_diff_extras(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
) -> Dict[str, Any]:
    ctx = ensure_wizard_context(run)
    ct = _config_type(step_def)
    cluster = normalize_generation_cluster(ctx.get("generation_cluster"))
    err: Optional[str] = None
    diff_text = ""
    cur_p = ""
    gen_p = ""
    cur_missing = True
    gen_missing = True
    sbs_rows: Optional[List[Dict[str, str]]] = None
    sbs_truncated = False
    pol = _config_policy_error(wizard_def, ct)
    if pol:
        err = pol
    elif not cluster:
        err = "Set cluster on the previous generate step (or enter it there first)."
    elif cluster:
        try:
            cur_p = current_config_path(settings, cluster, ct)
            gen_p = generated_config_path(settings, cluster, ct)
        except ValueError as e:
            err = str(e)
        else:
            left, _ = read_text_file(cur_p)
            right, gen_ok = read_text_file(gen_p)
            gen_missing = not gen_ok
            cur_missing = not os.path.isfile(cur_p)
            if not gen_ok:
                err = "Generated file not found; run Generate for this config first."
            else:
                diff_text = unified_diff_text(
                    left,
                    right,
                    "current (%s)" % cluster,
                    "generated (%s)" % cluster,
                )
                try:
                    raw_sbs = build_side_by_side_rows(left or "", right or "")
                    cap = 6000
                    if len(raw_sbs) > cap:
                        sbs_truncated = True
                        raw_sbs = raw_sbs[:cap]
                    sbs_rows = [
                        {"left": r.left, "right": r.right, "kind": r.kind}
                        for r in raw_sbs
                    ]
                except (TypeError, ValueError, AttributeError):
                    sbs_rows = None
    return {
        "diff_cluster": cluster or "",
        "diff_config_type": ct,
        "diff_text": diff_text,
        "diff_side_by_side_rows": sbs_rows,
        "diff_side_by_side_truncated": sbs_truncated,
        "diff_error": err,
        "diff_current_path": cur_p,
        "diff_generated_path": gen_p,
        "diff_current_missing": cur_missing,
        "diff_gen_missing": gen_missing,
        "config_policy_error": pol if pol else "",
    }


def post_diff(
    settings: Dict[str, Any],
    run_id: str,
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
    idx: int,
    steps_len: int,
) -> Any:
    """Advance after reviewing diff."""
    extras = get_diff_extras(settings, run, step_def, wizard_def)
    if extras.get("diff_error"):
        flash(str(extras["diff_error"]), "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))
    sid = str(step_def.get("id") or "diff")
    sd = run.setdefault("step_data", {})
    if isinstance(sd, dict):
        sd[sid] = {"kind": "diff", "config_type": _config_type(step_def), "status": "done"}
    new_idx = idx + 1
    upd: Dict[str, Any] = {"step_data": run.get("step_data"), "current_step_index": new_idx}
    if new_idx >= steps_len:
        upd["status"] = "completed"
    update_wizard_run(settings, run_id, upd)
    if new_idx >= steps_len:
        flash("Wizard completed.", "ok")
        return redirect(url_for("gmcassist.catalog"))
    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))


def get_publish_extras(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
) -> Dict[str, Any]:
    ctx = ensure_wizard_context(run)
    ct = _config_type(step_def)
    cluster = normalize_generation_cluster(ctx.get("generation_cluster"))
    gen_p = ""
    gen_ok = False
    err: Optional[str] = None
    pol = _config_policy_error(wizard_def, ct)
    if pol:
        err = pol
    elif not cluster:
        err = "Cluster is not set; complete the generate step for this config first."
    else:
        try:
            gen_p = generated_config_path(settings, cluster, ct)
            gen_ok = bool(gen_p and os.path.isfile(gen_p))
        except ValueError as e:
            err = str(e)
        if not err and not gen_ok:
            err = "Generated file missing; run Generate before Publish."
    queue_label = ""
    cluster_list = ctx.get("cluster_list") if isinstance(ctx.get("cluster_list"), list) else []
    idxs = ctx.get("cluster_queue_index") if isinstance(ctx.get("cluster_queue_index"), dict) else {}
    pos = int(idxs.get(ct) or 0)
    if cluster_list and cluster:
        queue_label = "Cluster %s — %s of %s for %s" % (cluster, pos + 1, len(cluster_list), ct)
    return {
        "publish_config_type": ct,
        "publish_generated_path": gen_p,
        "publish_ready": gen_ok and not err,
        "publish_error": err,
        "config_policy_error": pol if pol else "",
        "publish_cluster_queue_label": queue_label,
    }


def post_publish(
    settings: Dict[str, Any],
    run_id: str,
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
    idx: int,
    form: Any,
    steps_len: int,
) -> Any:
    ctx = ensure_wizard_context(run)
    nc = normalize_generation_cluster(ctx.get("generation_cluster"))
    if not nc:
        flash("Cluster is not set in wizard context; complete Generate for this config first.", "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    ct = _config_type(step_def)
    pol_err = _config_policy_error(wizard_def, ct)
    if pol_err:
        flash(pol_err, "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    try:
        gen_p = generated_config_path(settings, nc, ct)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))
    if not os.path.isfile(gen_p):
        flash("Generated file missing; run Generate first.", "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    req_no = str(run.get("request_number") or "").strip()
    if not req_no:
        flash("Request number is missing.", "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    scripts = _scripts_adapter(settings)
    try:
        dest = scripts.publish_to_request(ct, nc, req_no, gen_p)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    sid = str(step_def.get("id") or "publish")
    sd = run.setdefault("step_data", {})
    if isinstance(sd, dict):
        sd[sid] = {
            "kind": "publish",
            "config_type": ct,
            "destination_path": dest,
            "cluster": nc,
            "status": "done",
        }

    pcl = ctx.setdefault("published_clusters_log", [])
    if isinstance(pcl, list):
        pcl.append(
            {
                "cluster": nc,
                "config_type": ct,
                "destination_path": dest,
            }
        )

    prev_floor = int(ctx.get("cannot_go_before_index") or 0)
    ctx["cannot_go_before_index"] = max(prev_floor, idx)

    cluster_list = ctx.get("cluster_list") if isinstance(ctx.get("cluster_list"), list) else []
    idxm = ctx.get("cluster_queue_index") if isinstance(ctx.get("cluster_queue_index"), dict) else {}
    pos = int(idxm.get(ct) or 0)

    if isinstance(cluster_list, list) and len(cluster_list) and (pos + 1) < len(cluster_list):
        idxm[ct] = pos + 1
        ctx["cluster_queue_index"] = idxm
        ctx["generation_cluster"] = cluster_list[pos + 1]
        gen_idx = find_generate_step_index(wizard_def, ct)
        if gen_idx is None:
            flash(
                "Published %s but could not find the generate step to continue clusters."
                % ct,
                "error",
            )
            return redirect(url_for("gmcassist.wizard_run", run_id=run_id))
        upd_loop: Dict[str, Any] = {
            "context": ctx,
            "step_data": run.get("step_data"),
            "current_step_index": gen_idx,
        }
        update_wizard_run(settings, run_id, upd_loop)
        flash(
            "Published %s for cluster %s. Continue with cluster %s."
            % (ct, nc, cluster_list[pos + 1]),
            "ok",
        )
        return redirect(url_for("gmcassist.wizard_run", run_id=run_id))

    new_idx = idx + 1
    upd: Dict[str, Any] = {
        "context": ctx,
        "step_data": run.get("step_data"),
        "current_step_index": new_idx,
    }
    if new_idx >= steps_len:
        upd["status"] = "completed"
    update_wizard_run(settings, run_id, upd)
    if new_idx >= steps_len:
        flash("Published %s. Wizard completed." % ct, "ok")
        return redirect(url_for("gmcassist.catalog"))
    flash("Published %s into the request directory." % ct, "ok")
    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))


def get_finalize_check_extras(settings: Dict[str, Any], run: Dict[str, Any]) -> Dict[str, Any]:
    rn = str(run.get("request_number") or "").strip()
    marker_path = ""
    has_marker = False
    err: Optional[str] = None
    if not rn:
        err = "Request number is missing."
    else:
        try:
            has_marker = request_has_finalized_marker(settings, rn)
            marker_path = finalized_marker_path(settings, rn)
        except ValueError as e:
            err = str(e)
    return {
        "finalize_error": err,
        "finalize_has_marker": has_marker,
        "finalize_marker_path": marker_path,
        "finalize_message": (
            "This step is read-only: it only checks whether the request directory contains a "
            ".finalized marker file."
        ),
        "finalize_bullets": [
            "GMCAssist does not perform finalize — it never creates or removes the .finalized marker.",
            "Run finalize in GMC / Grid (or your site procedure) outside this tool.",
            "The status below shows only whether that marker file exists on the configured request path.",
        ],
    }


def post_finalize_check(
    settings: Dict[str, Any],
    run_id: str,
    _run: Dict[str, Any],
    idx: int,
    steps_len: int,
) -> Any:
    new_idx = idx + 1
    upd: Dict[str, Any] = {"current_step_index": new_idx}
    if new_idx >= steps_len:
        upd["status"] = "completed"
    update_wizard_run(settings, run_id, upd)
    if new_idx >= steps_len:
        flash("Wizard completed.", "ok")
        return redirect(url_for("gmcassist.catalog"))
    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))


def get_handoff_extras(run: Dict[str, Any]) -> Dict[str, Any]:
    ctx = ensure_wizard_context(run)
    rn = str(run.get("request_number") or "").strip()
    req_disp = rn or "<REQNO>"
    pcl = ctx.get("published_clusters_log")
    items: List[Dict[str, str]] = []
    seen = set()
    if isinstance(pcl, list):
        for entry in pcl:
            if not isinstance(entry, dict):
                continue
            cl = normalize_generation_cluster(entry.get("cluster"))
            if cl and cl not in seen:
                seen.add(cl)
                items.append(
                    {
                        "cluster": cl,
                        "command": "bsub.gmcreq -m PUT -r %s -c %s -nu" % (req_disp, cl),
                    }
                )
    if not items:
        cl = normalize_generation_cluster(ctx.get("generation_cluster"))
        cluster = cl or "<cluster>"
        items.append(
            {
                "cluster": cluster,
                "command": "bsub.gmcreq -m PUT -r %s -c %s -nu" % (req_disp, cluster),
            }
        )
    primary = items[0]
    return {
        "handoff_clusters": items,
        "handoff_command": primary.get("command", ""),
        "handoff_request": rn,
        "handoff_cluster": primary.get("cluster", ""),
        "handoff_restart_note": (
            "Restart or reload the relevant Grid / LSF services using your site procedure."
        ),
    }


def post_handoff(
    settings: Dict[str, Any],
    run_id: str,
    idx: int,
    steps_len: int,
) -> Any:
    new_idx = idx + 1
    upd: Dict[str, Any] = {"current_step_index": new_idx}
    if new_idx >= steps_len:
        upd["status"] = "completed"
    update_wizard_run(settings, run_id, upd)
    if new_idx >= steps_len:
        flash("Wizard completed.", "ok")
        return redirect(url_for("gmcassist.catalog"))
    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))


MANUAL_NOTE_BODY = (
    "Complete ELIM follow-up outside GMCAssist: edit elim_license_resource.json, "
    "add the resource/server as required, and restart lim (per your site procedure)."
)


def get_manual_note_extras() -> Dict[str, Any]:
    return {"manual_note_text": MANUAL_NOTE_BODY}


def post_manual_note(settings: Dict[str, Any], run_id: str, idx: int, steps_len: int) -> Any:
    new_idx = idx + 1
    upd: Dict[str, Any] = {"current_step_index": new_idx}
    if new_idx >= steps_len:
        upd["status"] = "completed"
    update_wizard_run(settings, run_id, upd)
    if new_idx >= steps_len:
        flash("Wizard completed.", "ok")
        return redirect(url_for("gmcassist.catalog"))
    return redirect(url_for("gmcassist.wizard_run", run_id=run_id))


def merge_get_template_extras(
    settings: Dict[str, Any],
    run: Dict[str, Any],
    step_def: Dict[str, Any],
    wizard_def: Dict[str, Any],
) -> Dict[str, Any]:
    """Extra template variables keyed by step type."""
    st = str(step_def.get("type") or "").strip()
    if st == "generate":
        return get_generate_extras(settings, run, step_def, wizard_def)
    if st == "diff":
        return get_diff_extras(settings, run, step_def, wizard_def)
    if st == "publish":
        return get_publish_extras(settings, run, step_def, wizard_def)
    if st == "finalize_check":
        return get_finalize_check_extras(settings, run)
    if st == "handoff":
        return get_handoff_extras(run)
    if st == "manual_note":
        return get_manual_note_extras()
    if st == "cluster_selection":
        return get_cluster_selection_extras(run)
    return {}
