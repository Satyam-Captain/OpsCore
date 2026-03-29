"""
Request blueprint metadata and workspace view-model builders.

Service definitions may declare ``request_blueprint``, ``config_files``, and ``tables``.
Rendering stays generic; ELIM-specific status inference lives in private helpers below.
"""


import json
from typing import Any, Dict, List, Optional, Tuple

from flask import url_for

from services.elim_db import (
    TABLE_LICENSE_SERVERS,
    TABLE_RESOURCES_REF,
    elim_can_rollback,
    elim_db_already_applied,
    elim_db_step_done,
    elim_rollback_executed,
    is_license_resource_workspace_operation,
    validate_elim_service_inputs,
)
from services.elim_lsb import (
    elim_config_ui_rows,
    normalize_generation_cluster,
    normalize_generation_state,
    normalize_publish_state,
)
from services.input_help import iter_guided_fields
from services.workspace_lifecycle import effective_lifecycle_status

# --- Blueprint accessors (definition JSON; safe on any service) ---


def get_request_blueprint(service_def: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(service_def, dict):
        return None
    rb = service_def.get("request_blueprint")
    return rb if isinstance(rb, dict) else None


def get_config_files(service_def: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(service_def, dict):
        return []
    raw = service_def.get("config_files")
    if not isinstance(raw, list):
        return []
    rows = [x for x in raw if isinstance(x, dict) and x.get("id")]
    return sorted(rows, key=lambda x: int(x.get("order", 0)))


def get_table_defs(service_def: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if not isinstance(service_def, dict):
        return {}
    t = service_def.get("tables")
    if not isinstance(t, dict):
        return {}
    return {k: v for k, v in t.items() if isinstance(v, dict) and v.get("id")}


def get_blueprint_generation_config_ids(
    service_def: Optional[Dict[str, Any]],
) -> Tuple[str, ...]:
    """Ordered config file ids that support mock generation (for operation UI rows)."""
    return tuple(
        str(c.get("id"))
        for c in get_config_files(service_def)
        if c.get("generation_supported")
    )


def get_blueprint_finalization_config_ids(
    service_def: Optional[Dict[str, Any]],
) -> Tuple[str, ...]:
    """Config ids required before request finalization (.finalized)."""
    return tuple(
        str(c.get("id"))
        for c in get_config_files(service_def)
        if c.get("required_for_finalization")
    )


def get_config_by_id(
    service_def: Optional[Dict[str, Any]], config_id: str
) -> Optional[Dict[str, Any]]:
    cid = str(config_id or "").strip()
    if not cid:
        return None
    for c in get_config_files(service_def):
        if str(c.get("id")) == cid:
            return c
    return None


def get_tables_for_config(
    service_def: Optional[Dict[str, Any]], config_id: str
) -> List[Dict[str, Any]]:
    cfg = get_config_by_id(service_def, config_id)
    if not cfg:
        return []
    tmap = get_table_defs(service_def)
    out: List[Dict[str, Any]] = []
    for tid in cfg.get("depends_on_tables") or []:
        key = str(tid)
        if key in tmap:
            out.append(tmap[key])
    return out


def config_anchor_id(config_id: str) -> str:
    """HTML id fragment for in-page links (no dots)."""
    return "cfg-" + str(config_id).replace(".", "-")


def service_has_guided_inputs(service_def: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(service_def, dict):
        return False
    return len(iter_guided_fields(service_def)) > 0


# --- ELIM-specific workspace state (isolated heuristics) ---


def _elim_workspace_table_states(doc: Dict[str, Any]) -> Dict[str, str]:
    """
    Per logical table id: ``not_started`` | ``applied`` | ``rolled_back``.

    After rollback, rows that had been inserted are shown as rolled back for audit.
    """
    ls_done = elim_db_step_done(doc, "insert_license_server")
    rr_done = elim_db_step_done(doc, "insert_resource")
    if elim_rollback_executed(doc):
        return {
            "license_servers": "rolled_back" if ls_done else "not_started",
            "resources_REF": "rolled_back" if rr_done else "not_started",
        }
    return {
        "license_servers": "applied" if ls_done else "not_started",
        "resources_REF": "applied" if rr_done else "not_started",
    }


def _elim_publish_done(doc: Dict[str, Any], config_id: str) -> bool:
    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    pub = normalize_publish_state(sr.get("publish"))
    ent = pub.get(config_id) if isinstance(pub.get(config_id), dict) else {}
    return ent.get("status") == "done"


def _elim_generation_done(doc: Dict[str, Any], config_id: str) -> bool:
    sr = doc.get("step_results") if isinstance(doc.get("step_results"), dict) else {}
    gen = normalize_generation_state(sr.get("generation"))
    ent = gen.get(config_id) if isinstance(gen.get(config_id), dict) else {}
    return ent.get("status") == "done"


def _earliest_config_for_table(
    ordered_configs: List[Dict[str, Any]], table_id: str
) -> Optional[str]:
    for c in ordered_configs:
        deps = c.get("depends_on_tables") or []
        if table_id in [str(x) for x in deps]:
            return str(c.get("id"))
    return None


def _infer_elim_config_status(
    cfg: Dict[str, Any],
    doc: Dict[str, Any],
    table_states: Dict[str, str],
) -> str:
    """``not_started`` | ``in_progress`` | ``published`` | ``rolled_back``."""
    cid = str(cfg.get("id"))
    if elim_rollback_executed(doc):
        return "rolled_back"
    if _elim_publish_done(doc, cid):
        return "published"
    deps = [str(x) for x in (cfg.get("depends_on_tables") or [])]
    dep_touched = any(table_states.get(d) in ("applied", "rolled_back") for d in deps)
    if _elim_generation_done(doc, cid) or dep_touched:
        return "in_progress"
    # Configs with no declared table deps (e.g. generation-only): in progress once request DB apply exists.
    if not deps and elim_db_already_applied(doc):
        return "in_progress"
    return "not_started"


def _status_css_workspace(kind: str, status: str) -> str:
    """Shared class names for workspace strip and sections."""
    base = "workspace-status workspace-status--" + kind
    return base + " workspace-status--" + status


def _workspace_config_status_phrase(status: str) -> str:
    """Human phrase for coarse config strip status (non-ELIM / legacy)."""
    m = {
        "not_started": "Not started",
        "in_progress": "In progress",
        "published": "Published",
        "rolled_back": "Rolled back",
    }
    return m.get(status, status.replace("_", " ").title())


def _table_row_for_config(
    table_id: str,
    table_def: Dict[str, Any],
    config_id: str,
    ordered_configs: List[Dict[str, Any]],
    table_states: Dict[str, str],
) -> Dict[str, Any]:
    """One dependency row: applied vs reused vs rolled back."""
    label = str(table_def.get("label") or table_id)
    base = table_states.get(table_id, "not_started")
    earliest = _earliest_config_for_table(ordered_configs, table_id)
    reused = (
        base == "applied"
        and earliest is not None
        and earliest != config_id
    )
    if base == "rolled_back":
        display = "rolled_back"
        note = None
        reused_flag = False
    elif base == "not_started":
        display = "not_started"
        note = None
        reused_flag = False
    elif reused:
        display = "reused"
        note = "Already satisfied — reused from request state (no new input for this config)."
        reused_flag = True
    else:
        display = "applied"
        note = None
        reused_flag = False

    return {
        "table_id": table_id,
        "label": label,
        "table_name": str(table_def.get("table_name") or table_id),
        "status": display,
        "css_class": _status_css_workspace("table", display),
        "reused": reused_flag,
        "satisfied_note": note,
        "reuse_within_request": bool(table_def.get("reuse_within_request")),
    }


def _empty_workspace(
    *,
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    message: str,
) -> Dict[str, Any]:
    return {
        "has_blueprint": False,
        "blueprint_meta": None,
        "config_strip": [],
        "config_sections": [],
        "workspace_summary": {"total_configs": 0, "published_count": 0},
        "fallback_message": message,
        "show_elim_actions": is_license_resource_workspace_operation(operation),
    }


def build_workspace_view_model(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    settings: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a dict for ``workspace.html`` (strip, sections, summary).

    When the service has no blueprint metadata, returns a safe minimal model.
    ELIM operations get table/config statuses from ``operation`` heuristics;
    other services still render blueprint structure with neutral ``not_started`` states.
    """
    rb = get_request_blueprint(service_def)
    cfg_files = get_config_files(service_def)
    if not rb or not cfg_files:
        return _empty_workspace(
            service_def=service_def,
            operation=operation,
            message="No request blueprint is configured for this service.",
        )

    use_elim_inference = is_license_resource_workspace_operation(operation)
    if use_elim_inference:
        table_states = _elim_workspace_table_states(operation)
    else:
        table_states = {k: "not_started" for k in get_table_defs(service_def).keys()}

    tmap = get_table_defs(service_def)
    sr = operation.get("step_results") if isinstance(operation.get("step_results"), dict) else {}
    elim_config_enabled = use_elim_inference and elim_db_already_applied(operation)
    gen_cluster = (
        normalize_generation_cluster(operation.get("generation_context_cluster"))
        if use_elim_inference
        else None
    )
    elim_rows_by_ct: Dict[str, Dict[str, Any]] = {}
    if use_elim_inference:
        rows = elim_config_ui_rows(settings, sr, elim_config_enabled, gen_cluster)
        elim_rows_by_ct = {str(r["config_type"]): r for r in rows}

    config_strip: List[Dict[str, Any]] = []
    config_sections: List[Dict[str, Any]] = []
    published_count = 0

    for cfg in cfg_files:
        cid = str(cfg.get("id"))
        if use_elim_inference:
            cstatus = _infer_elim_config_status(cfg, operation, table_states)
        else:
            cstatus = "not_started"
        if cstatus == "published":
            published_count += 1

        short = str(cfg.get("short_label") or cfg.get("label") or cid)
        cfg_label = str(cfg.get("label") or cid)
        if use_elim_inference:
            stage_phrase = _elim_config_stage_label(
                infer_config_stage(service_def, operation, cid, settings)
            )
        else:
            stage_phrase = _workspace_config_status_phrase(cstatus)
        stage_line = f"{cfg_label} — {stage_phrase}"
        strip_item = {
            "id": cid,
            "label": cfg_label,
            "short_label": short,
            "status": cstatus,
            "css_class": _status_css_workspace("config", cstatus),
            "anchor_href": "#" + config_anchor_id(cid),
            "stage_line": stage_line,
        }
        config_strip.append(strip_item)

        dep_ids = [str(x) for x in (cfg.get("depends_on_tables") or [])]
        has_manual_tables = len(dep_ids) > 0
        table_rows: List[Dict[str, Any]] = []
        for tid in dep_ids:
            if tid not in tmap:
                continue
            if use_elim_inference:
                table_rows.append(
                    _table_row_for_config(
                        tid, tmap[tid], cid, cfg_files, table_states
                    )
                )
            else:
                table_rows.append(
                    {
                        "table_id": tid,
                        "label": str(tmap[tid].get("label") or tid),
                        "table_name": str(tmap[tid].get("table_name") or tid),
                        "status": "not_started",
                        "css_class": _status_css_workspace("table", "not_started"),
                        "reused": False,
                        "satisfied_note": None,
                        "reuse_within_request": bool(
                            tmap[tid].get("reuse_within_request")
                        ),
                    }
                )

        erow = elim_rows_by_ct.get(cid, {})
        gen_only_note = None
        if not has_manual_tables:
            gen_only_note = str(cfg.get("workspace_generation_note") or "").strip() or (
                "This config is generation-only for this request type: no manual DB table "
                "row is tied to this artifact. Use generate, diff, and publish when the "
                "request is ready."
                if bool(cfg.get("derived_only"))
                else (
                    "No manual DB table dependencies are declared for this config. "
                    "Use generate, diff, and publish when ready."
                )
            )
        section = {
            "id": cid,
            "label": cfg_label,
            "order": int(cfg.get("order", 0)),
            "status": cstatus,
            "css_class": _status_css_workspace("config", cstatus),
            "anchor_id": config_anchor_id(cid),
            "stage_line": stage_line,
            "generation_supported": bool(cfg.get("generation_supported")),
            "diff_supported": bool(cfg.get("diff_supported")),
            "publish_supported": bool(cfg.get("publish_supported")),
            "required_for_finalization": bool(cfg.get("required_for_finalization")),
            "derived_only": bool(cfg.get("derived_only")),
            "has_manual_tables": has_manual_tables,
            "generation_only_note": gen_only_note,
            "tables": table_rows,
            "actions": {
                "generate": bool(erow.get("can_generate")),
                "diff": bool(erow.get("can_diff")),
                "publish": bool(erow.get("can_publish")),
            },
        }
        config_sections.append(section)

    return {
        "has_blueprint": True,
        "blueprint_meta": {
            "mode": rb.get("mode"),
            "label": rb.get("label"),
            "description": rb.get("description"),
            "completion_strategy": rb.get("completion_strategy"),
        },
        "config_strip": config_strip,
        "config_sections": config_sections,
        "workspace_summary": {
            "total_configs": len(cfg_files),
            "published_count": published_count,
        },
        "fallback_message": None,
        "show_elim_actions": use_elim_inference,
    }


def _manual_table_dependency_note(row: Dict[str, Any]) -> str:
    """Human-readable note for config workspace dependency rows."""
    if row.get("satisfied_note"):
        return str(row["satisfied_note"])
    st = row.get("status")
    if st == "reused" or row.get("reused"):
        return "Already satisfied / reused for this config."
    if st == "applied":
        return "Satisfied for this config (applied in request DB)."
    if st == "rolled_back":
        return "Rolled back; re-apply request DB if needed."
    if st == "not_started":
        return "Needs upstream service inputs and DB apply."
    return ""


def _manual_table_is_actionable(row: Dict[str, Any]) -> bool:
    """
    True when this dependency still needs operator attention for the request.

    Reused or freshly applied rows are not actionable from this config's perspective.
    """
    st = row.get("status")
    if row.get("reused") or st == "reused":
        return False
    if st == "applied":
        return False
    return st in ("not_started", "rolled_back")


def _collect_manual_table_rows_for_config(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Same shape as ``manual_tables`` in the config workspace model (for reuse + stage inference).
    """
    cid = str(cfg.get("id"))
    use_elim_inference = is_license_resource_workspace_operation(operation)
    cfg_files = get_config_files(service_def)
    if use_elim_inference:
        table_states = _elim_workspace_table_states(operation)
    else:
        table_states = {k: "not_started" for k in get_table_defs(service_def).keys()}
    tmap = get_table_defs(service_def)
    dep_ids = [str(x) for x in (cfg.get("depends_on_tables") or [])]
    manual_tables: List[Dict[str, Any]] = []
    for tid in dep_ids:
        if tid not in tmap:
            continue
        if use_elim_inference:
            raw = _table_row_for_config(tid, tmap[tid], cid, cfg_files, table_states)
        else:
            raw = {
                "table_id": tid,
                "label": str(tmap[tid].get("label") or tid),
                "table_name": str(tmap[tid].get("table_name") or tid),
                "status": "not_started",
                "css_class": _status_css_workspace("table", "not_started"),
                "reused": False,
                "satisfied_note": None,
                "reuse_within_request": bool(tmap[tid].get("reuse_within_request")),
            }
        manual_tables.append(
            {
                "table_id": raw.get("table_id"),
                "label": raw.get("label"),
                "table_name": raw.get("table_name"),
                "status": raw.get("status"),
                "css_class": raw.get("css_class"),
                "note": _manual_table_dependency_note(raw),
                "is_reused": bool(raw.get("reused") or raw.get("status") == "reused"),
                "is_actionable": _manual_table_is_actionable(raw),
            }
        )
    return manual_tables


def _next_config_in_blueprint_order(
    service_def: Optional[Dict[str, Any]], current_id: str
) -> Optional[Dict[str, str]]:
    cfg_files = get_config_files(service_def)
    ids = [str(c.get("id")) for c in cfg_files]
    cur = str(current_id)
    try:
        i = ids.index(cur)
    except ValueError:
        return None
    if i + 1 >= len(cfg_files):
        return None
    nxt = cfg_files[i + 1]
    nid = str(nxt.get("id"))
    return {"id": nid, "label": str(nxt.get("label") or nid)}


_ELIM_STAGE_LABELS: Dict[str, str] = {
    "closed": "Request is closed",
    "rolled_back": "Rolled back — re-apply database if needed",
    "published": "Published",
    "ready_for_diff": "Ready to review diff",
    "ready_for_publish": "Ready to publish",
    "generated": "Generated — review or publish when available",
    "ready_for_generate": "Ready to generate",
    "ready_for_generation_context": "Set generation context",
    "ready_for_db_preview": "Ready for DB preview",
    "needs_inputs": "Needs service inputs",
    "db_applied": "Database applied — continue to generation",
    "not_started": "Not started",
}


def _infer_elim_config_stage(
    cfg: Dict[str, Any],
    operation: Dict[str, Any],
    manual_tables: List[Dict[str, Any]],
    actions: Dict[str, bool],
    lifecycle_active: bool,
) -> str:
    """Fine-grained ELIM stage from existing operation + blueprint-derived rows (no new persistence)."""
    if not lifecycle_active:
        return "closed"
    cid = str(cfg.get("id"))
    if elim_rollback_executed(operation):
        return "rolled_back"
    if _elim_publish_done(operation, cid):
        return "published"

    has_manual = len(manual_tables) > 0
    tables_blocking = has_manual and any(bool(t.get("is_actionable")) for t in manual_tables)

    si = operation.get("service_inputs") if isinstance(operation.get("service_inputs"), dict) else {}
    has_elim_inputs = bool(si)
    ve = validate_elim_service_inputs(si) if si else []
    ve_fail = len(ve) > 0
    db_applied = elim_db_already_applied(operation)
    gen_cluster = normalize_generation_cluster(operation.get("generation_context_cluster"))
    gen_done = _elim_generation_done(operation, cid)
    gen_sup = bool(cfg.get("generation_supported"))

    if tables_blocking:
        if not has_elim_inputs or ve_fail:
            return "needs_inputs"
        if not db_applied:
            return "ready_for_db_preview"
        return "ready_for_db_preview"

    if not db_applied:
        if not has_elim_inputs or ve_fail:
            return "needs_inputs"
        return "ready_for_db_preview"

    if not gen_cluster:
        return "ready_for_generation_context"

    if gen_sup and not gen_done:
        return "ready_for_generate"

    if gen_done:
        if actions.get("can_view_diff"):
            return "ready_for_diff"
        if actions.get("can_publish"):
            return "ready_for_publish"
        return "generated"

    if actions.get("can_view_diff"):
        return "ready_for_diff"
    if actions.get("can_publish"):
        return "ready_for_publish"

    return "db_applied"


def _elim_config_stage_label(stage: str) -> str:
    return _ELIM_STAGE_LABELS.get(stage, stage.replace("_", " ").title())


def _workflow_action_link(
    *,
    key: str,
    label: str,
    url: str,
    enabled: bool,
    blocked_reason: Optional[str] = None,
    primary: bool = False,
) -> Dict[str, Any]:
    return {
        "key": key,
        "kind": "link",
        "label": label,
        "url": url,
        "enabled": enabled,
        "blocked_reason": blocked_reason,
        "primary": primary,
    }


def _workflow_action_form(
    *,
    key: str,
    label: str,
    action: str,
    enabled: bool,
    blocked_reason: Optional[str] = None,
    primary: bool = False,
) -> Dict[str, Any]:
    return {
        "key": key,
        "kind": "form_post",
        "label": label,
        "action": action,
        "enabled": enabled,
        "blocked_reason": blocked_reason,
        "primary": primary,
    }


def _build_elim_config_workflow(
    *,
    operation: Dict[str, Any],
    service_def: Optional[Dict[str, Any]],
    settings: Dict[str, Any],
    cfg: Dict[str, Any],
    manual_tables: List[Dict[str, Any]],
    actions: Dict[str, bool],
    lifecycle_active: bool,
) -> Dict[str, Any]:
    """URLs and labels for config workspace sequencing (ELIM)."""
    oid = str(operation.get("operation_id", ""))
    cid = str(cfg.get("id"))
    cfg_label = str(cfg.get("label") or cid)
    si = operation.get("service_inputs") if isinstance(operation.get("service_inputs"), dict) else {}
    ve_list = validate_elim_service_inputs(si) if si else []

    stage = _infer_elim_config_stage(
        cfg, operation, manual_tables, actions, lifecycle_active
    )
    stage_label = _elim_config_stage_label(stage)
    next_cfg = _next_config_in_blueprint_order(service_def, cid)

    blocked: List[str] = []
    if not lifecycle_active:
        blocked.append("Request is closed — inputs, database apply, generation, and publish are disabled.")
    if stage == "needs_inputs" and ve_list:
        blocked.extend(ve_list)
    if stage == "ready_for_db_preview" and not actions.get("can_preview_db") and lifecycle_active:
        if not ve_list:
            blocked.append("Save valid service inputs before opening DB preview.")

    next_action = "none"
    next_action_label = "Review sections below"
    primary: Optional[Dict[str, Any]] = None
    secondaries: List[Dict[str, Any]] = []

    def add_secondaries(entries: List[Dict[str, Any]]) -> None:
        for e in entries:
            if primary and e.get("key") == primary.get("key"):
                continue
            secondaries.append(e)

    if not lifecycle_active:
        next_action = "closed"
        next_action_label = "Request is closed"
        primary = _workflow_action_link(
            key="readiness",
            label="Check request readiness",
            url=url_for("services.operation_readiness", operation_id=oid),
            enabled=actions.get("can_readiness", False),
            primary=True,
        )
        if not primary["enabled"]:
            primary = _workflow_action_link(
                key="workspace",
                label="Return to workspace",
                url=url_for("services.operation_workspace", operation_id=oid),
                enabled=True,
                primary=True,
            )
        add_secondaries(
            [
                _workflow_action_link(
                    key="workspace",
                    label="Return to workspace",
                    url=url_for("services.operation_workspace", operation_id=oid),
                    enabled=True,
                ),
                _workflow_action_link(
                    key="operation_detail",
                    label="Operation detail",
                    url=url_for("services.operation_view", operation_id=oid),
                    enabled=True,
                ),
            ]
        )
    elif stage == "published":
        if next_cfg:
            next_action = "continue_next_config"
            nl = next_cfg["label"]
            next_action_label = f"Continue to next config: {nl}"
            primary = _workflow_action_link(
                key="continue_next_config",
                label=next_action_label,
                url=url_for(
                    "services.operation_config_workspace",
                    operation_id=oid,
                    config_id=next_cfg["id"],
                ),
                enabled=True,
                primary=True,
            )
        else:
            next_action = "check_readiness"
            next_action_label = "All configs complete — check request readiness"
            primary = _workflow_action_link(
                key="readiness",
                label=next_action_label,
                url=url_for("services.operation_readiness", operation_id=oid),
                enabled=actions.get("can_readiness", False),
                primary=True,
            )
        add_secondaries(
            [
                _workflow_action_link(
                    key="workspace",
                    label="Return to workspace",
                    url=url_for("services.operation_workspace", operation_id=oid),
                    enabled=True,
                ),
            ]
        )
    elif stage == "rolled_back":
        next_action = "db_preview"
        next_action_label = "Preview DB changes"
        pe = actions.get("can_preview_db", False)
        primary = _workflow_action_link(
            key="db_preview",
            label="Preview DB changes",
            url=url_for("services.operation_db_preview", operation_id=oid),
            enabled=pe,
            blocked_reason=None if pe else "Save service inputs first",
            primary=True,
        )
        add_secondaries(
            [
                _workflow_action_link(
                    key="enter_inputs",
                    label="Enter service inputs",
                    url=url_for("services.operation_inputs", operation_id=oid),
                    enabled=actions.get("can_enter_inputs", False),
                ),
                _workflow_action_link(
                    key="rollback",
                    label="Rollback changes",
                    url=url_for("services.operation_rollback_preview", operation_id=oid),
                    enabled=actions.get("can_rollback", False),
                ),
            ]
        )
    elif stage == "ready_for_diff":
        next_action = "view_diff"
        next_action_label = "View diff"
        ce = actions.get("can_view_diff", False)
        primary = _workflow_action_link(
            key="view_diff",
            label="View diff",
            url=url_for(
                "services.operation_config_diff",
                operation_id=oid,
                config_type=cid,
            ),
            enabled=ce,
            blocked_reason=None if ce else "Diff not available yet",
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    elif stage == "ready_for_publish":
        next_action = "publish"
        next_action_label = "Publish config"
        pe = actions.get("can_publish", False)
        primary = _workflow_action_form(
            key="publish",
            label="Publish config",
            action=url_for(
                "services.operation_publish_config",
                operation_id=oid,
                config_type=cid,
            ),
            enabled=pe,
            blocked_reason=None if pe else "Publish not available yet",
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    elif stage == "ready_for_generate":
        next_action = "generate"
        next_action_label = "Generate config"
        ge = actions.get("can_generate", False)
        primary = _workflow_action_form(
            key="generate",
            label="Generate config",
            action=url_for(
                "services.operation_generate_config",
                operation_id=oid,
                config_type=cid,
            ),
            enabled=ge,
            blocked_reason=None if ge else "Set generation context and ensure DB apply is recorded",
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    elif stage == "ready_for_generation_context":
        next_action = "set_generation_context"
        next_action_label = "Set generation context"
        ce = actions.get("can_generation_context", False)
        primary = _workflow_action_link(
            key="generation_context",
            label="Set generation context",
            url=url_for("services.operation_generation_context", operation_id=oid),
            enabled=ce,
            blocked_reason=None if ce else "Complete database apply first",
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    elif stage == "ready_for_db_preview":
        next_action = "db_preview"
        next_action_label = "Preview DB changes"
        pe = actions.get("can_preview_db", False)
        primary = _workflow_action_link(
            key="db_preview",
            label="Preview DB changes",
            url=url_for("services.operation_db_preview", operation_id=oid),
            enabled=pe,
            blocked_reason=None if pe else "Save valid service inputs first",
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    elif stage == "needs_inputs":
        next_action = "enter_inputs"
        next_action_label = "Enter service inputs"
        ie = actions.get("can_enter_inputs", False)
        primary = _workflow_action_link(
            key="enter_inputs",
            label="Enter service inputs",
            url=url_for("services.operation_inputs", operation_id=oid),
            enabled=ie,
            blocked_reason=None if ie else "No guided input form is configured for this service",
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    elif stage in ("generated", "db_applied"):
        next_action = "readiness"
        next_action_label = "Check request readiness"
        primary = _workflow_action_link(
            key="readiness",
            label="Check request readiness",
            url=url_for("services.operation_readiness", operation_id=oid),
            enabled=actions.get("can_readiness", False),
            primary=True,
        )
        add_secondaries(_elim_workflow_extras(operation, oid, cid, actions, primary))
    else:
        next_action = "workspace"
        next_action_label = "Return to workspace"
        primary = _workflow_action_link(
            key="workspace",
            label="Return to workspace",
            url=url_for("services.operation_workspace", operation_id=oid),
            enabled=True,
            primary=True,
        )

    available = _elim_available_actions_catalog(
        operation_id=oid,
        config_id=cid,
        actions=actions,
        lifecycle_active=lifecycle_active,
        primary_key=primary.get("key") if primary else None,
    )

    return {
        "is_elim": True,
        "lifecycle_active": lifecycle_active,
        "current_stage": stage,
        "current_stage_label": stage_label,
        "next_action": next_action,
        "next_action_label": next_action_label,
        "blocked_reasons": blocked,
        "primary": primary,
        "secondary_actions": secondaries,
        "available_actions": available,
        "next_config": next_cfg,
        "all_configs_complete_message": (
            "All configs complete — check request readiness"
            if stage == "published" and not next_cfg
            else None
        ),
    }


def _elim_workflow_extras(
    operation: Dict[str, Any],
    operation_id: str,
    config_id: str,
    actions: Dict[str, bool],
    primary: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Non-primary ELIM links (forms omitted here; Section C still exposes them)."""
    _ = operation
    pk = primary.get("key") if primary else None
    out: List[Dict[str, Any]] = []
    if pk != "workspace":
        out.append(
            _workflow_action_link(
                key="workspace",
                label="Return to workspace",
                url=url_for("services.operation_workspace", operation_id=operation_id),
                enabled=True,
            )
        )
    if actions.get("can_readiness") and pk != "readiness":
        out.append(
            _workflow_action_link(
                key="readiness",
                label="Check request readiness",
                url=url_for("services.operation_readiness", operation_id=operation_id),
                enabled=True,
            )
        )
    if actions.get("can_enter_inputs") and pk != "enter_inputs":
        out.append(
            _workflow_action_link(
                key="enter_inputs",
                label="Enter service inputs",
                url=url_for("services.operation_inputs", operation_id=operation_id),
                enabled=True,
            )
        )
    if actions.get("can_preview_db") and pk != "db_preview":
        out.append(
            _workflow_action_link(
                key="db_preview",
                label="Preview DB changes",
                url=url_for("services.operation_db_preview", operation_id=operation_id),
                enabled=True,
            )
        )
    if actions.get("can_generation_context") and pk != "generation_context":
        out.append(
            _workflow_action_link(
                key="generation_context",
                label="Set generation context",
                url=url_for("services.operation_generation_context", operation_id=operation_id),
                enabled=True,
            )
        )
    if actions.get("can_view_diff") and pk != "view_diff":
        out.append(
            _workflow_action_link(
                key="view_diff",
                label="View diff",
                url=url_for(
                    "services.operation_config_diff",
                    operation_id=operation_id,
                    config_type=config_id,
                ),
                enabled=True,
            )
        )
    return out


def _elim_available_actions_catalog(
    *,
    operation_id: str,
    config_id: str,
    actions: Dict[str, bool],
    lifecycle_active: bool,
    primary_key: Optional[str],
) -> List[Dict[str, Any]]:
    """Flat list for templates: enabled + optional blocked_reason."""
    rows: List[Dict[str, Any]] = []

    def add(
        key: str,
        label: str,
        kind: str,
        url: Optional[str] = None,
        form_action: Optional[str] = None,
    ) -> None:
        enabled = False
        reason: Optional[str] = None
        if key == "enter_inputs":
            enabled = bool(actions.get("can_enter_inputs")) and lifecycle_active
            if not actions.get("can_enter_inputs"):
                reason = "No guided inputs configured"
            elif not lifecycle_active:
                reason = "Request is closed"
        elif key == "db_preview":
            enabled = bool(actions.get("can_preview_db")) and lifecycle_active
            if not actions.get("can_preview_db"):
                reason = "Inputs missing or invalid for DB preview"
            elif not lifecycle_active:
                reason = "Request is closed"
        elif key == "generation_context":
            enabled = bool(actions.get("can_generation_context")) and lifecycle_active
            if not actions.get("can_generation_context"):
                reason = "Database apply not recorded yet"
            elif not lifecycle_active:
                reason = "Request is closed"
        elif key == "generate":
            enabled = bool(actions.get("can_generate")) and lifecycle_active
            if not actions.get("can_generate"):
                reason = "Set generation context and ensure prerequisites are met"
            elif not lifecycle_active:
                reason = "Request is closed"
        elif key == "view_diff":
            enabled = bool(actions.get("can_view_diff"))
            if not actions.get("can_view_diff"):
                reason = "Generate first or wait for generation context"
        elif key == "publish":
            enabled = bool(actions.get("can_publish")) and lifecycle_active
            if not actions.get("can_publish"):
                reason = "Generate and review diff before publish"
            elif not lifecycle_active:
                reason = "Request is closed"
        elif key == "rollback":
            enabled = bool(actions.get("can_rollback")) and lifecycle_active
            if not lifecycle_active:
                reason = "Request is closed"
            elif not actions.get("can_rollback"):
                reason = "Rollback is not available for this request state"
        elif key == "readiness":
            enabled = bool(actions.get("can_readiness"))
            if not enabled:
                reason = "Readiness is only available for ELIM requests"
        rows.append(
            {
                "key": key,
                "label": label,
                "kind": kind,
                "url": url,
                "form_action": form_action,
                "enabled": enabled,
                "blocked_reason": None if enabled else reason,
                "is_primary": key == primary_key,
            }
        )

    add(
        "enter_inputs",
        "Enter service inputs",
        "link",
        url=url_for("services.operation_inputs", operation_id=operation_id),
    )
    add(
        "db_preview",
        "Preview DB changes",
        "link",
        url=url_for("services.operation_db_preview", operation_id=operation_id),
    )
    add(
        "generation_context",
        "Set generation context",
        "link",
        url=url_for("services.operation_generation_context", operation_id=operation_id),
    )
    add(
        "generate",
        "Generate config",
        "form_post",
        form_action=url_for(
            "services.operation_generate_config",
            operation_id=operation_id,
            config_type=config_id,
        ),
    )
    add(
        "view_diff",
        "View diff",
        "link",
        url=url_for(
            "services.operation_config_diff",
            operation_id=operation_id,
            config_type=config_id,
        ),
    )
    add(
        "publish",
        "Publish config",
        "form_post",
        form_action=url_for(
            "services.operation_publish_config",
            operation_id=operation_id,
            config_type=config_id,
        ),
    )
    add(
        "rollback",
        "Rollback changes",
        "link",
        url=url_for("services.operation_rollback_preview", operation_id=operation_id),
    )
    add(
        "readiness",
        "Check request readiness",
        "link",
        url=url_for("services.operation_readiness", operation_id=operation_id),
    )
    return rows


def _neutral_config_workflow(
    *,
    operation: Dict[str, Any],
    service_def: Optional[Dict[str, Any]],
    cfg: Dict[str, Any],
    actions: Dict[str, bool],
    lifecycle_active: bool,
) -> Dict[str, Any]:
    oid = str(operation.get("operation_id", ""))
    cid = str(cfg.get("id"))
    stage = "closed" if not lifecycle_active else "not_started"
    stage_label = _elim_config_stage_label(stage) if stage == "closed" else "Use blueprint actions below"
    blocked: List[str] = []
    if not lifecycle_active:
        blocked.append("Request is closed.")
    primary: Optional[Dict[str, Any]] = None
    if actions.get("can_enter_inputs") and lifecycle_active:
        primary = _workflow_action_link(
            key="enter_inputs",
            label="Enter service inputs",
            url=url_for("services.operation_inputs", operation_id=oid),
            enabled=True,
            primary=True,
        )
    else:
        primary = _workflow_action_link(
            key="workspace",
            label="Return to workspace",
            url=url_for("services.operation_workspace", operation_id=oid),
            enabled=True,
            primary=True,
        )
    sec: List[Dict[str, Any]] = []
    if primary.get("key") != "workspace":
        sec.append(
            _workflow_action_link(
                key="workspace",
                label="Return to workspace",
                url=url_for("services.operation_workspace", operation_id=oid),
                enabled=True,
            )
        )
    sec.append(
        _workflow_action_link(
            key="operation_detail",
            label="Operation detail",
            url=url_for("services.operation_view", operation_id=oid),
            enabled=True,
        )
    )
    return {
        "is_elim": False,
        "lifecycle_active": lifecycle_active,
        "current_stage": stage,
        "current_stage_label": stage_label,
        "next_action": "enter_inputs" if primary.get("key") == "enter_inputs" else "workspace",
        "next_action_label": primary["label"],
        "blocked_reasons": blocked,
        "primary": primary,
        "secondary_actions": sec,
        "available_actions": [],
        "next_config": _next_config_in_blueprint_order(service_def, cid),
        "all_configs_complete_message": None,
    }


def infer_config_stage(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    config_id: str,
    settings: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Infer a coarse workflow stage for one blueprint config (ELIM uses rich stages; others ``not_started``/``closed``).
    """
    st = settings if isinstance(settings, dict) else {}
    cfg = get_config_by_id(service_def, config_id)
    if not cfg:
        return "not_started"
    lifecycle_active = effective_lifecycle_status(operation) == "active"
    if not lifecycle_active:
        return "closed"
    if not is_license_resource_workspace_operation(operation):
        return "not_started"
    manual_tables = _collect_manual_table_rows_for_config(service_def, operation, cfg)
    actions = _infer_elim_action_flags(
        operation, st, str(cfg.get("id")), cfg, service_def
    )
    return _infer_elim_config_stage(
        cfg, operation, manual_tables, actions, lifecycle_active
    )


def build_config_stage_actions(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    config_id: str,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build sequencing metadata for a config: stage, next step, blocked reasons, and action descriptors.
    """
    st = settings if isinstance(settings, dict) else {}
    cfg = get_config_by_id(service_def, config_id)
    if not cfg:
        return {
            "is_elim": False,
            "lifecycle_active": False,
            "current_stage": "not_started",
            "current_stage_label": "Unknown config",
            "next_action": "none",
            "next_action_label": "",
            "blocked_reasons": [],
            "primary": None,
            "secondary_actions": [],
            "available_actions": [],
            "next_config": None,
            "all_configs_complete_message": None,
        }
    lifecycle_active = effective_lifecycle_status(operation) == "active"
    manual_tables = _collect_manual_table_rows_for_config(service_def, operation, cfg)
    if is_license_resource_workspace_operation(operation):
        actions = _infer_elim_action_flags(
            operation, st, str(cfg.get("id")), cfg, service_def
        )
        return _build_elim_config_workflow(
            operation=operation,
            service_def=service_def,
            settings=st,
            cfg=cfg,
            manual_tables=manual_tables,
            actions=actions,
            lifecycle_active=lifecycle_active,
        )
    actions = _neutral_action_flags(service_def)
    return _neutral_config_workflow(
        operation=operation,
        service_def=service_def,
        cfg=cfg,
        actions=actions,
        lifecycle_active=lifecycle_active,
    )


def _build_elim_table_workflow_sequencing(
    *,
    lifecycle_active: bool,
    table_row: Dict[str, Any],
    elim_table_apply: Dict[str, Any],
    has_elim_inputs: bool,
    next_hints: Dict[str, Any],
    next_dependency_table: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Table workspace: state line + blocked reasons + satisfied copy (ELIM-oriented)."""
    st = str(table_row.get("status") or "")
    reused = bool(table_row.get("reused") or st == "reused")
    is_satisfied = st in ("applied", "reused") or reused
    is_actionable = _manual_table_is_actionable(table_row)
    state_label = _elim_config_stage_label("not_started")
    if st == "applied":
        state_label = "Applied for this request"
    elif st == "reused":
        state_label = "Reused — already satisfied"
    elif st == "rolled_back":
        state_label = "Rolled back"
    elif st == "not_started":
        state_label = "Not applied yet"

    blocked: List[str] = []
    satisfied_message: Optional[str] = None
    recommended = str(next_hints.get("lead") or "")

    if not lifecycle_active:
        blocked.append("Request is closed — table-scoped apply and input saves are disabled.")
        recommended = "No execution actions — use readiness or operation detail if you need audit context."
    elif is_satisfied and not is_actionable:
        satisfied_message = (
            "No further input needed for this table in this request — the dependency is already satisfied."
        )
        if next_dependency_table:
            nl = next_dependency_table.get("label") or next_dependency_table.get("id")
            recommended = (
                f"Continue table-by-table: open {nl} from the config workspace for this config, "
                "then preview and apply that insert when ready."
            )
        else:
            recommended = (
                "All manual tables for this config are satisfied — return to the config workspace "
                "for generation and downstream steps."
            )
    else:
        ve = elim_table_apply.get("validation_errors") if isinstance(elim_table_apply, dict) else []
        if isinstance(ve, list) and ve:
            blocked.extend(str(x) for x in ve)
        dep = elim_table_apply.get("dependency_message") if isinstance(elim_table_apply, dict) else None
        if dep:
            blocked.append(str(dep))
        if is_actionable and not has_elim_inputs:
            recommended = (
                "Use Insert operation below to save these fields to the request, then open "
                "Preview DB changes for this table (Section DB)."
            )
        elif is_actionable and has_elim_inputs and elim_table_apply.get("can_table_preview"):
            recommended = "Preview DB changes for this table, then apply from the preview page when ready."
        elif is_actionable and has_elim_inputs and blocked:
            recommended = "Resolve the blocked items above, then preview DB changes for this table."

    emphasize_flow = bool(
        lifecycle_active and is_actionable and not is_satisfied and not blocked
    )

    return {
        "lifecycle_active": lifecycle_active,
        "current_state_label": state_label,
        "recommended_next_label": recommended,
        "blocked_reasons": blocked,
        "satisfied_complete_message": satisfied_message,
        "emphasize_save_then_preview": emphasize_flow,
    }


def _infer_elim_action_flags(
    operation: Dict[str, Any],
    settings: Dict[str, Any],
    config_id: str,
    cfg_entry: Dict[str, Any],
    service_def: Optional[Dict[str, Any]],
) -> Dict[str, bool]:
    """
    Map to existing ELIM routes; booleans mirror operation_view / workspace heuristics.

    ELIM/LS license-resource fields are False when the operation is not ELIM or LS.
    """
    si = operation.get("service_inputs") if isinstance(operation.get("service_inputs"), dict) else {}
    has_elim_inputs = bool(si)
    sr = operation.get("step_results") if isinstance(operation.get("step_results"), dict) else {}
    is_elim = is_license_resource_workspace_operation(operation)
    has_db_apply = isinstance(sr.get("db_apply"), list) and len(sr.get("db_apply")) > 0
    elim_config_enabled = is_elim and has_db_apply
    gen_cluster = (
        normalize_generation_cluster(operation.get("generation_context_cluster"))
        if is_elim
        else None
    )
    erow: Dict[str, Any] = {}
    if is_elim:
        rows = elim_config_ui_rows(settings, sr, elim_config_enabled, gen_cluster)
        erow = next((r for r in rows if str(r.get("config_type")) == config_id), {})

    gen_sup = bool(cfg_entry.get("generation_supported"))
    diff_sup = bool(cfg_entry.get("diff_supported"))
    pub_sup = bool(cfg_entry.get("publish_supported"))

    return {
        "can_enter_inputs": service_has_guided_inputs(service_def),
        "can_preview_db": is_elim and has_elim_inputs,
        "can_apply_db": is_elim and has_elim_inputs and not elim_db_already_applied(operation),
        "can_generation_context": elim_config_enabled,
        "can_generate": is_elim and gen_sup and bool(erow.get("can_generate")),
        "can_view_diff": is_elim and diff_sup and bool(erow.get("can_diff")),
        "can_publish": is_elim and pub_sup and bool(erow.get("can_publish")),
        "can_rollback": is_elim and elim_can_rollback(operation),
        "can_readiness": is_elim,
    }


def _neutral_action_flags(service_def: Optional[Dict[str, Any]]) -> Dict[str, bool]:
    return {
        "can_enter_inputs": service_has_guided_inputs(service_def),
        "can_preview_db": False,
        "can_apply_db": False,
        "can_generation_context": False,
        "can_generate": False,
        "can_view_diff": False,
        "can_publish": False,
        "can_rollback": False,
        "can_readiness": False,
    }


def _config_state_snippets(
    operation: Dict[str, Any], config_id: str
) -> Dict[str, Any]:
    """Small summaries from ``step_results`` for one config file id."""
    sr = operation.get("step_results") if isinstance(operation.get("step_results"), dict) else {}
    db_apply = sr.get("db_apply")
    db_apply_summary = db_apply if isinstance(db_apply, list) else []
    gen_map = normalize_generation_state(sr.get("generation"))
    pub_map = normalize_publish_state(sr.get("publish"))
    gen_ent = gen_map.get(config_id) if isinstance(gen_map.get(config_id), dict) else None
    pub_ent = pub_map.get(config_id) if isinstance(pub_map.get(config_id), dict) else None
    return {
        "db_apply_steps": db_apply_summary,
        "generation": gen_ent,
        "publish": pub_ent,
    }


def build_config_workspace_view_model(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    settings: Dict[str, Any],
    config_id: str,
) -> Optional[Dict[str, Any]]:
    """
    View model for ``config_workspace.html``: one blueprint config file + deps + actions.

    Returns ``None`` if the service has no blueprint or ``config_id`` is not declared.
    """
    rb = get_request_blueprint(service_def)
    cfg_files = get_config_files(service_def)
    if not rb or not cfg_files:
        return None
    cfg = get_config_by_id(service_def, config_id)
    if not cfg:
        return None

    cid = str(cfg.get("id"))
    use_elim_inference = is_license_resource_workspace_operation(operation)
    if use_elim_inference:
        table_states = _elim_workspace_table_states(operation)
    else:
        table_states = {k: "not_started" for k in get_table_defs(service_def).keys()}

    dep_ids = [str(x) for x in (cfg.get("depends_on_tables") or [])]
    has_manual_tables = len(dep_ids) > 0
    manual_tables = _collect_manual_table_rows_for_config(service_def, operation, cfg)

    if use_elim_inference:
        cstatus = _infer_elim_config_status(cfg, operation, table_states)
    else:
        cstatus = "not_started"

    gen_only_note = None
    generation_only = not has_manual_tables
    if generation_only:
        gen_only_note = str(cfg.get("workspace_generation_note") or "").strip() or (
            "This config is generation-only for this request type: no manual DB table "
            "row is tied to this artifact. Use generate, diff, and publish when the "
            "request is ready."
            if bool(cfg.get("derived_only"))
            else (
                "No manual DB table dependencies are declared for this config. "
                "Use generate, diff, and publish when ready."
            )
        )

    if use_elim_inference:
        actions = _infer_elim_action_flags(operation, settings, cid, cfg, service_def)
    else:
        actions = _neutral_action_flags(service_def)

    lifecycle_active = effective_lifecycle_status(operation) == "active"
    if use_elim_inference:
        workflow = _build_elim_config_workflow(
            operation=operation,
            service_def=service_def,
            settings=settings,
            cfg=cfg,
            manual_tables=manual_tables,
            actions=actions,
            lifecycle_active=lifecycle_active,
        )
    else:
        workflow = _neutral_config_workflow(
            operation=operation,
            service_def=service_def,
            cfg=cfg,
            actions=actions,
            lifecycle_active=lifecycle_active,
        )

    return {
        "config": {
            "id": cid,
            "label": str(cfg.get("label") or cid),
            "short_label": str(cfg.get("short_label") or cfg.get("label") or cid),
            "status": cstatus,
            "css_class": _status_css_workspace("config", cstatus),
            "generation_supported": bool(cfg.get("generation_supported")),
            "diff_supported": bool(cfg.get("diff_supported")),
            "publish_supported": bool(cfg.get("publish_supported")),
            "required_for_finalization": bool(cfg.get("required_for_finalization")),
            "derived_only": bool(cfg.get("derived_only")),
            "has_manual_tables": has_manual_tables,
            "generation_only": generation_only,
            "generation_only_note": gen_only_note,
        },
        "manual_tables": manual_tables,
        "actions": actions,
        "state": _config_state_snippets(operation, cid),
        "show_elim_actions": use_elim_inference,
        "workflow": workflow,
    }


# Map blueprint table id -> ELIM db_apply step name (orchestration hints only).
_ELIM_TABLE_APPLY_STEPS: Dict[str, str] = {
    "license_servers": "insert_license_server",
    "resources_REF": "insert_resource",
}


def _prefill_value_to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def build_table_default_values(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    db_adapter: Optional[Any],
    table_id: str,
) -> Dict[str, str]:
    """
    Blueprint-driven defaults for table workspace inputs (string values for HTML).

    * ``prefill_mode == "sample_row"``: first JSON DB row, mapped onto guided field keys
      (by ``column`` / ``key``); ``ID`` is skipped.
    * ``tables.<id>.defaults``: static dict; keys are guided field keys; **overrides** sample.
    """
    _ = operation
    out: Dict[str, str] = {}
    if not isinstance(service_def, dict):
        return out
    tmap = get_table_defs(service_def)
    tid = str(table_id or "").strip()
    if tid not in tmap:
        return out
    table_def = tmap[tid]
    tname = str(table_def.get("table_name") or tid).strip()
    prefill = str(table_def.get("prefill_mode") or "").strip()
    fields_raw = collect_guided_fields_for_table(service_def, table_def, tid)
    skip_cols = {"ID", "id"}

    if prefill == "sample_row" and db_adapter is not None:
        getter = getattr(db_adapter, "get_sample_row", None)
        if callable(getter):
            try:
                sample = getter(tname)
            except (TypeError, ValueError, OSError, json.JSONDecodeError):
                sample = None
            if isinstance(sample, dict):
                for f in fields_raw:
                    fk = str(f.get("key") or "")
                    col = str(f.get("column") or fk).strip()
                    if not fk or col in skip_cols:
                        continue
                    if col in sample and col not in skip_cols:
                        out[fk] = _prefill_value_to_str(sample[col])

    defaults = table_def.get("defaults")
    if isinstance(defaults, dict):
        for k, v in defaults.items():
            ks = str(k).strip()
            if not ks or ks in skip_cols:
                continue
            out[ks] = _prefill_value_to_str(v)
    return out


def collect_guided_fields_for_table(
    service_def: Optional[Dict[str, Any]],
    table_def: Dict[str, Any],
    table_id: str,
) -> List[Dict[str, Any]]:
    """
    Fields for a blueprint table: match ``guided_inputs`` by ``table`` column
    or by ``guided_group`` on the table definition.
    """
    if not isinstance(service_def, dict):
        return []
    tname = str(table_def.get("table_name") or table_id).strip()
    ggroup = str(table_def.get("guided_group") or "").strip()
    guided = iter_guided_fields(service_def)
    picked: List[Dict[str, Any]] = []
    for f in guided:
        fk_table = str(f.get("table") or "").strip()
        fk_group = str(f.get("group") or "").strip()
        if tname and fk_table == tname:
            picked.append(f)
        elif ggroup and fk_group == ggroup:
            picked.append(f)
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for f in picked:
        k = f.get("key")
        if k and k not in seen:
            seen.add(k)
            out.append(f)
    return out


def guided_field_display_rows(fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize guided field dicts for read-only table workspace display."""
    rows: List[Dict[str, Any]] = []
    for f in fields:
        hm = f.get("help_mode")
        help_note = ""
        if hm:
            help_note = f"help: {hm}"
            if f.get("help_table"):
                help_note += f" ({f.get('help_table')})"
        hm_raw = f.get("help_mode")
        hm_str = str(hm_raw).strip() if hm_raw else ""
        rows.append(
            {
                "key": str(f.get("key") or ""),
                "label": str(f.get("label") or f.get("key") or ""),
                "required": bool(f.get("required")),
                "type": str(f.get("type") or ""),
                "column": str(f.get("column") or ""),
                "help_note": help_note.strip(),
                "help_mode": hm_str or None,
                "placeholder": str(f.get("placeholder") or "").strip() or None,
            }
        )
    return rows


def _elim_db_apply_snapshot_for_table(
    operation: Dict[str, Any], table_id: str
) -> Optional[Dict[str, Any]]:
    """Return matching done db_apply step with inserted_id if present (ELIM)."""
    step_name = _ELIM_TABLE_APPLY_STEPS.get(table_id)
    if not step_name:
        return None
    sr = operation.get("step_results") if isinstance(operation.get("step_results"), dict) else {}
    steps = sr.get("db_apply")
    if not isinstance(steps, list):
        return None
    for s in steps:
        if not isinstance(s, dict):
            continue
        if s.get("step") == step_name and s.get("status") == "done":
            return {
                "step": step_name,
                "status": s.get("status"),
                "inserted_id": s.get("inserted_id"),
            }
    return None


def _infer_recommended_next_action(
    *,
    use_elim: bool,
    row: Dict[str, Any],
    has_elim_inputs: bool,
    elim_db_applied: bool,
) -> str:
    """
    One of: fill_inputs | review_db_preview | already_satisfied | rolled_back_needs_reapply
    """
    st = row.get("status")
    reused = bool(row.get("reused") or st == "reused")
    if st == "rolled_back":
        return "rolled_back_needs_reapply"
    if st in ("applied", "reused") or reused:
        return "already_satisfied"
    if not use_elim:
        return "fill_inputs"
    if not has_elim_inputs:
        return "fill_inputs"
    if not elim_db_applied:
        return "review_db_preview"
    return "review_db_preview"


def _next_action_ui_hints(action_key: str) -> Dict[str, Any]:
    """Copy for table workspace Section E."""
    leads = {
        "fill_inputs": "Review fields below, then use Insert operation to persist them for this request.",
        "review_db_preview": "Inputs are saved — open DB preview to validate and apply inserts for this request.",
        "already_satisfied": "No new database entry is required for this table in the current request state.",
        "rolled_back_needs_reapply": "This insert was rolled back — update inputs if needed, then re-apply from DB preview.",
    }
    k = action_key if action_key in leads else "fill_inputs"
    return {
        "key": k,
        "lead": leads[k],
        "emphasize_inputs": k in ("fill_inputs", "rolled_back_needs_reapply"),
        "emphasize_preview": k == "review_db_preview",
        "show_satisfied_banner": k == "already_satisfied",
    }


def build_table_workspace_view_model(
    service_def: Optional[Dict[str, Any]],
    operation: Dict[str, Any],
    settings: Dict[str, Any],
    config_id: str,
    table_id: str,
    db_adapter: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """
    View model for ``table_workspace.html``: one manual table in context of a config.

    Returns ``None`` if blueprint/config/table is invalid or the table is not listed
    under the config's ``depends_on_tables``.
    """
    _ = settings  # reserved for future parity with other workspace builders
    rb = get_request_blueprint(service_def)
    cfg_files = get_config_files(service_def)
    if not rb or not cfg_files:
        return None
    cfg = get_config_by_id(service_def, config_id)
    tid = str(table_id or "").strip()
    if not cfg or tid not in [str(x) for x in (cfg.get("depends_on_tables") or [])]:
        return None
    tmap = get_table_defs(service_def)
    if tid not in tmap:
        return None

    table_def = tmap[tid]
    cid = str(cfg.get("id"))
    dep_ids = [str(x) for x in (cfg.get("depends_on_tables") or [])]
    ordered_dep_tables: List[Dict[str, str]] = []
    for did in dep_ids:
        if did in tmap:
            ordered_dep_tables.append(
                {"id": did, "label": str(tmap[did].get("label") or did)}
            )
    next_dep: Optional[Dict[str, str]] = None
    try:
        pos = dep_ids.index(tid)
        if pos + 1 < len(dep_ids):
            nid = dep_ids[pos + 1]
            if nid in tmap:
                next_dep = {
                    "id": nid,
                    "label": str(tmap[nid].get("label") or nid),
                }
    except ValueError:
        pass
    table_sequence = {
        "ordered_tables": ordered_dep_tables,
        "current_table_id": tid,
        "next_table": next_dep,
    }
    use_elim_inference = is_license_resource_workspace_operation(operation)
    if use_elim_inference:
        table_states = _elim_workspace_table_states(operation)
        raw_row = _table_row_for_config(tid, table_def, cid, cfg_files, table_states)
    else:
        raw_row = {
            "table_id": tid,
            "label": str(table_def.get("label") or tid),
            "table_name": str(table_def.get("table_name") or tid),
            "status": "not_started",
            "css_class": _status_css_workspace("table", "not_started"),
            "reused": False,
            "satisfied_note": None,
            "reuse_within_request": bool(table_def.get("reuse_within_request")),
        }

    st = raw_row.get("status")
    reused_flag = bool(raw_row.get("reused") or st == "reused")
    is_satisfied = st in ("applied", "reused") or reused_flag
    is_actionable = _manual_table_is_actionable(raw_row)

    si = operation.get("service_inputs") if isinstance(operation.get("service_inputs"), dict) else {}
    has_elim_inputs = bool(si) if use_elim_inference else bool(si)
    elim_db_applied = elim_db_already_applied(operation) if use_elim_inference else False

    action_key = _infer_recommended_next_action(
        use_elim=use_elim_inference,
        row=raw_row,
        has_elim_inputs=has_elim_inputs,
        elim_db_applied=elim_db_applied,
    )
    next_hints = _next_action_ui_hints(action_key)

    earliest = _earliest_config_for_table(cfg_files, tid)
    owner_cid = earliest or cid
    reuse_blurb = None
    if earliest and earliest != cid and (reused_flag or st == "reused"):
        owner_entry = get_config_by_id(service_def, earliest)
        owner_label = str((owner_entry or {}).get("label") or earliest)
        reuse_blurb = (
            f"This table was already satisfied earlier in the request (first required "
            f"under config “{owner_label}” / {earliest}). "
            f"No additional row is needed for config {cid}."
        )
    elif earliest == cid and st == "applied":
        reuse_blurb = (
            "This config is the first in the blueprint that requires this table; "
            "the insert applies to the whole request."
        )

    note = _manual_table_dependency_note(raw_row)
    fields_raw = collect_guided_fields_for_table(service_def, table_def, tid)
    field_rows = guided_field_display_rows(fields_raw)

    apply_snapshot = None
    if use_elim_inference:
        apply_snapshot = _elim_db_apply_snapshot_for_table(operation, tid)

    show_elim = use_elim_inference
    ve_list = validate_elim_service_inputs(si) if show_elim else []
    ve_fail = bool(ve_list)
    elim_table_apply: Dict[str, Any] = {"show": False}
    if show_elim and tid in (TABLE_LICENSE_SERVERS, TABLE_RESOURCES_REF):
        ls_done = elim_db_step_done(operation, "insert_license_server")
        rr_done = elim_db_step_done(operation, "insert_resource")
        if tid == TABLE_LICENSE_SERVERS:
            dep_msg = None
            this_applied = ls_done
            can_preview = has_elim_inputs and not ls_done and not ve_fail
        else:
            dep_msg = None if ls_done else "Requires license_servers to be applied first."
            this_applied = rr_done
            can_preview = (
                has_elim_inputs
                and ls_done
                and not rr_done
                and not ve_fail
                and dep_msg is None
            )
        elim_table_apply = {
            "show": True,
            "ls_done": ls_done,
            "rr_done": rr_done,
            "this_table_applied": this_applied,
            "can_table_preview": can_preview,
            "dependency_message": dep_msg,
            "validation_errors": ve_list,
        }

    default_values = build_table_default_values(
        service_def, operation, db_adapter, tid
    )
    this_applied_flag = bool(elim_table_apply.get("this_table_applied")) if show_elim else False
    show_table_input_form = (
        bool(field_rows)
        and service_has_guided_inputs(service_def)
        and not this_applied_flag
    )

    lc_active = effective_lifecycle_status(operation) == "active"
    table_workflow = _build_elim_table_workflow_sequencing(
        lifecycle_active=lc_active,
        table_row=raw_row,
        elim_table_apply=elim_table_apply,
        has_elim_inputs=has_elim_inputs,
        next_hints=next_hints,
        next_dependency_table=next_dep,
    )

    return {
        "table": {
            "id": tid,
            "label": str(table_def.get("label") or tid),
            "table_name": str(table_def.get("table_name") or tid),
            "css_class": raw_row.get("css_class"),
            "status": st,
            "note": note,
            "is_reused": reused_flag,
            "is_satisfied": is_satisfied,
            "is_actionable": is_actionable,
        },
        "parent_config": {
            "id": cid,
            "label": str(cfg.get("label") or cid),
        },
        "reuse_context": {
            "owner_config_id": owner_cid,
            "viewing_config_id": cid,
            "blurb": reuse_blurb,
        },
        "fields": field_rows,
        "field_hints": str(table_def.get("field_hints") or "").strip() or None,
        "apply_snapshot": apply_snapshot,
        "recommended_next_action": action_key,
        "next": next_hints,
        "show_elim_actions": show_elim,
        "elim_table_apply": elim_table_apply,
        "links": {
            "can_enter_inputs": service_has_guided_inputs(service_def),
            "can_preview_db": show_elim and has_elim_inputs,
        },
        "default_values": default_values,
        "show_table_input_form": show_table_input_form,
        "table_workflow": table_workflow,
        "lifecycle_execution_enabled": lc_active,
        "table_sequence": table_sequence,
    }
