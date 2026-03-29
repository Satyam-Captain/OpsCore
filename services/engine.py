"""
Minimal service engine: create operation, run request-dir check, set initial status.

Full step orchestration is intentionally not implemented yet.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from services.adapters import build_adapters
from services.state_store import create_operation


def start_service_operation(
    settings: Dict[str, Any],
    service_def: Dict[str, Any],
    request_number: str,
    extra_inputs: Optional[Dict[str, Any]] = None,
    workspace_control: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Create persisted operation state and perform initial request-directory check.

    The operation is **request-scoped**: only ``request_number`` is required at
    start. Deployment target cluster(s) are out of scope until a later step.

    Returns (operation_document, engine_meta) where engine_meta is for debugging/UI.
    """
    service_id = service_def["id"]
    _, request_adapter, _ = build_adapters(settings)

    req_clean = (request_number or "").strip()
    inputs: Dict[str, Any] = {"request_number": req_clean}
    if extra_inputs:
        inputs.update(extra_inputs)

    request_path = request_adapter.get_request_path(req_clean)
    exists = request_adapter.request_exists(req_clean)
    require_dir = bool(settings.get("service_require_request_dir", True))
    allow_bypass = bool(settings.get("service_allow_request_bypass", True))

    step_payload = {
        "request_path": request_path,
        "exists": exists,
        "require_request_dir": require_dir,
        "allow_request_bypass": allow_bypass,
    }

    if exists:
        status = "created"
    elif require_dir and not allow_bypass:
        status = "blocked_request_missing"
    else:
        status = "created"

    first_step = None
    steps = service_def.get("steps") or []
    if isinstance(steps, list) and steps:
        first = steps[0]
        if isinstance(first, str):
            first_step = first
        elif isinstance(first, dict) and "id" in first:
            first_step = str(first["id"])

    current_step = first_step
    if status == "blocked_request_missing":
        current_step = "request_check"

    wc = workspace_control
    if wc is None:
        wc = {
            "role_at_creation": "admin",
            "is_superadmin_override": False,
            "override_reason": None,
        }

    doc = create_operation(
        settings,
        service_id=service_id,
        status=status,
        request_number=req_clean,
        inputs=inputs,
        cluster=None,
        deployment_targets=[],
        current_step=current_step,
        step_results={"request_check": step_payload},
        workspace_control=wc,
    )

    meta = {"request_check": step_payload, "status_reason": status}
    return doc, meta
