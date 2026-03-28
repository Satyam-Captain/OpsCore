from __future__ import annotations

import os
from typing import List

from flask import Flask, flash, redirect, render_template, request, session, url_for

from urllib.parse import urlencode

from core.file_detail import resolve_file_page
from core.gold_decisions import gold_decision_key, load_gold_map, save_gold_map
from core.matrix import build_matrix_view
from core.models import Domain, Source
from core.scanner import run_scan
from core.scan_snapshot import is_valid_scan_id, load_scan_snapshot, save_scan_snapshot
from core.storage import load_json
from core.side_by_side_diff import build_side_by_side_rows
from core.text_diff import unified_diff_text
from providers.exceptions import SshReadError
from providers.factory import create_inventory_provider

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "opscore-dev-insecure-change-for-production")


def load_sources():
    raw = load_json("config/sources.json")
    return [Source(**item) for item in raw["sources"] if item.get("enabled", True)]


def load_domains():
    raw = load_json("config/domains.json")
    return [Domain(**item) for item in raw["domains"]]


def load_settings():
    return load_json("config/settings.json")


def get_domain_by_id(domain_id: str):
    for domain in load_domains():
        if domain.id == domain_id:
            return domain
    return None


def gold_decisions_file_path() -> str:
    return load_settings().get("gold_decisions_path", "data/gold/gold_decisions.json")


def inventory_provider():
    """Configured backend (mock vs ssh) from settings.json — recreate per use so config edits apply."""
    return create_inventory_provider(load_settings())


def run_scan_and_matrix(
    domain_id: str,
    path_input: str,
    recursive: bool,
    source_ids: List[str],
    sources: List[Source],
):
    domain = get_domain_by_id(domain_id)
    selected_sources = [s for s in sources if s.id in source_ids]
    if not domain or not selected_sources:
        return None, None
    provider = inventory_provider()
    result = run_scan(
        provider=provider,
        domain=domain,
        sources=selected_sources,
        path_input=path_input,
        recursive=recursive,
    )
    gold_map = load_gold_map(gold_decisions_file_path())
    matrix = build_matrix_view(
        result,
        selected_sources,
        provider,
        domain,
        sources,
        gold_map,
    )
    return result, matrix


def _store_scan_replay_from_form():
    scan_id = request.form.get("replay_scan_id", "").strip()
    if scan_id and is_valid_scan_id(scan_id):
        session["scan_replay"] = {"scan_id": scan_id}
        return
    session["scan_replay"] = {
        "domain_id": request.form.get("replay_domain_id", "").strip(),
        "path_input": request.form.get("replay_path_input", "").strip(),
        "recursive": request.form.get("replay_recursive") == "1",
        "source_ids": request.form.getlist("replay_source_id"),
    }


def _replay_sources_from_request() -> List[str]:
    raw = request.args.get("replay_sources", "")
    return [x.strip() for x in raw.split(",") if x.strip()]


def _stash_scan_replay_from_query():
    scan_id = request.args.get("scan_id", "").strip()
    if scan_id and is_valid_scan_id(scan_id):
        session["scan_replay"] = {"scan_id": scan_id}
        return
    domain_id = request.args.get("domain_id", "").strip()
    path_input = request.args.get("replay_path_input", "")
    recursive = request.args.get("replay_recursive", "0") == "1"
    replay_sources = _replay_sources_from_request()
    if domain_id and replay_sources:
        session["scan_replay"] = {
            "domain_id": domain_id,
            "path_input": path_input,
            "recursive": recursive,
            "source_ids": replay_sources,
        }


def _load_snapshot_pair(scan_id: str, catalog: List[Source]):
    if not is_valid_scan_id(scan_id):
        return None
    return load_scan_snapshot(scan_id, catalog)


@app.route("/", methods=["GET", "POST"])
def index():
    sources = load_sources()
    domains = load_domains()
    result = None
    matrix = None

    scan_id_q = request.args.get("scan_id", "").strip()
    if scan_id_q:
        loaded = _load_snapshot_pair(scan_id_q, sources)
        if loaded:
            result, matrix = loaded
        else:
            flash("Saved scan not found or outdated. Run a new scan.", "error")

    if result is None:
        replay = session.pop("scan_replay", None)
        if replay and replay.get("scan_id"):
            loaded = _load_snapshot_pair(replay["scan_id"], sources)
            if loaded:
                result, matrix = loaded
            else:
                flash("Could not restore the scan view. Run Scan again.", "error")
        elif replay and replay.get("domain_id") and replay.get("source_ids"):
            result, matrix = run_scan_and_matrix(
                replay["domain_id"],
                replay.get("path_input", ""),
                replay.get("recursive", False),
                replay["source_ids"],
                sources,
            )
            if not result:
                flash("Could not restore the scan view. Run Scan again.", "error")

    if request.method == "POST":
        domain_id = request.form.get("domain_id", "")
        path_input = request.form.get("path_input", "").strip()
        recursive = request.form.get("recursive") == "on"
        selected_source_ids = request.form.getlist("source_ids")

        selected_sources = [source for source in sources if source.id in selected_source_ids]
        domain = get_domain_by_id(domain_id)

        if domain and selected_sources:
            provider = inventory_provider()
            result = run_scan(
                provider=provider,
                domain=domain,
                sources=selected_sources,
                path_input=path_input,
                recursive=recursive,
            )
            gold_map = load_gold_map(gold_decisions_file_path())
            matrix = build_matrix_view(
                result,
                selected_sources,
                provider,
                domain,
                sources,
                gold_map,
            )
            try:
                save_scan_snapshot(result, matrix, sources)
            except (OSError, ValueError) as exc:
                flash(f"Scan completed but could not save snapshot: {exc}", "error")

    return render_template(
        "index.html",
        sources=sources,
        domains=domains,
        result=result,
        matrix=matrix,
    )


@app.route("/compare")
def compare_landing():
    return render_template("compare.html")


@app.route("/source-of-truth")
def source_of_truth():
    return render_template(
        "source_of_truth.html",
        gold_path=gold_decisions_file_path(),
    )


@app.route("/gold/registry")
def gold_registry():
    sources = load_sources()
    by_id = {s.id: s for s in sources}
    gold_map = load_gold_map(gold_decisions_file_path())
    entries = []
    for key, sid in sorted(gold_map.items()):
        if "::" in key:
            domain_id, rel = key.split("::", 1)
        else:
            domain_id, rel = "—", key
        src = by_id.get(sid)
        gold_label = f"{src.name} ({src.id})" if src else sid
        entries.append(
            {
                "domain_id": domain_id,
                "relative_path": rel,
                "gold_source_id": sid,
                "gold_label": gold_label,
            }
        )
    return render_template(
        "gold_registry.html",
        entries=entries,
        gold_path=gold_decisions_file_path(),
    )


@app.route("/gold/form", methods=["GET"])
def gold_form():
    sources = load_sources()
    by_id = {s.id: s for s in sources}

    domain_id = request.args.get("domain_id", "").strip()
    relative_path = request.args.get("relative_path", "").strip()
    mode = request.args.get("mode", "set")
    allowed_ids = [x.strip() for x in request.args.get("allowed_ids", "").split(",") if x.strip()]
    replay_domain_id = request.args.get("replay_domain_id", "").strip() or domain_id
    replay_path_input = request.args.get("replay_path_input", "")
    replay_recursive = request.args.get("replay_recursive", "0")
    replay_source_ids = [
        x.strip() for x in request.args.get("replay_sources", "").split(",") if x.strip()
    ]
    replay_scan_id = request.args.get("scan_id", "").strip()
    current_gold = request.args.get("current_gold", "").strip()

    if not domain_id or not relative_path or not allowed_ids:
        flash("Invalid gold request (missing file, domain, or available sources).", "error")
        return redirect(url_for("index"))

    if not get_domain_by_id(domain_id):
        flash("Unknown domain.", "error")
        return redirect(url_for("index"))

    for sid in allowed_ids:
        if sid not in by_id:
            flash("Unknown source in selection.", "error")
            return redirect(url_for("index"))

    if mode not in ("set", "change"):
        mode = "set"

    if replay_scan_id and is_valid_scan_id(replay_scan_id):
        session["scan_replay"] = {"scan_id": replay_scan_id}
    elif replay_source_ids:
        session["scan_replay"] = {
            "domain_id": replay_domain_id,
            "path_input": replay_path_input,
            "recursive": replay_recursive == "1",
            "source_ids": replay_source_ids,
        }
    else:
        flash("Missing scan context. Open gold from scan results or run Scan again.", "error")
        return redirect(url_for("index"))

    options = []
    for sid in allowed_ids:
        src = by_id.get(sid)
        label = f"{src.name} ({src.id})" if src else sid
        options.append((sid, label))

    return render_template(
        "gold_form.html",
        domain_id=domain_id,
        relative_path=relative_path,
        mode=mode,
        page_title="Change gold" if mode == "change" else "Set gold",
        page_description=(
            "Pick which source is treated as canonical (gold) for this file when scoring drift."
        ),
        options=options,
        allowed_ids_csv=",".join(allowed_ids),
        replay_domain_id=replay_domain_id,
        replay_path_input=replay_path_input,
        replay_recursive=replay_recursive,
        replay_source_ids=replay_source_ids,
        replay_scan_id=replay_scan_id if is_valid_scan_id(replay_scan_id) else "",
        current_gold=current_gold if current_gold in allowed_ids else "",
    )


@app.route("/gold/save", methods=["POST"])
def gold_save():
    path = gold_decisions_file_path()
    domain_id = request.form.get("domain_id", "").strip()
    relative_path = request.form.get("relative_path", "").strip()
    gold_source_id = request.form.get("gold_source_id", "").strip()
    allowed_ids = [x.strip() for x in request.form.get("allowed_ids", "").split(",") if x.strip()]

    _store_scan_replay_from_form()

    if not domain_id or not relative_path or not gold_source_id:
        flash("Missing domain, file path, or gold source.", "error")
        return redirect(url_for("index"))

    if gold_source_id not in allowed_ids:
        flash("That source is not allowed as gold for this file in the current scan.", "error")
        return redirect(url_for("index"))

    key = gold_decision_key(domain_id, relative_path)
    gold_map = load_gold_map(path)
    had_gold = key in gold_map
    gold_map[key] = gold_source_id
    save_gold_map(path, gold_map)

    if had_gold:
        flash(f"Gold changed for {relative_path}.", "ok")
    else:
        flash(f"Gold set for {relative_path}.", "ok")

    return redirect(url_for("index"))


@app.route("/gold/remove", methods=["POST"])
def gold_remove():
    path = gold_decisions_file_path()
    domain_id = request.form.get("domain_id", "").strip()
    relative_path = request.form.get("relative_path", "").strip()

    _store_scan_replay_from_form()

    if not domain_id or not relative_path:
        flash("Missing domain or file path.", "error")
        return redirect(url_for("index"))

    key = gold_decision_key(domain_id, relative_path)
    gold_map = load_gold_map(path)
    if key in gold_map:
        del gold_map[key]
        save_gold_map(path, gold_map)
        flash(f"Gold removed for {relative_path}.", "ok")
    else:
        flash(f"No gold decision existed for {relative_path}.", "error")

    return redirect(url_for("index"))


@app.route("/file")
def file_detail():
    sources = load_sources()
    scan_id = request.args.get("scan_id", "").strip()
    domain_id = request.args.get("domain_id", "").strip()
    relative_path = request.args.get("relative_path", "").strip()

    if not scan_id or not is_valid_scan_id(scan_id):
        flash("Missing or invalid scan. Open Compare from the current scan results.", "error")
        return redirect(url_for("index"))

    if not domain_id or not relative_path:
        flash("Missing file context. Open Compare from a scan result.", "error")
        return redirect(url_for("index"))

    domain = get_domain_by_id(domain_id)
    if not domain:
        flash("Unknown domain.", "error")
        return redirect(url_for("index"))

    _stash_scan_replay_from_query()

    ctx = resolve_file_page(
        scan_id,
        domain,
        relative_path,
        sources,
        inventory_provider(),
    )
    if ctx is None:
        flash("That file is not in this scan snapshot.", "error")
        return redirect(url_for("index"))

    return render_template("file.html", ctx=ctx)


@app.route("/diff")
def diff_view():
    sources = load_sources()
    by_id = {s.id: s for s in sources}
    scan_id = request.args.get("scan_id", "").strip()
    domain_id = request.args.get("domain_id", "").strip()
    relative_path = request.args.get("relative_path", "").strip()
    src1 = request.args.get("src1", "").strip()
    src2 = request.args.get("src2", "").strip()

    if not scan_id or not is_valid_scan_id(scan_id):
        flash("Missing or invalid scan for diff.", "error")
        return redirect(url_for("index"))

    if not domain_id or not relative_path or not src1 or not src2:
        flash("Missing diff parameters.", "error")
        return redirect(url_for("index"))

    snap = load_scan_snapshot(scan_id, sources)
    if not snap:
        flash("Saved scan not found. Run a new scan.", "error")
        return redirect(url_for("index"))
    result, _matrix = snap
    if result.domain_id != domain_id:
        flash("Domain does not match saved scan.", "error")
        return redirect(url_for("index"))

    paths = {g.relative_path for g in result.groups}
    if relative_path not in paths:
        flash("File is not in this scan snapshot.", "error")
        return redirect(url_for("index"))

    allowed = set(result.source_ids)
    if src1 == src2:
        flash("Choose two different sources to compare.", "error")
        return redirect(
            url_for(
                "file_detail",
                scan_id=scan_id,
                domain_id=domain_id,
                relative_path=relative_path,
            )
        )
    if src1 not in allowed or src2 not in allowed:
        flash("Sources must be part of the saved scan.", "error")
        return redirect(url_for("index"))

    domain = get_domain_by_id(domain_id)
    if not domain:
        flash("Unknown domain.", "error")
        return redirect(url_for("index"))

    s1 = by_id.get(src1)
    s2 = by_id.get(src2)
    if not s1 or not s2:
        flash("Unknown source.", "error")
        return redirect(url_for("index"))

    _stash_scan_replay_from_query()

    provider = inventory_provider()
    try:
        left = provider.read_file_content(s1, domain, relative_path)
        right = provider.read_file_content(s2, domain, relative_path)
    except SshReadError as exc:
        flash(str(exc), "error")
        return redirect(
            url_for(
                "file_detail",
                scan_id=scan_id,
                domain_id=domain_id,
                relative_path=relative_path,
            )
        )
    except ValueError as exc:
        flash(str(exc), "error")
        return redirect(
            url_for(
                "file_detail",
                scan_id=scan_id,
                domain_id=domain_id,
                relative_path=relative_path,
            )
        )
    label1 = f"{s1.name} ({s1.id})"
    label2 = f"{s2.name} ({s2.id})"
    diff_text = unified_diff_text(left, right, label1, label2)
    side_by_side_rows = build_side_by_side_rows(left, right)

    file_back_q = urlencode(
        {
            "scan_id": scan_id,
            "domain_id": domain_id,
            "relative_path": relative_path,
        }
    )

    return render_template(
        "diff.html",
        scan_id=scan_id,
        domain_id=domain_id,
        relative_path=relative_path,
        src1_label=label1,
        src2_label=label2,
        side_by_side_rows=side_by_side_rows,
        diff_text=diff_text,
        file_back_query=file_back_q,
    )


if __name__ == "__main__":
    app.run(debug=True)
