#!/usr/bin/env python3
"""
CardChamp Dashboard Server

Runs on the EC2 alongside the sync jobs.
Serves the dashboard HTML and exposes API endpoints that read/write
real project files (owner_mapping.csv, sync logs).

Usage:
    python3 dashboard/server.py           # default port 8050
    python3 dashboard/server.py --port 8080

Access locally via SSH tunnel:
    ssh -i ~/pair1.pem -N -L 8050:localhost:8050 ubuntu@ec2-16-52-4-19.ca-central-1.compute.amazonaws.com
    Then open http://localhost:8050
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, send_file, send_from_directory

ROOT = Path(__file__).resolve().parent.parent
OWNER_CSV     = ROOT / "data" / "owner_mapping.csv"
SYNC_SUMMARY  = ROOT / "sync" / "last_batch_run.json"
SYNC_HISTORY  = ROOT / "sync" / "run_history.json"
SYNC_LOG_FILE = ROOT / "sync" / "go_live_pipeline.log"
STATIC_DIR    = Path(__file__).resolve().parent

app = Flask(__name__, static_folder=str(STATIC_DIR))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read_summary() -> dict:
    if SYNC_SUMMARY.exists():
        return json.loads(SYNC_SUMMARY.read_text())
    return {}


def _read_history() -> list[dict]:
    """Rolling list of last 50 batch run summaries, newest first."""
    if SYNC_HISTORY.exists():
        return json.loads(SYNC_HISTORY.read_text())
    return []


def _append_to_history(summary: dict) -> None:
    history = _read_history()
    history.insert(0, summary)
    history = history[:50]
    SYNC_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    SYNC_HISTORY.write_text(json.dumps(history, indent=2))


def _read_owners() -> list[dict]:
    """
    Parse data/owner_mapping.csv.
    Real columns: CoPilot Sales Code, CoPilot Descriptor,
                  Contact Owner / Deal Stage Owner, (blank),
                  User ID, First Name, Last Name, Email
    Returns normalised list of dicts.
    """
    if not OWNER_CSV.exists():
        return []
    rows = []
    with OWNER_CSV.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sales_code  = (row.get("CoPilot Sales Code") or "").strip()
            descriptor  = (row.get("CoPilot Descriptor") or "").strip()
            user_id     = (row.get("User ID") or "").strip()
            first       = (row.get("First Name") or "").strip()
            last        = (row.get("Last Name") or "").strip()
            email       = (row.get("Email") or "").strip()
            if not sales_code:
                continue
            rows.append({
                "sales_code":  sales_code,
                "descriptor":  descriptor,
                "user_id":     user_id,
                "first_name":  first,
                "last_name":   last,
                "owner_name":  f"{first} {last}".strip(),
                "email":       email,
                "active":      bool(user_id),
            })
    return rows


def _write_owners(rows: list[dict]) -> None:
    """Write normalised owner rows back to data/owner_mapping.csv."""
    OWNER_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "CoPilot Sales Code", "CoPilot Descriptor",
        "Contact Owner / Deal Stage Owner", "",
        "User ID", "First Name", "Last Name", "Email",
    ]
    with OWNER_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "CoPilot Sales Code": r.get("sales_code", ""),
                "CoPilot Descriptor": r.get("descriptor", ""),
                "Contact Owner / Deal Stage Owner": "",
                "": "",
                "User ID":    r.get("user_id", ""),
                "First Name": r.get("first_name", ""),
                "Last Name":  r.get("last_name", ""),
                "Email":      r.get("email", ""),
            })


def _tail_log(n: int = 200) -> list[str]:
    """Return last n lines of the cron log file."""
    if not SYNC_LOG_FILE.exists():
        return []
    lines = SYNC_LOG_FILE.read_text(errors="replace").splitlines()
    return lines[-n:]


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/api/status")
def api_status():
    """Last batch run summary + rolling history."""
    return jsonify({
        "last_run": _read_summary(),
        "history":  _read_history(),
    })


@app.route("/api/owners", methods=["GET"])
def api_owners_get():
    return jsonify(_read_owners())


@app.route("/api/owners", methods=["POST"])
def api_owners_post():
    """Replace the full owner map from a JSON body (array of row objects)."""
    rows = request.get_json(force=True)
    if not isinstance(rows, list):
        return jsonify({"error": "expected array"}), 400
    _write_owners(rows)
    return jsonify({"ok": True, "rows": len(rows)})


@app.route("/api/owners/upload", methods=["POST"])
def api_owners_upload():
    """
    Accept a CSV file upload, parse it, merge with existing map,
    and write back. Returns the merged rows.

    Accepts either the native owner_mapping.csv format OR a simplified
    3-column format: sales_code, owner_name, email
    """
    if "file" not in request.files:
        return jsonify({"error": "no file"}), 400
    f = request.files["file"]
    content = f.read().decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    headers = [h.strip() for h in (reader.fieldnames or [])]

    uploaded: list[dict] = []
    for row in reader:
        # Native format
        if "CoPilot Sales Code" in headers:
            sc    = (row.get("CoPilot Sales Code") or "").strip()
            desc  = (row.get("CoPilot Descriptor") or "").strip()
            uid   = (row.get("User ID") or "").strip()
            first = (row.get("First Name") or "").strip()
            last  = (row.get("Last Name") or "").strip()
            email = (row.get("Email") or "").strip()
        # Simplified 3-col format: sales_code, owner_name, email
        else:
            sc    = (row.get("sales_code") or "").strip()
            desc  = (row.get("descriptor") or "").strip()
            uid   = (row.get("user_id") or "").strip()
            name  = (row.get("owner_name") or "").strip()
            parts = name.split(" ", 1)
            first = parts[0] if parts else ""
            last  = parts[1] if len(parts) > 1 else ""
            email = (row.get("email") or "").strip()
        if not sc:
            continue
        uploaded.append({
            "sales_code": sc, "descriptor": desc,
            "user_id": uid, "first_name": first, "last_name": last,
            "owner_name": f"{first} {last}".strip(), "email": email,
            "active": bool(uid),
        })

    # Merge: uploaded rows win; keep existing rows not in upload
    existing = {r["sales_code"]: r for r in _read_owners()}
    for r in uploaded:
        existing[r["sales_code"]] = r
    merged = list(existing.values())
    _write_owners(merged)
    return jsonify({"ok": True, "rows": len(merged), "uploaded": len(uploaded)})


@app.route("/api/owners/download")
def api_owners_download():
    """Download the current owner_mapping.csv."""
    if not OWNER_CSV.exists():
        return jsonify({"error": "file not found"}), 404
    return send_file(
        str(OWNER_CSV),
        mimetype="text/csv",
        as_attachment=True,
        download_name="owner_mapping.csv",
    )


@app.route("/api/sync-log")
def api_sync_log():
    """Last 200 lines of the cron log file."""
    lines = _tail_log(200)
    return jsonify({"lines": lines, "file": str(SYNC_LOG_FILE)})


@app.route("/api/sync/trigger", methods=["POST"])
def api_sync_trigger():
    """
    Trigger a sync run in the background.
    Body: {} for batch, or {"email": "someone@example.com"} for single contact.
    Returns immediately; check /api/status after a few minutes for results.
    """
    body  = request.get_json(force=True, silent=True) or {}
    email = (body.get("email") or "").strip()
    venv  = ROOT / ".venv" / "bin" / "python"
    python = str(venv) if venv.exists() else sys.executable

    if email:
        cmd = [python, str(ROOT / "jobs" / "sync_with_status.py"), email]
    else:
        cmd = [python, str(ROOT / "jobs" / "batch_sync.py")]

    log_path = str(SYNC_LOG_FILE)
    SYNC_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(log_path, "a") as log_fh:
        log_fh.write(f"\n--- triggered via dashboard {datetime.now(timezone.utc).isoformat()} ---\n")
        subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=log_fh,
            stderr=log_fh,
            start_new_session=True,
        )

    return jsonify({
        "ok": True,
        "job": "single" if email else "batch",
        "target": email or "all contacts",
    })


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CardChamp dashboard server")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--host", default="127.0.0.1",
                        help="Bind address. Use 0.0.0.0 to expose on LAN (not recommended without auth).")
    args = parser.parse_args()
    print(f"CardChamp dashboard → http://{args.host}:{args.port}")
    print(f"  Owner map : {OWNER_CSV}")
    print(f"  Sync log  : {SYNC_LOG_FILE}")
    app.run(host=args.host, port=args.port, debug=False)
