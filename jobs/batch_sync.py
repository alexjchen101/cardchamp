#!/usr/bin/env python3
"""
Batch runner for daily CoPilot -> HubSpot sync.

Modes:
- all (default): every HubSpot contact with ``copilot_account`` set
- allowlist: only emails listed in ``config/live_allowlist.txt``
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from hubspot.client import HubSpotClient
from jobs.sync_with_status import sync_with_status

DEFAULT_ALLOWLIST = ROOT / "config" / "live_allowlist.txt"
DEFAULT_SUMMARY_FILE = ROOT / "sync" / "last_batch_run.json"


def _load_allowlist(path: Path) -> list[str]:
    emails = []
    if not path.is_file():
        return emails
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        emails.append(line)
    return emails


def _emails_from_hubspot_with_copilot() -> list[str]:
    client = HubSpotClient()
    emails = []
    seen = set()
    for row in client.iter_contacts_with_property("copilot_account"):
        props = row.get("properties", {}) or {}
        email = (props.get("email") or "").strip()
        if not email or email in seen:
            continue
        seen.add(email)
        emails.append(email)
    return emails


def _write_summary(summary_file: Path, summary: dict) -> None:
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def _run_batch(emails: list[str], mode: str, summary_file: Path) -> int:
    total = len(emails)
    ok = 0
    failed = 0
    failed_contacts = []
    for idx, email in enumerate(emails, 1):
        print(f"\n[{idx}/{total}] Syncing {email}")
        success = sync_with_status(email)
        if success:
            ok += 1
        else:
            failed += 1
            failed_contacts.append(email)
    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "total": total,
        "succeeded": ok,
        "failed": failed,
        "failed_contacts": failed_contacts,
    }
    _write_summary(summary_file, summary)
    print("\n" + "=" * 60)
    print("BATCH SYNC SUMMARY")
    print("=" * 60)
    print(f"Total: {total}")
    print(f"Succeeded: {ok}")
    print(f"Failed: {failed}")
    print(f"Summary file: {summary_file}")
    print("=" * 60)
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run batch CoPilot -> HubSpot sync")
    parser.add_argument(
        "--mode",
        choices=("all", "allowlist"),
        default="all",
        help="all = every HubSpot contact with copilot_account (default); allowlist = email file only",
    )
    parser.add_argument(
        "--allowlist-file",
        default=str(DEFAULT_ALLOWLIST),
        help="Path to allowlist file for allowlist mode",
    )
    parser.add_argument(
        "--summary-file",
        default=str(DEFAULT_SUMMARY_FILE),
        help="Path to write the JSON summary report",
    )
    args = parser.parse_args()

    if args.mode == "all":
        emails = _emails_from_hubspot_with_copilot()
    else:
        emails = _load_allowlist(Path(args.allowlist_file))

    if not emails:
        print("No contacts to sync.")
        return 0

    return _run_batch(emails, args.mode, Path(args.summary_file))


if __name__ == "__main__":
    raise SystemExit(main())
