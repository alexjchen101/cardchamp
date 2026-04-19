#!/usr/bin/env python3
"""
One-time migration: set every deal stage from CoPilot + volume rules **without** sticky Live.

Use this once to align HubSpot with MTD/YTD vs. the boarded-to-live threshold. Afterward,
run normal ``batch_sync`` / ``sync_with_status`` (default ``sticky_live=True``) so deals
that are already Live are never auto-moved back to Boarded.

Examples::

    python3 jobs/migrate_deal_stages_volume_gate.py
    python3 jobs/migrate_deal_stages_volume_gate.py --limit 10

Requires the same HubSpot / CoPilot / data services setup as ``jobs/batch_sync.py``.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from jobs.batch_sync import DEFAULT_SUMMARY_FILE, _emails_from_hubspot_with_copilot, _write_summary
from jobs.sync_with_status import sync_with_status


def main() -> int:
    parser = argparse.ArgumentParser(
        description="One-time deal stage migration (volume gate, no sticky Live)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N contacts (for a dry run on a subset)",
    )
    parser.add_argument(
        "--summary-file",
        default=str(DEFAULT_SUMMARY_FILE.with_name("last_migrate_deal_stages.json")),
        help="Path to write the JSON summary report",
    )
    args = parser.parse_args()

    emails = _emails_from_hubspot_with_copilot()
    if args.limit is not None:
        emails = emails[: args.limit]

    if not emails:
        print("No contacts to migrate.")
        return 0

    total = len(emails)
    ok = 0
    failed = 0
    failed_contacts: list[str] = []

    print("=" * 60)
    print("MIGRATE DEAL STAGES (sticky_live=False)")
    print("=" * 60)

    for idx, email in enumerate(emails, 1):
        print(f"\n[{idx}/{total}] {email}")
        if sync_with_status(email, sticky_live=False):
            ok += 1
        else:
            failed += 1
            failed_contacts.append(email)

    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "migrate_deal_stages_volume_gate",
        "sticky_live": False,
        "total": total,
        "succeeded": ok,
        "failed": failed,
        "failed_contacts": failed_contacts,
    }
    _write_summary(Path(args.summary_file), summary)

    print("\n" + "=" * 60)
    print("MIGRATION SUMMARY")
    print("=" * 60)
    print(f"Total: {total}")
    print(f"Succeeded: {ok}")
    print(f"Failed: {failed}")
    print(f"Summary file: {args.summary_file}")
    print("=" * 60)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
