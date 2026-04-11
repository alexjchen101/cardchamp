#!/usr/bin/env python3
"""
Show which CardConnect Data Services SFTP files are recorded in SQLite (imported).

Each remote CSV is **one daily (or per-batch) snapshot**; we aggregate into
``daily_txn_totals`` / ``mid_last_funding`` / ``merchant_pci`` ourselves — the
DB is the rolled-up source of truth, not the CSVs on disk.

Usage:
  python3 tools/data_services_import_status.py
  python3 tools/data_services_import_status.py --compare-remote   # needs .env SFTP
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from data_services import db as dsdb
from data_services.sftp_client import DataServicesSFTPClient, is_required_hubspot_data_file

# DS_CARDCHAMP_<KIND>_<YYYYMMDDHHMM>.csv
_RE_FILE = re.compile(
    r"^DS_CARDCHAMP_(MERCHANT|FUNDING|TRANSACTION|ADJUSTMENT|CHARGEBACK)_(\d+)\.csv$",
    re.I,
)


def _day_from_suffix(suffix: str) -> str:
    """YYYYMMDDHHMM -> YYYYMMDD calendar day."""
    if len(suffix) >= 8:
        return suffix[:8]
    return suffix


def _rows_from_log(row) -> dict:
    return {
        "filename": row["filename"],
        "file_date": row["file_date"],
        "kind": row["file_kind"] or "",
        "merchant": row["rows_merchant"] or 0,
        "funding": row["rows_funding"] or 0,
        "txn": row["rows_transactions"] or 0,
        "imported_at": row["imported_at"] or "",
    }


def print_db_report(conn) -> None:
    rows = conn.execute(
        """
        SELECT filename, file_date, file_kind, rows_merchant, rows_funding,
               rows_transactions, imported_at
        FROM file_import_log
        WHERE filename LIKE 'DS_CARDCHAMP_%'
        ORDER BY filename
        """
    ).fetchall()

    print("=== IMPORT LOG (DS_CARDCHAMP files in SQLite) ===\n")
    if not rows:
        print("(none)\n")
        return

    by_day: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        d = _rows_from_log(r)
        m = _RE_FILE.match(d["filename"])
        if m:
            day = _day_from_suffix(m.group(2))
            kind = m.group(1).upper()
            by_day[day][kind].append(d["filename"])
        else:
            by_day["(unparsed)"]["OTHER"].append(d["filename"])

    print("By calendar day (from filename timestamp):\n")
    for day in sorted(by_day.keys()):
        kinds = by_day[day]
        hubspot_kinds = ("MERCHANT", "FUNDING", "TRANSACTION")
        status = []
        for k in hubspot_kinds:
            status.append(f"{k}: {'✓' if k in kinds else '—'}")
        print(f"  {day}  {'  '.join(status)}")
        for k, names in sorted(kinds.items()):
            for fn in names:
                print(f"      {fn}")

    print("\n--- Detail (all rows) ---\n")
    for r in rows:
        d = _rows_from_log(r)
        print(
            f"  {d['filename']}\n"
            f"      file_date={d['file_date']} kind={d['kind'] or '—'} | "
            f"merchant_rows={d['merchant']} funding_mids={d['funding']} txn_buckets={d['txn']} | "
            f"{d['imported_at'][:19]}Z\n"
        )


def print_remote_compare(conn) -> None:
    client = DataServicesSFTPClient()
    remote = sorted(client.list_remote_files())
    imported = {
        r[0]
        for r in conn.execute("SELECT filename FROM file_import_log").fetchall()
    }

    print(
        "\n=== SFTP incoming/ vs SQLite ===\n"
        "HubSpot pipeline imports MERCHANT / FUNDING / TRANSACTION only. "
        "ADJUSTMENT / CHARGEBACK are listed as skipped (by design).\n"
    )
    for name in remote:
        if not name.upper().startswith("DS_CARDCHAMP_"):
            continue
        if not is_required_hubspot_data_file(name):
            print(f"  [skipped — not used for PCI/volume/deposit] {name}")
            continue
        state = "IMPORTED" if name in imported else "PENDING (not ingested yet)"
        print(f"  [{state}] {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Data Services import status (SQLite + optional SFTP)")
    parser.add_argument(
        "--compare-remote",
        action="store_true",
        help="List SFTP incoming/ and show IMPORTED vs not for each DS_CARDCHAMP file",
    )
    args = parser.parse_args()

    dsdb.init_db()
    conn = dsdb.get_connection()

    print(
        "CardConnect sends **independent daily/batch CSVs**. We ingest each file once "
        "(tracked by filename), then **aggregate** into rollups in SQLite — MTD/YTD "
        "are sums over ``daily_txn_totals``, not one giant cumulative file.\n"
    )
    print_db_report(conn)
    if args.compare_remote:
        print_remote_compare(conn)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
