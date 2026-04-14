#!/usr/bin/env python3
"""
Data Services Sync Job

Downloads the daily SFTP drop from CardConnect, ingests it into the
local SQLite database, then pushes updated MTD Volume, YTD Volume,
Last Date Deposit, and PCI Compliance into HubSpot.

**Production order:** run CoPilot → HubSpot first, then this job. Use
``jobs/run_go_live_pipeline.py`` so the batch API runs before the SFTP step.

Intended to run once daily after noon (the drop arrives ~12:00 ET).

Usage
-----
    # Normal daily run: SFTP ingest, then HubSpot for every contact with copilot_account
    python3 jobs/sync_data_services.py

    # Re-process a specific already-downloaded file
    python3 jobs/sync_data_services.py --file data/sftp_downloads/some_file.csv

    # Download + ingest only (no HubSpot push)
    python3 jobs/sync_data_services.py --ingest-only

    # HubSpot push only — recompute SFTP-derived fields (mtd_volume, ytd_volume,
    # last_deposit_date, pci_compliance) from whatever is already in SQLite. Use when
    # there is no new SFTP file but you want HubSpot refreshed from the last ingest.
    python3 jobs/sync_data_services.py --hubspot-only

    # Same, after CoPilot batch, without re-downloading SFTP:
    python3 jobs/run_go_live_pipeline.py --skip-copilot-batch --hubspot-only

    # Dry-run: show what would be sent to HubSpot without sending
    python3 jobs/sync_data_services.py --dry-run

    # Force re-download files already marked processed
    python3 jobs/sync_data_services.py --force-download

    # Peek at what's on the SFTP without downloading
    python3 jobs/sync_data_services.py --list-remote

    # HubSpot push for specific emails only (e.g. test accounts; optional)
    python3 jobs/sync_data_services.py --hubspot-only --allowlist
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from data_services.sftp_client import DataServicesSFTPClient, SFTP_REMOTE_DIR
from data_services.parser import detect_cardconnect_flat_kind, read_flat_csv_dict_rows
from data_services import db as dsdb
from copilot.merchant import MerchantAPI
from data_services.hubspot_sync import sync_data_services_to_hubspot
from hubspot.client import HubSpotClient

DEFAULT_ALLOWLIST = ROOT / "config" / "live_allowlist.txt"
_DOWNLOADS_DIR = (ROOT / "data" / "sftp_downloads").resolve()

# Only these three files are pulled from SFTP and ingested for HubSpot metrics.
_INGESTABLE_KINDS = frozenset({"merchant", "funding", "transaction"})


def _safe_delete_downloaded_csv(path: Path) -> None:
    """Delete a CSV under ``data/sftp_downloads/`` after successful ingest (saves disk)."""
    try:
        if not path.is_file():
            return
        if path.resolve().parent != _DOWNLOADS_DIR:
            return
        path.unlink()
        print(f"   Removed local CSV: {path.name}")
    except OSError as exc:
        print(f"   ℹ️  Could not remove {path}: {exc}")


def _load_allowlist(path: Path) -> list[str]:
    emails: list[str] = []
    if not path.is_file():
        return emails
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        emails.append(line)
    return emails


def _extract_file_date(filename: str) -> str:
    """
    Attempt to extract a YYYYMMDD date from the filename.
    Falls back to today's date if none found.
    """
    m = re.search(r"(\d{8})", filename)
    if m:
        return m.group(1)
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def ingest_file(conn, path: Path) -> dict:
    """
    Parse a Data Services file into compact SQLite tables, then remove the local CSV.

    **Daily independence:** each file is that day’s snapshot only. We store
    **per-day totals per MID** for volume (not unbounded raw rows) and **max**
    funding date per MID.

    **Ingestable files only:** ``DS_CARDCHAMP_MERCHANT_*``, ``FUNDING_*``, ``TRANSACTION_*``
    (same columns as the reference Excel). Other names are rejected.
    """
    filename = path.name
    if dsdb.is_file_imported(conn, filename):
        print(f"   ↩  Already imported: {filename} – skipping ingest")
        # Force-download re-fetches files already in the DB; drop the duplicate local copy.
        _safe_delete_downloaded_csv(path)
        return {"filename": filename, "skipped": True}

    file_date = _extract_file_date(filename)
    kind = detect_cardconnect_flat_kind(filename)

    if not kind or kind not in _INGESTABLE_KINDS:
        print(
            f"   ✗ Expected DS_CARDCHAMP_MERCHANT_* | FUNDING_* | TRANSACTION_* "
            f"(got {kind!r}): {filename}"
        )
        return {"filename": filename, "skipped": True, "error": "unsupported_file"}

    if path.stat().st_size == 0:
        print(f"   ↩  Empty file (0 bytes): {filename}")
        with conn:
            dsdb.log_import(
                conn,
                filename,
                file_date,
                file_kind=kind or "empty",
            )
        _safe_delete_downloaded_csv(path)
        return {"filename": filename, "file_date": file_date, "empty": True, "kind": kind}

    print(f"   Parsing: {filename}")

    rows = read_flat_csv_dict_rows(path)
    with conn:
        n_merchant = n_funding = n_txn = 0
        if kind == "merchant":
            n_merchant = dsdb.upsert_merchant_pci(conn, rows)
        elif kind == "funding":
            n_funding = dsdb.ingest_funding_last_dates(conn, rows, file_date)
        elif kind == "transaction":
            n_txn = dsdb.ingest_transaction_daily_totals(conn, rows, file_date)
        dsdb.log_import(
            conn,
            filename,
            file_date,
            file_kind=kind,
            rows_merchant=n_merchant if kind == "merchant" else 0,
            rows_funding=n_funding if kind == "funding" else 0,
            rows_transactions=n_txn if kind == "transaction" else 0,
        )

    print(
        f"   ✓ Ingested [{kind}]: merchant_rows={n_merchant}, "
        f"funding_mids={n_funding}, txn_day_buckets={n_txn}"
    )
    result = {
        "filename": filename,
        "file_date": file_date,
        "kind": kind,
        "rows_merchant": n_merchant if kind == "merchant" else 0,
        "rows_funding": n_funding if kind == "funding" else 0,
        "rows_transactions": n_txn if kind == "transaction" else 0,
    }
    _safe_delete_downloaded_csv(path)
    return result


def run(
    file: str | None = None,
    ingest_only: bool = False,
    hubspot_only: bool = False,
    dry_run: bool = False,
    force_download: bool = False,
    list_remote: bool = False,
    use_allowlist: bool = False,
    allowlist_file: str | None = None,
) -> int:
    print("=" * 60)
    print("DATA SERVICES SYNC")
    print("=" * 60)

    # -- List remote files only ------------------------------------------
    if list_remote:
        sftp = DataServicesSFTPClient()
        files = sftp.list_remote_files()
        print(f"Remote files in {SFTP_REMOTE_DIR}/: {files or '(none)'}")
        return 0

    # -- Determine which local files to ingest ---------------------------
    files_to_ingest: list[Path] = []

    if file:
        p = Path(file)
        if not p.is_file():
            print(f"✗ File not found: {file}")
            return 1
        files_to_ingest = [p]
        print(f"\n1. Using specified file: {p}")

    elif not hubspot_only:
        print("\n1. Downloading new files from SFTP...")
        sftp = DataServicesSFTPClient()
        files_to_ingest = sftp.download_new_files(force=force_download)
        if not files_to_ingest:
            print("   No new files to download.")
            if not hubspot_only:
                print("   (If you expected a file, check the SFTP with --list-remote)")

    # -- Ingest into SQLite -----------------------------------------------
    if not hubspot_only:
        print("\n2. Ingesting into local database...")
        dsdb.init_db()
        conn = dsdb.get_connection()

        ingest_summaries = []
        for path in files_to_ingest:
            summary = ingest_file(conn, path)
            ingest_summaries.append(summary)

        if not ingest_summaries and not hubspot_only:
            print("   Nothing to ingest.")

    # -- Push to HubSpot --------------------------------------------------
    if not ingest_only:
        print("\n3. Pushing to HubSpot...")
        if hubspot_only:
            dsdb.init_db()
            conn = dsdb.get_connection()

        hubspot = HubSpotClient()
        copilot = MerchantAPI()
        allowlist_emails = None
        if use_allowlist:
            path = Path(allowlist_file or str(DEFAULT_ALLOWLIST))
            allowlist_emails = _load_allowlist(path)
            if not allowlist_emails:
                print(f"   ✗ Allowlist empty or missing: {path}")
                return 1

        hs_summary = sync_data_services_to_hubspot(
            conn,
            hubspot,
            copilot,
            dry_run=dry_run,
            allowlist_emails=allowlist_emails,
        )
    else:
        hs_summary = {"skipped": "ingest-only mode"}

    # -- Final summary ----------------------------------------------------
    print("\n" + "=" * 60)
    print("DATA SERVICES SYNC COMPLETE")
    print("=" * 60)
    if not hubspot_only:
        print(f"Files processed: {len(files_to_ingest)}")
    print(f"HubSpot sync: {json.dumps(hs_summary)}")
    if dry_run:
        print("(DRY RUN – no changes written to HubSpot)")
    print("=" * 60)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download CardConnect SFTP data drop and sync to HubSpot"
    )
    parser.add_argument(
        "--file",
        help="Path to an already-downloaded file to (re-)process",
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help="Download and ingest into DB only; skip HubSpot push",
    )
    parser.add_argument(
        "--hubspot-only",
        action="store_true",
        help="Push from DB to HubSpot only; skip download/ingest",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be sent to HubSpot without actually sending",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download files that have already been processed",
    )
    parser.add_argument(
        "--list-remote",
        action="store_true",
        help="List files available on the SFTP and exit",
    )
    parser.add_argument(
        "--allowlist",
        action="store_true",
        help="Limit HubSpot updates to emails in live_allowlist.txt (or --allowlist-file)",
    )
    parser.add_argument(
        "--allowlist-file",
        default=str(DEFAULT_ALLOWLIST),
        help="Path to allowlist file for --allowlist (default: config/live_allowlist.txt)",
    )
    args = parser.parse_args()

    return run(
        file=args.file,
        ingest_only=args.ingest_only,
        hubspot_only=args.hubspot_only,
        dry_run=args.dry_run,
        force_download=args.force_download,
        list_remote=args.list_remote,
        use_allowlist=args.allowlist,
        allowlist_file=args.allowlist_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())
