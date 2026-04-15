"""
Data Services → HubSpot Sync

Resolves **processing MIDs** from HubSpot ``copilot_account`` (same CoPilot IDs as
``batch_sync``) via the CoPilot API (``backEndMid``), then matches SFTP rollups in SQLite.

Pushes:

    mtd_volume            (Single-line text)
    ytd_volume            (Single-line text)
    last_deposit_date     (Date picker – label "Last Deposit Date"; epoch ms string)
    pci_compliance        (Multiple checkboxes – semicolon-separated values)

PCI override rule
-----------------
If ANY MID on a contact is 'N', the contact is marked not compliant.
The HubSpot checkbox field expects semicolon-separated option values.
Adjust PCI_COMPLIANT_VALUE / PCI_NOT_COMPLIANT_VALUE to match the actual
option values you've set up in HubSpot.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from copilot.merchant import MerchantAPI
from field_mappings import get_copilot_accounts_from_contact
from hubspot.client import HubSpotClient
from data_services.aggregator import get_metrics_for_mids

# ---------------------------------------------------------------------------
# HubSpot field names (internal names, not labels)
# ---------------------------------------------------------------------------
FIELD_MTD_VOLUME = "mtd_volume"
FIELD_YTD_VOLUME = "ytd_volume"
# HubSpot internal name for the contact property whose label is "Last Deposit Date"
FIELD_LAST_DEPOSIT = "last_deposit_date"
FIELD_PCI_COMPLIANCE = "pci_compliance"

# Option **labels / values** for ``pci_compliance`` (must match HubSpot property options).
PCI_COMPLIANT_VALUE = "Compliant"
PCI_NOT_COMPLIANT_VALUE = "Non Compliant"


def _build_pci_hubspot_value(pci: str | None) -> str | None:
    """
    Convert 'Y'/'N' PCI status to a HubSpot multi-checkbox string.
    Returns None when status is unknown.
    """
    if pci == "Y":
        return PCI_COMPLIANT_VALUE
    if pci == "N":
        return PCI_NOT_COMPLIANT_VALUE
    return None


def _backend_mid_from_merchant_payload(merchant_payload: dict) -> str | None:
    """Processing MID from CoPilot ``GET /merchant/{id}`` response."""
    m = merchant_payload.get("merchant") or {}
    mid = (m.get("processing") or {}).get("platformDetails", {}).get("backEndMid")
    if mid is None:
        return None
    s = str(mid).strip()
    return s or None


def _resolve_mids_from_copilot_ids(copilot: MerchantAPI, copilot_ids: list[str]) -> list[str]:
    """Call CoPilot for each ID; return unique backEndMids in first-seen order."""
    mids: list[str] = []
    seen: set[str] = set()
    for raw_id in copilot_ids:
        cid = (raw_id or "").strip()
        if not cid:
            continue
        try:
            payload = copilot.get_merchant(cid)
        except Exception as exc:
            print(f"   ⚠️  CoPilot get_merchant({cid!r}) failed: {exc}")
            continue
        mid = _backend_mid_from_merchant_payload(payload)
        if mid and mid not in seen:
            seen.add(mid)
            mids.append(mid)
    return mids


def _collect_db_mids(conn: sqlite3.Connection) -> set[str]:
    out: set[str] = set()
    for row in conn.execute(
        "SELECT DISTINCT mid FROM daily_txn_totals "
        "UNION SELECT DISTINCT mid FROM merchant_pci "
        "UNION SELECT DISTINCT mid FROM mid_last_funding"
    ):
        if row[0]:
            out.add(row[0])
    return out


def _property_updates_for_contact(conn: sqlite3.Connection, mids: list[str]) -> dict[str, str]:
    """
    Map Data Services metrics to HubSpot property dict for one contact's MID list.
    Omits keys when there is nothing to set.
    """
    metrics = get_metrics_for_mids(conn, mids)
    updates: dict[str, str] = {}
    if metrics["mtd_volume"] is not None:
        updates[FIELD_MTD_VOLUME] = metrics["mtd_volume"]
    if metrics["ytd_volume"] is not None:
        updates[FIELD_YTD_VOLUME] = metrics["ytd_volume"]
    if metrics["last_deposit_date"] is not None:
        updates[FIELD_LAST_DEPOSIT] = metrics["last_deposit_date"]
    if metrics["pci_compliant"] is not None:
        pci_val = _build_pci_hubspot_value(metrics["pci_compliant"])
        if pci_val:
            updates[FIELD_PCI_COMPLIANCE] = pci_val
    return updates


def _build_contact_workset_from_copilot_full(
    conn: sqlite3.Connection,
    hubspot: HubSpotClient,
    copilot: MerchantAPI,
    *,
    only_mids_in_db: bool,
) -> tuple[dict[str, list[str]], dict[str, dict]]:
    """
    All HubSpot contacts with ``copilot_account`` set: resolve MIDs via CoPilot API.
    """
    db_mids = _collect_db_mids(conn)
    contact_to_mids: dict[str, list[str]] = {}
    contact_meta: dict[str, dict] = {}

    for contact in hubspot.iter_contacts_with_property("copilot_account"):
        props = contact.get("properties") or {}
        contact_id = contact.get("id")
        if not contact_id:
            continue
        email = (props.get("email") or "").strip()
        copilot_ids = get_copilot_accounts_from_contact(
            {"copilot_account": props.get("copilot_account")}
        )
        if not copilot_ids:
            continue
        mids = _resolve_mids_from_copilot_ids(copilot, copilot_ids)
        if only_mids_in_db:
            mids = [m for m in mids if m in db_mids]
        if not mids:
            continue
        contact_to_mids[contact_id] = mids
        contact_meta[contact_id] = {
            "id": contact_id,
            "email": email,
            "copilot_account": (props.get("copilot_account") or "").strip(),
        }

    return contact_to_mids, contact_meta


def sync_data_services_to_hubspot(
    conn: sqlite3.Connection,
    hubspot: HubSpotClient,
    copilot: MerchantAPI,
    dry_run: bool = False,
    *,
    only_mids_in_db: bool = True,
) -> dict:
    """
    Push Data Services metrics (MTD/YTD, last deposit, PCI) into HubSpot.

    Every contact with ``copilot_account`` set: resolve MIDs via CoPilot API. Optionally
    restrict to MIDs present in the local DB (default).

    Args:
        only_mids_in_db: If True, skip MIDs not present in SQLite rollups.
        conn:    Open SQLite connection to the data services DB.
        hubspot: Authenticated HubSpotClient.
        copilot: CoPilot API client (used to map CoPilot ID → backEndMid).
        dry_run: If True, print what would be sent without PATCHing.

    Returns:
        Summary dict with counts.
    """
    print("\n--- DATA SERVICES → HUBSPOT SYNC ---")

    print("   Contacts with copilot_account (MIDs from CoPilot API)")
    db_count = len(_collect_db_mids(conn))
    print(f"   MIDs in local DB: {db_count}")
    contact_to_mids, contact_meta = _build_contact_workset_from_copilot_full(
        conn,
        hubspot,
        copilot,
        only_mids_in_db=only_mids_in_db,
    )

    print(f"   Contacts to process: {len(contact_to_mids)}")

    updated = 0
    skipped = 0
    errors = 0

    for contact_id, mids in contact_to_mids.items():
        meta = contact_meta[contact_id]
        email = meta.get("email") or contact_id
        try:
            updates = _property_updates_for_contact(conn, mids)
            if not updates:
                skipped += 1
                continue

            print(f"   [{contact_id}] {email}")
            print(f"       MIDs: {', '.join(mids)}")
            for k, v in updates.items():
                print(f"       {k}: {v}")

            if not dry_run:
                hubspot.update_contact(contact_id, updates, filter_to_existing=True)

            updated += 1

        except Exception as exc:
            print(f"   ✗ Error updating {email}: {exc}")
            errors += 1

    summary = {
        "contacts_updated": updated,
        "contacts_skipped_no_data": skipped,
        "contacts_errored": errors,
        "dry_run": dry_run,
        "mode": "all",
    }

    print(f"\n   Done: {updated} updated, {skipped} skipped, {errors} errors")
    return summary
