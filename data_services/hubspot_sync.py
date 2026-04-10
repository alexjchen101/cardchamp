"""
Data Services → HubSpot Sync

Finds HubSpot contacts whose ``merchant_id`` property contains one of the
MIDs present in the SFTP data, then pushes:

    mtd_volume            (Single-line text)
    ytd_volume            (Single-line text)
    last_deposit_date     (Date picker – label "Last Deposit Date"; epoch ms string)
    pci_compliance        (Multiple checkboxes – semicolon-separated values)

MID matching
------------
HubSpot stores MIDs in the ``merchant_id`` property as a slash-separated
string (mirrors CoPilot backEndMid).  Example: "5023456789 / 5023456790"
We split on "/" and strip whitespace to build the lookup table.

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


def _parse_mids_from_property(merchant_id_value: str) -> list[str]:
    """
    Split a slash-separated merchant_id property value into individual MIDs.
    Handles " / ", "/", and plain whitespace as separators.
    """
    if not merchant_id_value:
        return []
    parts = []
    for part in merchant_id_value.replace("/", " ").split():
        mid = part.strip()
        if mid:
            parts.append(mid)
    return parts


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


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


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


def build_mid_to_contact_map(hubspot: HubSpotClient) -> dict[str, list[dict]]:
    """
    Iterate all HubSpot contacts that have a ``merchant_id`` value.
    Returns a dict mapping each individual MID string → list of contact records.

    One MID should map to exactly one contact, but the list handles edge cases.
    Each contact record is: {"id": "...", "email": "...", "merchant_id": "...", "contact_id": "..."}
    """
    mid_map: dict[str, list[dict]] = {}
    for contact in hubspot.iter_contacts_with_property("merchant_id"):
        props = contact.get("properties") or {}
        raw_mid = props.get("merchant_id") or ""
        contact_id = contact.get("id")
        email = props.get("email") or ""
        for mid in _parse_mids_from_property(raw_mid):
            entry = {"id": contact_id, "email": email, "merchant_id": raw_mid}
            mid_map.setdefault(mid, []).append(entry)
    return mid_map


def _build_contact_workset_from_mid_map(
    conn: sqlite3.Connection,
    mid_map: dict[str, list[dict]],
    *,
    only_mids_in_db: bool = True,
) -> tuple[dict[str, list[str]], dict[str, dict]]:
    """
    Group MID → HubSpot hits into contact_id → list of MIDs.

    If ``only_mids_in_db``, only MIDs that appear in the local DB (rollups + PCI)
    are attached.
    """
    db_mids: set[str] = set()
    if only_mids_in_db:
        for row in conn.execute(
            "SELECT DISTINCT mid FROM daily_txn_totals "
            "UNION SELECT DISTINCT mid FROM merchant_pci "
            "UNION SELECT DISTINCT mid FROM mid_last_funding"
        ):
            if row[0]:
                db_mids.add(row[0])

    contact_to_mids: dict[str, list[str]] = {}
    contact_meta: dict[str, dict] = {}

    for mid, contacts in mid_map.items():
        if only_mids_in_db and mid not in db_mids:
            continue
        for c in contacts:
            cid = c["id"]
            contact_to_mids.setdefault(cid, []).append(mid)
            contact_meta[cid] = c

    return contact_to_mids, contact_meta


def _build_contact_workset_from_allowlist(
    conn: sqlite3.Connection,
    hubspot: HubSpotClient,
    allowlist_emails: list[str],
) -> tuple[dict[str, list[str]], dict[str, dict], dict[str, int]]:
    """
    For each allowlisted email, resolve the HubSpot contact and parse ``merchant_id``
    into MIDs.  Metrics still come from SQLite (CSV / multi-section ingest).

    Returns (contact_to_mids, contact_meta, tallies) where tallies hold
    not_found / no_merchant_id counts.
    """
    contact_to_mids: dict[str, list[str]] = {}
    contact_meta: dict[str, dict] = {}
    tallies = {"not_found": 0, "no_merchant_id": 0}

    seen_contact: set[str] = set()
    for raw_email in allowlist_emails:
        email = (raw_email or "").strip()
        if not email or email.startswith("#"):
            continue
        norm = _normalize_email(email)

        try:
            search_results = hubspot.search_contacts_by_email(email)
        except Exception as exc:
            print(f"   ✗ HubSpot search failed for {email!r}: {exc}")
            tallies["not_found"] += 1
            continue

        if not search_results.get("results"):
            print(f"   ℹ️  No HubSpot contact for allowlisted email {email!r}")
            tallies["not_found"] += 1
            continue

        contact_id = search_results["results"][0]["id"]
        contact = hubspot.get_contact(contact_id, properties=["email", "merchant_id"])
        props = contact.get("properties") or {}
        resolved_email = (props.get("email") or email).strip()
        raw_mid = props.get("merchant_id") or ""
        mids = _parse_mids_from_property(raw_mid)

        if not mids:
            print(f"   ℹ️  {resolved_email} (id={contact_id}): no merchant_id in HubSpot")
            tallies["no_merchant_id"] += 1
            continue

        if contact_id in seen_contact:
            print(f"   ℹ️  {resolved_email}: duplicate allowlist hit for same contact; merging MIDs")
        seen_contact.add(contact_id)

        existing = contact_to_mids.setdefault(contact_id, [])
        for mid in mids:
            if mid not in existing:
                existing.append(mid)

        contact_meta[contact_id] = {
            "id": contact_id,
            "email": resolved_email,
            "merchant_id": raw_mid,
        }

    return contact_to_mids, contact_meta, tallies


def sync_data_services_to_hubspot(
    conn: sqlite3.Connection,
    hubspot: HubSpotClient,
    dry_run: bool = False,
    allowlist_emails: list[str] | None = None,
    allowlist_only_mids_in_db: bool = True,
) -> dict:
    """
    Push Data Services metrics (MTD/YTD, last deposit, PCI) into HubSpot.

    **Full sync (default):** scan every HubSpot contact that has ``merchant_id``,
    keep MIDs that exist in the local DB, update.

    **Allowlist sync:** pass ``allowlist_emails`` (e.g. from ``config/live_allowlist.txt``).
    For each email, look up the contact, read ``merchant_id``, compute metrics from SQLite.
    MIDs do not need a pre-built global map; PCI still comes from ``merchant_pci`` when present.

    Args:
        allowlist_only_mids_in_db: When using full sync, ignore HubSpot MIDs absent from DB.
            Allowlist mode always uses HubSpot MIDs as given (metrics may be sparse if MID missing in DB).
        conn:    Open SQLite connection to the data services DB.
        hubspot: Authenticated HubSpotClient.
        dry_run: If True, print what would be sent without PATCHing.

    Returns:
        Summary dict with counts.
    """
    print("\n--- DATA SERVICES → HUBSPOT SYNC ---")

    tallies: dict[str, int] = {}

    if allowlist_emails is not None:
        print(f"   Mode: allowlist ({len(allowlist_emails)} line(s) in file)")
        contact_to_mids, contact_meta, tallies = _build_contact_workset_from_allowlist(
            conn, hubspot, allowlist_emails
        )
        if tallies.get("not_found"):
            print(f"   Allowlist: {tallies['not_found']} email(s) not found in HubSpot")
        if tallies.get("no_merchant_id"):
            print(f"   Allowlist: {tallies['no_merchant_id']} contact(s) without merchant_id")
    else:
        print("   Mode: all contacts with merchant_id (MIDs present in local DB)")
        mid_map = build_mid_to_contact_map(hubspot)
        print(f"   Found {len(mid_map)} unique MID(s) across HubSpot contacts")
        db_count = len({
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT mid FROM daily_txn_totals "
                "UNION SELECT DISTINCT mid FROM merchant_pci "
                "UNION SELECT DISTINCT mid FROM mid_last_funding"
            )
            if row[0]
        })
        print(f"   MIDs in local DB: {db_count}")
        contact_to_mids, contact_meta = _build_contact_workset_from_mid_map(
            conn, mid_map, only_mids_in_db=allowlist_only_mids_in_db
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
        "allowlist_not_found": tallies.get("not_found", 0),
        "allowlist_no_merchant_id": tallies.get("no_merchant_id", 0),
    }
    if allowlist_emails is not None:
        summary["mode"] = "allowlist"
    else:
        summary["mode"] = "all"

    print(f"\n   Done: {updated} updated, {skipped} skipped, {errors} errors")
    return summary
