"""
Data Services Aggregator

Computes per-MID metrics from the SQLite database:

    mtd_volume       – sum of transaction amounts for the current calendar month
    ytd_volume       – sum of transaction amounts for the current calendar year
    last_deposit_date – most recent funding date in YYYYMMDD format
    pci_compliant    – 'Y' if all MIDs are compliant, 'N' if any are not

Volume calculation:
    - Raw file rows are filtered (sale/auth/capture + processed/funded) when building
      ``daily_txn_totals``; MTD/YTD sum those **daily** buckets for the month/year.

PCI override rule (from field mapping notes):
    "If one account is NOT PCI Compliant and one is, the NOT Compliant one overrides."
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

# HubSpot deal stage: CoPilot LIVE stays Boarded until rolled-up volume reaches this (MTD or YTD).
BOARDED_TO_LIVE_VOLUME_THRESHOLD_USD = 500.0


def _ym(dt: datetime) -> str:
    """Return YYYYMM prefix for the given datetime."""
    return dt.strftime("%Y%m")


def _y(dt: datetime) -> str:
    """Return YYYY prefix for the given datetime."""
    return dt.strftime("%Y")


def get_mtd_volume(conn: sqlite3.Connection, mids: list[str], as_of: datetime = None) -> float:
    """
    Sum of **daily rolled-up** sale amounts for ``mids`` in the current calendar month.

    Source: ``daily_txn_totals`` (one row per MID per business day; each day’s file
    replaces that day’s contribution — files are not cumulative raw history).
    """
    as_of = as_of or datetime.now(timezone.utc)
    prefix = _ym(as_of) + "%"
    placeholders = ",".join("?" * len(mids))
    sql = f"""
        SELECT COALESCE(SUM(sale_amount), 0)
        FROM daily_txn_totals
        WHERE mid IN ({placeholders})
          AND business_date LIKE ?
    """
    params = mids + [prefix]
    row = conn.execute(sql, params).fetchone()
    return float(row[0]) if row else 0.0


def backend_mids_in_order(merchant_data_list: list[dict]) -> list[str]:
    """
    Unique ``backEndMid`` values from CoPilot merchant payloads, in first-seen order
    (same order as ``copilot_account`` / fetch order).
    """
    out: list[str] = []
    seen: set[str] = set()
    for md in merchant_data_list:
        mid = (md.get("merchant") or {}).get("processing", {}).get("platformDetails", {}).get(
            "backEndMid"
        )
        if mid is None:
            continue
        s = str(mid).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def has_qualifying_processing_volume(conn: sqlite3.Connection, mids: list[str]) -> bool | None:
    """
    True if rolled-up MTD or YTD sale amount meets ``BOARDED_TO_LIVE_VOLUME_THRESHOLD_USD``.

    Returns None when ``mids`` is empty so callers can skip the gate (CoPilot-only).
    """
    if not mids:
        return None
    mtd = get_mtd_volume(conn, mids)
    ytd = get_ytd_volume(conn, mids)
    t = BOARDED_TO_LIVE_VOLUME_THRESHOLD_USD
    return mtd >= t or ytd >= t


def get_ytd_volume(conn: sqlite3.Connection, mids: list[str], as_of: datetime = None) -> float:
    """
    Sum of daily rolled-up sale amounts for ``mids`` in the current calendar year.
    """
    as_of = as_of or datetime.now(timezone.utc)
    prefix = _y(as_of) + "%"
    placeholders = ",".join("?" * len(mids))
    sql = f"""
        SELECT COALESCE(SUM(sale_amount), 0)
        FROM daily_txn_totals
        WHERE mid IN ({placeholders})
          AND business_date LIKE ?
    """
    params = mids + [prefix]
    row = conn.execute(sql, params).fetchone()
    return float(row[0]) if row else 0.0


def get_last_deposit_date(conn: sqlite3.Connection, mids: list[str]) -> str | None:
    """
    Latest **Funding Date** from the funding CSVs (YYYYMMDD), as ``MAX`` over ``mids``.

    Many contacts look the same in HubSpot on purpose: CardConnect’s ``Funding Date``
    is the **deposit calendar day** for that line, and most merchants are funded on the
    same business day, so ``mid_last_funding`` clusters on a small set of dates.
    """
    placeholders = ",".join("?" * len(mids))
    sql = f"""
        SELECT MAX(last_funding_date)
        FROM mid_last_funding
        WHERE mid IN ({placeholders})
          AND last_funding_date != ''
    """
    row = conn.execute(sql, mids).fetchone()
    val = row[0] if row else None
    return val if val else None


def get_pci_status(conn: sqlite3.Connection, mids: list[str]) -> str | None:
    """
    Returns 'Y' only if ALL mids are PCI compliant.
    Returns 'N' if any mid is 'N'.
    Returns None if no records found.

    Override rule: a single non-compliant MID overrides all compliant ones.
    """
    placeholders = ",".join("?" * len(mids))
    sql = f"""
        SELECT pci_compliant
        FROM merchant_pci
        WHERE mid IN ({placeholders})
    """
    rows = conn.execute(sql, mids).fetchall()
    values = [r[0] for r in rows if r[0]]
    if not values:
        return None
    if any(v.upper() == "N" for v in values):
        return "N"
    return "Y"


def format_volume(amount: float) -> str:
    """Format a dollar amount as a readable string for HubSpot text fields."""
    return f"${amount:,.2f}"


def yyyymmdd_to_epoch_ms(date_str: str) -> str | None:
    """
    Convert YYYYMMDD string to epoch milliseconds (string) for HubSpot date pickers.
    Returns None on invalid input.
    """
    if not date_str or len(date_str) < 8:
        return None
    try:
        dt = datetime(
            int(date_str[0:4]),
            int(date_str[4:6]),
            int(date_str[6:8]),
            tzinfo=timezone.utc,
        )
        return str(int(dt.timestamp() * 1000))
    except (ValueError, IndexError):
        return None


def get_metrics_for_mids(conn: sqlite3.Connection, mids: list[str]) -> dict:
    """
    Convenience wrapper: returns all four metrics for a list of MIDs.

    Returns:
        {
            "mtd_volume":         "$1,234.56"  or None,
            "ytd_volume":         "$9,876.54"  or None,
            "last_deposit_date":  "1234567890000" (epoch ms)  or None,
            "pci_compliant":      "Y" / "N"  or None,
        }
    """
    if not mids:
        return {
            "mtd_volume": None,
            "ytd_volume": None,
            "last_deposit_date": None,
            "pci_compliant": None,
        }

    mtd = get_mtd_volume(conn, mids)
    ytd = get_ytd_volume(conn, mids)
    last_date = get_last_deposit_date(conn, mids)
    pci = get_pci_status(conn, mids)

    # Use ``mids`` (not truthiness of totals) so $0.00 MTD/YTD still syncs.
    return {
        "mtd_volume": format_volume(mtd) if mids else None,
        "ytd_volume": format_volume(ytd) if mids else None,
        "last_deposit_date": yyyymmdd_to_epoch_ms(last_date) if last_date else None,
        "pci_compliant": pci,
    }
