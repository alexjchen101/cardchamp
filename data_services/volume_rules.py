"""
Which transaction rows count toward HubSpot MTD / YTD volume.

CardConnect sends **one file per day** with that period’s activity only; we store
**per-day totals per MID** (not every raw row) for efficiency.
"""

from __future__ import annotations

# Align with ``aggregator`` filters.
SALE_TYPES = frozenset({"sale", "auth", "capture"})
PROCESSED_STATUSES = frozenset({"processed", "funded"})


def row_counts_toward_volume(row: dict) -> bool:
    typ = (row.get("Type") or "").strip().lower()
    status = (row.get("Status") or "").strip().lower()
    return typ in SALE_TYPES and status in PROCESSED_STATUSES
