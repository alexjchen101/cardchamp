"""
CardConnect daily CSV helpers.

Production drops are **separate files** named ``DS_CARDCHAMP_<KIND>_<timestamp>.csv``
(same columns as the reference Excel). Each file is one table: header row + data rows.

We do **not** parse combined multi-section workbooks here — only these flat CSVs.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path


def detect_cardconnect_flat_kind(filename: str) -> str | None:
    """
    Return ``merchant``, ``funding``, ``transaction``, ``adjustment``, ``chargeback``,
    or ``None`` if the filename is not a CardConnect Data Services drop.
    """
    u = filename.upper()
    if not u.startswith("DS_CARDCHAMP_"):
        return None
    if "_MERCHANT_" in u:
        return "merchant"
    if "_FUNDING_" in u:
        return "funding"
    if "_TRANSACTION_" in u:
        return "transaction"
    if "_ADJUSTMENT_" in u:
        return "adjustment"
    if "_CHARGEBACK_" in u:
        return "chargeback"
    return None


def _is_footer_row(row: list[str]) -> bool:
    first = (row[0] or "").strip().lower()
    return first.startswith("number of records")


def read_flat_csv_dict_rows(path: Path | str) -> list[dict]:
    """
    Read one CSV table: header row + data rows. Skips blank rows and
    ``Number of Records:`` footer lines. Delimiter is comma or pipe.
    """
    path = Path(path)
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    if not raw.strip():
        return []
    first_line = raw.split("\n", 1)[0]
    delimiter = "|" if first_line.count("|") > first_line.count(",") else ","

    reader = csv.reader(io.StringIO(raw), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return []
    headers = [c.strip() for c in rows[0]]
    out: list[dict] = []
    for row in rows[1:]:
        if not row or all(cell.strip() == "" for cell in row):
            continue
        if _is_footer_row(row):
            break
        padded = row + [""] * max(0, len(headers) - len(row))
        out.append(dict(zip(headers, [c.strip() for c in padded])))
    return out
