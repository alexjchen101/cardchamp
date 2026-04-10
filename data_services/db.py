"""
Data Services SQLite Database

CardConnect delivers **independent daily files** (not one growing history):
each transaction file is that period’s activity only. We **do not** keep every
raw row forever — we store compact rollups:

daily_txn_totals
    (mid, business_date) → sale_amount for rows that count toward volume.
    Re-importing the same calendar day replaces that day’s totals.

mid_last_funding
    mid → latest Funding Date seen across daily funding files (canonical YYYYMMDD).
    Many MIDs will share the same date: the file’s ``Funding Date`` is the deposit
    calendar day for that row, and most merchants are funded on the same business day.

merchant_pci
    mid → PCI Y/N from the daily merchant snapshot.

Legacy tables (transactions, funding, adjustments, chargebacks) may exist from
older versions; new ingest uses only the tables above plus ``merchant_pci``.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from data_services.volume_rules import row_counts_toward_volume

ROOT = Path(__file__).resolve().parent.parent


def normalize_funding_date_yyyymmdd(raw: str) -> str | None:
    """
    Convert CardConnect ``Funding Date`` cell text to canonical YYYYMMDD for storage
    and lexicographic MAX. Accepts compact ``YYYYMMDD`` or ISO ``YYYY-MM-DD``.
    Returns None if the value cannot be parsed safely.
    """
    s = (raw or "").strip()
    if not s:
        return None
    if len(s) == 8 and s.isdigit():
        return s
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


DB_PATH = ROOT / "data" / "data_services.db"

DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    mid                 TEXT    NOT NULL,
    merchant_name       TEXT,
    gateway_ref_id      TEXT,
    funding_master_id   TEXT,
    batch_number        TEXT,
    batch_date          TEXT,
    card_type           TEXT,
    card_brand          TEXT,
    amount              REAL,
    currency_code       TEXT,
    txn_date            TEXT,   -- YYYYMMDD
    txn_type            TEXT,   -- Sale, Refund, ACH Return, …
    status              TEXT,   -- Processed, Rejected, …
    file_date           TEXT,
    UNIQUE(gateway_ref_id, mid)
);

CREATE TABLE IF NOT EXISTS funding (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    funding_master_id   TEXT,
    mid                 TEXT    NOT NULL,
    merchant_name       TEXT,
    net_sales           REAL,
    funding_amount      REAL,
    funding_date        TEXT,   -- YYYYMMDD
    currency_code       TEXT,
    file_date           TEXT,
    UNIQUE(funding_master_id, mid)
);

CREATE TABLE IF NOT EXISTS merchant_pci (
    mid                 TEXT    PRIMARY KEY,
    merchant_name       TEXT,
    pci_compliant       TEXT,   -- 'Y' or 'N'
    pci_expiration_date TEXT,
    merchant_status     TEXT,
    last_updated        TEXT
);

CREATE TABLE IF NOT EXISTS daily_txn_totals (
    mid                 TEXT    NOT NULL,
    business_date       TEXT    NOT NULL,  -- YYYYMMDD
    sale_amount         REAL    NOT NULL,
    source_file_date    TEXT,
    PRIMARY KEY (mid, business_date)
);

CREATE TABLE IF NOT EXISTS mid_last_funding (
    mid                 TEXT    PRIMARY KEY,
    last_funding_date   TEXT    NOT NULL   -- YYYYMMDD; max across files
);

CREATE INDEX IF NOT EXISTS idx_daily_mid_date ON daily_txn_totals(mid, business_date);

CREATE TABLE IF NOT EXISTS adjustments (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    funding_master_id   TEXT,
    mid                 TEXT    NOT NULL,
    merchant_name       TEXT,
    category            TEXT,
    type                TEXT,
    description         TEXT,
    amount              REAL,
    currency_code       TEXT,
    file_date           TEXT
);

CREATE TABLE IF NOT EXISTS chargebacks (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    case_number                 TEXT,
    mid                         TEXT    NOT NULL,
    merchant_name               TEXT,
    card_number                 TEXT,
    card_brand                  TEXT,
    transaction_date            TEXT,
    transaction_amount          REAL,
    transaction_currency_code   TEXT,
    chargeback_date             TEXT,
    chargeback_amount           REAL,
    chargeback_currency_code    TEXT,
    chargeback_reason           TEXT,
    file_date                   TEXT
);

CREATE TABLE IF NOT EXISTS file_import_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    filename    TEXT    UNIQUE NOT NULL,
    file_date   TEXT,
    file_kind   TEXT,
    rows_merchant     INTEGER DEFAULT 0,
    rows_funding      INTEGER DEFAULT 0,
    rows_transactions INTEGER DEFAULT 0,
    rows_adjustment   INTEGER DEFAULT 0,
    rows_chargeback   INTEGER DEFAULT 0,
    imported_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_txn_mid_date  ON transactions(mid, txn_date);
CREATE INDEX IF NOT EXISTS idx_fund_mid_date ON funding(mid, funding_date);
CREATE INDEX IF NOT EXISTS idx_adj_mid ON adjustments(mid);
CREATE INDEX IF NOT EXISTS idx_cb_mid ON chargebacks(mid);
"""


def _migrate_schema(conn: sqlite3.Connection) -> None:
    """Add columns when upgrading an older DB file."""
    rows = conn.execute("PRAGMA table_info(file_import_log)").fetchall()
    cols = {r[1] for r in rows}
    if "file_kind" not in cols:
        conn.execute("ALTER TABLE file_import_log ADD COLUMN file_kind TEXT")
    if "rows_adjustment" not in cols:
        conn.execute("ALTER TABLE file_import_log ADD COLUMN rows_adjustment INTEGER DEFAULT 0")
    if "rows_chargeback" not in cols:
        conn.execute("ALTER TABLE file_import_log ADD COLUMN rows_chargeback INTEGER DEFAULT 0")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(DDL)
        _migrate_schema(conn)
        conn.commit()


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def upsert_merchant_pci(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Insert or replace PCI compliance rows from the Merchant section."""
    sql = """
        INSERT INTO merchant_pci
            (mid, merchant_name, pci_compliant, pci_expiration_date, merchant_status, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(mid) DO UPDATE SET
            merchant_name       = excluded.merchant_name,
            pci_compliant       = excluded.pci_compliant,
            pci_expiration_date = excluded.pci_expiration_date,
            merchant_status     = excluded.merchant_status,
            last_updated        = excluded.last_updated
    """
    now = datetime.now(timezone.utc).isoformat()
    count = 0
    for row in rows:
        mid = (row.get("Mid") or row.get("MID") or "").strip()
        if not mid:
            continue
        conn.execute(sql, (
            mid,
            row.get("Merchant DBA Name") or row.get("Legal Business Name") or "",
            (row.get("PCI Compliant") or "").strip().upper(),
            row.get("PCI Expiration Date") or "",
            row.get("Merchant Status") or "",
            now,
        ))
        count += 1
    return count


def ingest_transaction_daily_totals(conn: sqlite3.Connection, rows: list[dict], file_date: str) -> int:
    """
    Aggregate qualifying transaction rows into **per-MID per calendar day** totals.
    Replaces any existing row for the same (mid, business_date) — daily files are
    independent snapshots, not cumulative raw history.
    """
    totals: dict[tuple[str, str], float] = defaultdict(float)
    for row in rows:
        if not row_counts_toward_volume(row):
            continue
        mid = (row.get("Mid") or row.get("MID") or "").strip()
        if not mid:
            continue
        d = (row.get("Date") or "").strip()
        if len(d) < 8:
            continue
        try:
            amt = float(row.get("Amount") or 0)
        except (ValueError, TypeError):
            amt = 0.0
        totals[(mid, d)] += amt

    sql = """
        INSERT INTO daily_txn_totals (mid, business_date, sale_amount, source_file_date)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(mid, business_date) DO UPDATE SET
            sale_amount = excluded.sale_amount,
            source_file_date = excluded.source_file_date
    """
    for (mid, d), amt in totals.items():
        conn.execute(sql, (mid, d, amt, file_date))
    return len(totals)


def ingest_funding_last_dates(conn: sqlite3.Connection, rows: list[dict], _file_date: str) -> int:
    """
    Update latest funding (deposit) date per MID from the daily funding file.

    Each row’s ``Funding Date`` is the deposit calendar day for that funding line.
    The same date often appears for many MIDs in one file (same funding day).
    """
    sql = """
        INSERT INTO mid_last_funding (mid, last_funding_date)
        VALUES (?, ?)
        ON CONFLICT(mid) DO UPDATE SET
            last_funding_date = CASE
                WHEN excluded.last_funding_date > mid_last_funding.last_funding_date
                THEN excluded.last_funding_date
                ELSE mid_last_funding.last_funding_date
            END
    """
    count = 0
    for row in rows:
        mid = (row.get("MID") or row.get("Mid") or "").strip()
        if not mid:
            continue
        fd = normalize_funding_date_yyyymmdd(row.get("Funding Date") or "")
        if not fd:
            continue
        conn.execute(sql, (mid, fd))
        count += 1
    return count


def insert_funding_rows(conn: sqlite3.Connection, rows: list[dict], file_date: str) -> int:
    """Insert funding rows; ignore duplicates."""
    sql = """
        INSERT OR IGNORE INTO funding
            (funding_master_id, mid, merchant_name, net_sales, funding_amount,
             funding_date, currency_code, file_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    count = 0
    for row in rows:
        mid = (row.get("MID") or row.get("Mid") or "").strip()
        if not mid:
            continue
        try:
            net_sales = float(row.get("Net Sales") or 0)
        except (ValueError, TypeError):
            net_sales = 0.0
        try:
            funding_amt = float(row.get("Funding Amount") or 0)
        except (ValueError, TypeError):
            funding_amt = 0.0
        conn.execute(sql, (
            row.get("Funding Master Id") or row.get("Funding Master ID") or "",
            mid,
            row.get("Merchant Name") or "",
            net_sales,
            funding_amt,
            row.get("Funding Date") or "",
            row.get("Currency Code") or "",
            file_date,
        ))
        count += 1
    return count


def insert_transaction_rows(conn: sqlite3.Connection, rows: list[dict], file_date: str) -> int:
    """Insert transaction rows; ignore duplicates based on (gateway_ref_id, mid)."""
    sql = """
        INSERT OR IGNORE INTO transactions
            (mid, merchant_name, gateway_ref_id, funding_master_id,
             batch_number, batch_date, card_type, card_brand,
             amount, currency_code, txn_date, txn_type, status, file_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    count = 0
    for row in rows:
        mid = (row.get("Mid") or row.get("MID") or "").strip()
        if not mid:
            continue
        try:
            amount = float(row.get("Amount") or 0)
        except (ValueError, TypeError):
            amount = 0.0
        conn.execute(sql, (
            mid,
            row.get("Merchant Name") or "",
            row.get("Gateway Reference Id") or row.get("Gateway Reference ID") or "",
            row.get("Funding Master Id") or row.get("Funding Master ID") or "",
            row.get("Batch Number") or "",
            row.get("Batch Date") or "",
            row.get("Card Type") or "",
            row.get("Card Brand") or "",
            amount,
            row.get("Currency Code") or "",
            row.get("Date") or "",
            row.get("Type") or "",
            row.get("Status") or "",
            file_date,
        ))
        count += 1
    return count


def insert_adjustment_rows(conn: sqlite3.Connection, rows: list[dict], file_date: str) -> int:
    """Insert adjustment rows (audit / reconciliation; not used for HubSpot MTD/YTD)."""
    sql = """
        INSERT INTO adjustments
            (funding_master_id, mid, merchant_name, category, type, description,
             amount, currency_code, file_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    count = 0
    for row in rows:
        mid = (row.get("MID") or row.get("Mid") or "").strip()
        if not mid:
            continue
        try:
            amount = float(row.get("Amount") or 0)
        except (ValueError, TypeError):
            amount = 0.0
        conn.execute(sql, (
            row.get("Funding Master Id") or row.get("Funding Master ID") or "",
            mid,
            row.get("Merchant Name") or "",
            row.get("Category") or "",
            row.get("Type") or "",
            row.get("Description") or "",
            amount,
            row.get("Currency Code") or "",
            file_date,
        ))
        count += 1
    return count


def insert_chargeback_rows(conn: sqlite3.Connection, rows: list[dict], file_date: str) -> int:
    """Insert chargeback rows (audit; volume logic is from transactions, not net of CB unless you add rules)."""
    sql = """
        INSERT INTO chargebacks
            (case_number, mid, merchant_name, card_number, card_brand,
             transaction_date, transaction_amount, transaction_currency_code,
             chargeback_date, chargeback_amount, chargeback_currency_code,
             chargeback_reason, file_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    count = 0
    for row in rows:
        mid = (row.get("MID") or row.get("Mid") or "").strip()
        if not mid:
            continue
        try:
            tx_amt = float(row.get("Transaction Amount") or 0)
        except (ValueError, TypeError):
            tx_amt = 0.0
        try:
            cb_amt = float(row.get("Chargeback Amount") or 0)
        except (ValueError, TypeError):
            cb_amt = 0.0
        conn.execute(sql, (
            str(row.get("Case Number") or ""),
            mid,
            row.get("Merchant Name") or "",
            row.get("Card Number") or "",
            row.get("Card Brand") or "",
            row.get("Transaction Date") or "",
            tx_amt,
            row.get("Transaction Currency Code") or "",
            row.get("Chargeback Date") or "",
            cb_amt,
            row.get("Chargeback Currency Code") or "",
            row.get("Chargeback Reason") or "",
            file_date,
        ))
        count += 1
    return count


def log_import(
    conn: sqlite3.Connection,
    filename: str,
    file_date: str,
    *,
    rows_merchant: int = 0,
    rows_funding: int = 0,
    rows_transactions: int = 0,
    rows_adjustment: int = 0,
    rows_chargeback: int = 0,
    file_kind: str = "",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO file_import_log
            (filename, file_date, file_kind, rows_merchant, rows_funding, rows_transactions,
             rows_adjustment, rows_chargeback, imported_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            filename,
            file_date,
            file_kind,
            rows_merchant,
            rows_funding,
            rows_transactions,
            rows_adjustment,
            rows_chargeback,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def is_file_imported(conn: sqlite3.Connection, filename: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM file_import_log WHERE filename = ?", (filename,)
    ).fetchone()
    return row is not None
