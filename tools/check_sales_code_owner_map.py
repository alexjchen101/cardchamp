#!/usr/bin/env python3
"""
Verify each row in the active sales-code owner CSV resolves to a HubSpot owner id.

Usage (repo root):
    python3 tools/check_sales_code_owner_map.py

Requires: HUBSPOT_ACCESS_TOKEN in .env
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from hubspot.client import HubSpotClient  # noqa: E402
from sales_code_owners import (  # noqa: E402
    _OWNER_MAPPING_CSV,
    _normalize_csv_fieldnames,
    _row_has_owner_identifiers,
    hubspot_owner_id_for_sales_code,
    reload_sales_code_owner_map,
)


def main() -> None:
    reload_sales_code_owner_map()
    if not _OWNER_MAPPING_CSV.is_file():
        print(f"No {_OWNER_MAPPING_CSV.name} — using JSON fallback only; nothing to check.")
        return
    client = HubSpotClient()
    text = _OWNER_MAPPING_CSV.read_text(encoding="utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    if not reader.fieldnames:
        print("CSV has no header row.")
        sys.exit(1)
    canon = _normalize_csv_fieldnames(reader.fieldnames)
    if not canon.get("sales_code"):
        print("CSV must include a sales_code column (or alias: salescode, code, …).")
        sys.exit(1)
    h_code = canon["sales_code"]
    ok = 0
    bad = 0
    for row in reader:
        code = str(row.get(h_code) or "").strip()
        if not code:
            continue
        if not _row_has_owner_identifiers(row, canon):
            print(f"SKIP (no owner row / inactive) {code!r}")
            continue
        oid = hubspot_owner_id_for_sales_code(code, client)
        if oid:
            print(f"OK  {code!r} -> {oid}")
            ok += 1
        else:
            print(f"MISSING  {code!r} -> could not resolve (add owner id, email, or dropdown name)")
            bad += 1
    if ok == 0 and bad == 0:
        print("No data rows in CSV (headers only).")
        return
    if bad:
        print(f"\n{ok} ok, {bad} missing")
        sys.exit(1)
    print(f"\nAll {ok} rows resolved.")


if __name__ == "__main__":
    main()
