"""
Map CoPilot ``merchant.salesCode`` → HubSpot ``hubspot_owner_id`` (contact + deal owner).

**Spreadsheet workflow (recommended)**  
Export the client spreadsheet to CSV and save as ``sales_code_owner_map.csv`` in the repo root.
Header row must include a sales-code column; include at least one owner column per row:

  - ``hubspot_owner_id`` (or ``owner_id``) — best: no API lookup
  - ``owner_email`` (or ``email``) — resolved via HubSpot CRM owners
  - ``owner_name`` / ``dropdown_name`` / ``rep_name`` / ``name`` — resolved by matching
    HubSpot owner first+last (case-insensitive, collapsed whitespace)

**Legacy**  
If ``sales_code_owner_map.csv`` is missing or has no data rows, ``sales_code_owner_map.json`` is used:
``{ "SALESCODE": "ownerId", ... }`` (case-insensitive keys).

List HubSpot owners: ``python3 scripts/list_hubspot_owners.py`` (needs ``HUBSPOT_ACCESS_TOKEN``).
"""

from __future__ import annotations

import csv
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hubspot.client import HubSpotClient

_ROOT = Path(__file__).resolve().parent
_CSV_PATH = _ROOT / "sales_code_owner_map.csv"
_JSON_PATH = _ROOT / "sales_code_owner_map.json"

# Cached owner list from HubSpot (per process). Cleared on reload_sales_code_owner_map().
_owners_cache: Optional[list] = None


def reload_sales_code_owner_map() -> None:
    """Reload mapping files and clear HubSpot owner cache."""
    global _owners_cache
    _read_mapping_table.cache_clear()
    _owners_cache = None


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _normalize_sales_code_key(code: str) -> str:
    return str(code).strip().upper()


def _normalize_owner_id(raw: Any) -> Optional[str]:
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        return None
    s = str(raw).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s or None


def _normalize_csv_fieldnames(names: Optional[list[str]]) -> dict[str, str]:
    """Map canonical key -> original header string."""
    if not names:
        return {}
    out: dict[str, str] = {}
    for raw in names:
        if raw is None:
            continue
        key = re.sub(r"[\s\-]+", "_", raw.strip().lower())
        out[key] = raw
    alias = {
        "sales_code": "sales_code",
        "salescode": "sales_code",
        "copilot_sales_code": "sales_code",
        "code": "sales_code",
        "hubspot_owner_id": "hubspot_owner_id",
        "owner_id": "hubspot_owner_id",
        "hubspot_id": "hubspot_owner_id",
        "hs_owner_id": "hubspot_owner_id",
        "owner_email": "owner_email",
        "email": "owner_email",
        "owner_name": "owner_name",
        "dropdown_name": "owner_name",
        "rep_name": "owner_name",
        "name": "owner_name",
        "contact_owner": "owner_name",
        "display_name": "owner_name",
    }
    canon: dict[str, str] = {}
    for k, orig in out.items():
        c = alias.get(k)
        if c:
            canon[c] = orig
    return canon


def _cell(row: dict, canon: dict[str, str], key: str) -> str:
    h = canon.get(key)
    if not h:
        return ""
    return str(row.get(h) or "").strip()


def _read_json_fallback() -> dict[str, dict[str, str]]:
    """Upper sales code -> row dict with hubspot_owner_id only."""
    if not _JSON_PATH.is_file():
        return {}
    try:
        data = json.loads(_JSON_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for k, v in data.items():
        if k is None or v is None:
            continue
        code = _normalize_sales_code_key(str(k).strip())
        oid = _normalize_owner_id(v)
        if code and oid:
            out[code] = {"hubspot_owner_id": oid, "owner_email": "", "owner_name": ""}
    return out


@lru_cache(maxsize=1)
def _read_mapping_table() -> dict[str, dict[str, str]]:
    """
    Returns dict: upper sales_code -> {hubspot_owner_id, owner_email, owner_name}
    (empty strings mean unset). Last row wins for duplicate codes.
    """
    by_code: dict[str, dict[str, str]] = {}
    if _CSV_PATH.is_file():
        try:
            text = _CSV_PATH.read_text(encoding="utf-8-sig")
            lines = text.splitlines()
            if lines:
                reader = csv.DictReader(lines)
                canon = _normalize_csv_fieldnames(reader.fieldnames)
                if canon.get("sales_code"):
                    for row in reader:
                        code_raw = _cell(row, canon, "sales_code")
                        if not code_raw:
                            continue
                        code = _normalize_sales_code_key(code_raw)
                        by_code[code] = {
                            "hubspot_owner_id": _cell(row, canon, "hubspot_owner_id"),
                            "owner_email": _cell(row, canon, "owner_email"),
                            "owner_name": _cell(row, canon, "owner_name"),
                        }
        except (OSError, ValueError):
            pass
    if not by_code:
        return _read_json_fallback()
    return by_code


def _get_cached_owners(client: "HubSpotClient") -> list:
    global _owners_cache
    if _owners_cache is None:
        _owners_cache = client.list_owners(limit=500)
    return _owners_cache


def _owner_id_from_email(owners: list, email: str) -> Optional[str]:
    if not email:
        return None
    e = email.strip().lower()
    for o in owners:
        if (o.get("email") or "").strip().lower() == e:
            return _normalize_owner_id(o.get("id"))
    return None


def _owner_id_from_display_name(owners: list, label: str) -> Optional[str]:
    if not label:
        return None
    target = _normalize_ws(label)
    if not target:
        return None
    for o in owners:
        fn = (o.get("firstName") or "").strip()
        ln = (o.get("lastName") or "").strip()
        candidates = [
            _normalize_ws(f"{fn} {ln}"),
            _normalize_ws(f"{ln}, {fn}"),
            _normalize_ws(f"{ln} {fn}"),
            _normalize_ws(f"{fn}, {ln}"),
        ]
        if target in candidates:
            return _normalize_owner_id(o.get("id"))
    for o in owners:
        fn = (o.get("firstName") or "").strip()
        ln = (o.get("lastName") or "").strip()
        full = _normalize_ws(f"{fn} {ln}")
        if full and (full in target or target in full):
            return _normalize_owner_id(o.get("id"))
    return None


def hubspot_owner_id_for_sales_code(
    sales_code: Optional[str],
    hubspot_client: Optional["HubSpotClient"] = None,
) -> Optional[str]:
    """
    Resolve HubSpot CRM owner id for a CoPilot sales code.

    Uses ``sales_code_owner_map.csv`` if it has data rows; otherwise ``sales_code_owner_map.json``.

    If the row only has email or display name, pass ``hubspot_client`` so we can match
    against ``GET /crm/v3/owners``.
    """
    if not sales_code:
        return None
    table = _read_mapping_table()
    code = _normalize_sales_code_key(str(sales_code).strip())
    row = table.get(code)
    if not row:
        return None

    oid = _normalize_owner_id(row.get("hubspot_owner_id"))
    if oid:
        return oid

    if not hubspot_client:
        return None

    owners = _get_cached_owners(hubspot_client)
    oid = _owner_id_from_email(owners, row.get("owner_email") or "")
    if oid:
        return oid
    oid = _owner_id_from_display_name(owners, row.get("owner_name") or "")
    return oid
