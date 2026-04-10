"""
Map CoPilot ``merchant.salesCode`` → HubSpot ``hubspot_owner_id`` (contact + deal owner).

**Spreadsheet workflow (recommended)**  
Save the owner mapping CSV in ``data/``:

  - ``data/owner_mapping.csv`` (primary: Sales Codes tab export; columns like ``CoPilot Sales Code``, ``User ID``, ``Email``, ``First Name`` / ``Last Name``)

Rows with a sales code but **no** owner identifiers (User ID, email, or name) are treated as **inactive / cancelled** for mapping: they are not used to set ``hubspot_owner_id``. See ``is_sales_code_inactive_in_owner_map()``.

Header row must include a sales-code column; include at least one owner column per row (when the partner is active):

  - ``hubspot_owner_id`` (or ``owner_id``) — best: no API lookup
  - ``owner_email`` (or ``email``) — resolved via HubSpot CRM owners
  - ``owner_name`` / ``dropdown_name`` / ``rep_name`` / ``name`` — resolved by matching
    HubSpot owner first+last (case-insensitive, collapsed whitespace)

**JSON fallback**  
If ``data/owner_mapping.csv`` is missing or has no data rows,
``data/legacy/sales_code_owner_map.json`` is used:
``{ "SALESCODE": "ownerId", ... }`` (case-insensitive keys).

List HubSpot owners: ``python3 tools/list_hubspot_owners.py`` (needs ``HUBSPOT_ACCESS_TOKEN``).
"""

from __future__ import annotations

import csv
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hubspot.client import HubSpotClient

_ROOT = Path(__file__).resolve().parent
_DATA_DIR = _ROOT / "data"
# Primary sales-code → owner table (replaces older ``sales_code_owner_map.csv`` / raw export CSVs).
_OWNER_MAPPING_CSV = _DATA_DIR / "owner_mapping.csv"
_JSON_PATH = _DATA_DIR / "legacy" / "sales_code_owner_map.json"

# Cached owner list from HubSpot (per process). Cleared on reload_sales_code_owner_map().
_owners_cache: Optional[list] = None


def reload_sales_code_owner_map() -> None:
    """Reload mapping files and clear HubSpot owner cache."""
    global _owners_cache
    _read_owner_mapping_bundle.cache_clear()
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
        key = re.sub(r"[^a-z0-9]+", "_", raw.strip().lower()).strip("_")
        out[key] = raw
    alias = {
        "sales_code": "sales_code",
        "salescode": "sales_code",
        "copilot_sales_code": "sales_code",
        "code": "sales_code",
        "hubspot_owner_id": "hubspot_owner_id",
        "owner_id": "hubspot_owner_id",
        "user_id": "hubspot_owner_id",
        "userid": "hubspot_owner_id",
        "hubspot_id": "hubspot_owner_id",
        "hs_owner_id": "hubspot_owner_id",
        "owner_email": "owner_email",
        "email": "owner_email",
        "owner_name": "owner_name",
        "dropdown_name": "owner_name",
        "rep_name": "owner_name",
        "name": "owner_name",
        "contact_owner": "owner_name",
        "contact_owner_deal_stage_owner": "owner_name",
        "display_name": "owner_name",
        "first_name": "first_name",
        "last_name": "last_name",
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


def _row_owner_name(row: dict, canon: dict[str, str]) -> str:
    owner_name = _cell(row, canon, "owner_name")
    if owner_name:
        return owner_name
    first = _cell(row, canon, "first_name")
    last = _cell(row, canon, "last_name")
    combined = " ".join(part for part in (first, last) if part).strip()
    return combined


def _row_has_owner_identifiers(row: dict, canon: dict[str, str]) -> bool:
    """True if CSV row can resolve to a HubSpot owner (id, email, or first+last name)."""
    if _normalize_owner_id(_cell(row, canon, "hubspot_owner_id")):
        return True
    if _cell(row, canon, "owner_email"):
        return True
    if _row_owner_name(row, canon):
        return True
    return False


def _skip_sales_code_cell(raw: str) -> bool:
    """Skip blank rows, section headers, and notes (e.g. cancelled-account banner rows)."""
    code = (raw or "").strip()
    if not code:
        return True
    low = code.lower()
    if len(code) > 80:
        return True
    if "cancelled" in low and ("account" in low or "sales code" in low):
        return True
    if low.startswith("inactive"):
        return True
    return False


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
def _read_owner_mapping_bundle() -> tuple[dict[str, dict[str, str]], frozenset[str]]:
    """
    Returns (by_code, inactive_codes).

    ``inactive_codes``: sales codes listed in the sheet with **no** owner id/email/name
    (e.g. cancelled partners). Those codes must not drive ``hubspot_owner_id``.
    Last data row wins for duplicate codes.
    """
    by_code: dict[str, dict[str, str]] = {}
    inactive: set[str] = set()
    csv_path = _OWNER_MAPPING_CSV
    if csv_path.is_file():
        try:
            text = csv_path.read_text(encoding="utf-8-sig")
            lines = text.splitlines()
            if lines:
                reader = csv.DictReader(lines)
                canon = _normalize_csv_fieldnames(reader.fieldnames)
                if canon.get("sales_code"):
                    for row in reader:
                        code_raw = _cell(row, canon, "sales_code")
                        if _skip_sales_code_cell(code_raw):
                            continue
                        code = _normalize_sales_code_key(code_raw)
                        if not code:
                            continue
                        if _row_has_owner_identifiers(row, canon):
                            inactive.discard(code)
                            by_code[code] = {
                                "hubspot_owner_id": _normalize_owner_id(
                                    _cell(row, canon, "hubspot_owner_id")
                                )
                                or "",
                                "owner_email": _cell(row, canon, "owner_email"),
                                "owner_name": _row_owner_name(row, canon),
                            }
                        else:
                            by_code.pop(code, None)
                            inactive.add(code)
        except (OSError, ValueError):
            pass
    if not by_code and not inactive:
        return _read_json_fallback(), frozenset()
    return by_code, frozenset(inactive)


def _read_mapping_table() -> dict[str, dict[str, str]]:
    return _read_owner_mapping_bundle()[0]


def is_sales_code_inactive_in_owner_map(sales_code: Optional[str]) -> bool:
    """
    True if ``sales_code`` appears in ``owner_mapping.csv`` as a row with no owner data
    (inactive / cancelled partner). Not set for unknown codes absent from the file.
    """
    if not sales_code:
        return False
    code = _normalize_sales_code_key(str(sales_code).strip())
    _, inactive = _read_owner_mapping_bundle()
    return code in inactive


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


def _owner_id_is_valid(owners: list, owner_id: str) -> bool:
    normalized = _normalize_owner_id(owner_id)
    if not normalized:
        return False
    return any(_normalize_owner_id(o.get("id")) == normalized for o in owners)


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

    Uses ``data/owner_mapping.csv`` when present; otherwise
    ``data/legacy/sales_code_owner_map.json``.

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

    if not hubspot_client:
        return _normalize_owner_id(row.get("hubspot_owner_id"))

    owners = _get_cached_owners(hubspot_client)
    oid = _normalize_owner_id(row.get("hubspot_owner_id"))
    if oid and _owner_id_is_valid(owners, oid):
        return oid
    oid = _owner_id_from_email(owners, row.get("owner_email") or "")
    if oid:
        return oid
    oid = _owner_id_from_display_name(owners, row.get("owner_name") or "")
    return oid
