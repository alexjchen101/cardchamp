"""
Field Mapping Utilities

Maps CoPilot data formats to HubSpot field formats.
"""

import re
from typing import Optional

from mcc_mapping import get_industry_from_mcc

# State code to full name mapping
STATE_CODE_TO_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "Washington, DC"
}


# Business-specific fields (excluded when using numbered properties for multi-business)
BUSINESS_SPECIFIC_FIELDS = {"company", "merchant_id", "monthly_processing_volume"}

# HubSpot ``point_of_sale`` catch-all when no 1:1 rule matches (add these options in the portal).
_POINT_OF_SALE_OTHER_LABELS = (
    "Other",
    "Other / Software - Not Listed",
    "Software - Not Listed",
)
# Last resort if the portal has not added an Other-style option yet (legacy).
_POINT_OF_SALE_LEGACY_FALLBACK = ("POS System",)


def point_of_sale_from_equipment_text(equipment_str: str):
    """Map equipment / order product text to a canonical POS label (for HubSpot matching)."""
    if not equipment_str:
        return None
    return logical_pos_label_for_equipment(equipment_str, "")


# ---------------------------------------------------------------------------
# HubSpot ``point_of_sale`` labels (must match portal option *labels* where possible).
# Rules are evaluated in order — first match wins. Expand as you see new catalog strings.
# ---------------------------------------------------------------------------
def _hubspot_pos_rules():
    """Return list of (hubspot_label, predicate). Predicate: (s_lower, raw_name, type_upper) -> bool"""

    def gw(t):
        return t == "GATEWAY"

    def not_gw(t):
        return t != "GATEWAY"

    def gwc(s: str) -> str:
        """Lowercase alphanumerics only — matches CoPilot names with spaces (e.g. ``ACCEPT BLUE GTW``)."""
        return re.sub(r"[^a-z0-9]", "", (s or "").lower())

    return [
        # (hubspot_option_label, predicate) — order matters; exact known models first,
        # then family buckets for long-tail variants. Unmatched → Other.
        (
            "CardPointe Virtual Terminal",
            lambda s, r, t: "cardpointe" in s
            and "gateway" in s
            and "integrated terminal" not in s,
        ),
        ("Payeezy Gateway", lambda s, r, t: gw(t) and "payeezy" in s),
        ("Authorize.net", lambda s, r, t: gw(t) and "authorize" in s),
        ("Paytrace Gateway", lambda s, r, t: gw(t) and "paytrace" in s),
        ("Moolah Gateway", lambda s, r, t: gw(t) and "moolah" in s),
        ("NMI", lambda s, r, t: gw(t) and ("nmi" in s or "network merchants" in s)),
        ("CyberSource", lambda s, r, t: gw(t) and "cybersource" in s),
        ("Worldnet", lambda s, r, t: gw(t) and "worldnet" in s),
        ("Element Virtual Terminal", lambda s, r, t: gw(t) and "element" in s and "virtual" in s),
        ("Transax", lambda s, r, t: gw(t) and ("transax" in s or "pineapple" in s)),
        ("iCheckGateway", lambda s, r, t: gw(t) and "icheck" in s),
        ("USA EPay", lambda s, r, t: gw(t) and "usa epay" in s),
        ("Open Edge", lambda s, r, t: gw(t) and "open edge" in s),
        ("Paymetric", lambda s, r, t: gw(t) and "paymetric" in s),
        ("Payjunction", lambda s, r, t: gw(t) and "payjunction" in gwc(s)),
        ("AcceptBlue", lambda s, r, t: gw(t) and "acceptblue" in gwc(s)),
        ("Clover Flex 4", lambda s, r, t: not_gw(t) and ("flex 4" in s or "flex4" in s) and ("clover" in s or "cardpointe" in s)),
        ("Clover Mini 3", lambda s, r, t: not_gw(t) and ("mini 3" in s or "mini3" in s) and ("clover" in s or "cardpointe" in s)),
        ("Mini 3 - CardPointe Integrated Terminal (US)", lambda s, r, t: not_gw(t) and "mini 3 - cardpointe" in s),
        ("Clover Compact - CardPointe Integrated (US)", lambda s, r, t: not_gw(t) and ("clover compact" in s or "compact - cardpointe" in s)),
        ("Clover Mini", lambda s, r, t: not_gw(t) and ("clover" in s or ("cardpointe" in s and "integrated terminal" in s)) and "mini" in s and "mini 3" not in s and "mini3" not in s),
        ("Clover Flex", lambda s, r, t: not_gw(t) and ("clover" in s or "cardpointe" in s) and "flex" in s and "flex 4" not in s and "flex4" not in s),
        ("Clover Compact", lambda s, r, t: not_gw(t) and "compact" in s and ("clover" in s or "cardpointe" in s)),
        ("Clover Station Duo", lambda s, r, t: not_gw(t) and "clover" in s and "station" in s and "duo" in s),
        ("Clover Station Solo", lambda s, r, t: not_gw(t) and "clover" in s and "station" in s and "solo" in s),
        ("Clover Duo", lambda s, r, t: not_gw(t) and "duo" in s and "clover" in s),
        ("Clover Solo", lambda s, r, t: not_gw(t) and "solo" in s and "clover" in s),
        ("Clover Go", lambda s, r, t: not_gw(t) and ("clover go" in s or " clover go" in s)),
        ("Clover Flex Pocket", lambda s, r, t: not_gw(t) and "clover" in s and "flex pocket" in s),
        ("Clover Kiosk", lambda s, r, t: not_gw(t) and "clover" in s and "kiosk" in s),
        ("Clover Kitchen Display", lambda s, r, t: not_gw(t) and "clover" in s and ("kitchen display" in s or "kds" in s)),
        (
            "Clover Barcode Scanner",
            lambda s, r, t: not_gw(t)
            and "clover" in s
            and (
                "barcode" in s
                or "zebra" in s
                or "honeywell" in s
                or "symbol" in s
                or "ds9308" in s
                or "ds2208" in s
                or "ds7708" in s
                or "ds457" in s
                or "ds8108" in s
            ),
        ),
        ("Clover Cash Drawer", lambda s, r, t: not_gw(t) and "clover" in s and "cash drawer" in s),
        ("Clover Virtual Terminal", lambda s, r, t: not_gw(t) and "clover virtual" in s),
        ("Clover Station", lambda s, r, t: not_gw(t) and "clover" in s and "station" in s),
        ("CardPointe Ingenico Lane3000", lambda s, r, t: not_gw(t) and ("lane 3000" in s or "lane3000" in s)),
        ("CardPointe Ingenico iPP320", lambda s, r, t: not_gw(t) and ("ipp320" in s or "ip 320" in s) and "ingenico" in s),
        ("Ingenico Lane/7000", lambda s, r, t: not_gw(t) and "ingenico" in s and ("lane/7000" in s or "lane 7000" in s)),
        ("Ingenico Lane/3600", lambda s, r, t: not_gw(t) and "ingenico" in s and ("lane/3600" in s or "lane 3600" in s)),
        ("Ingenico Desk1600", lambda s, r, t: not_gw(t) and "ingenico" in s and ("desk1600" in s or "desk 1600" in s)),
        ("Ingenico Desk2600", lambda s, r, t: not_gw(t) and "ingenico" in s and ("desk2600" in s or "desk 2600" in s)),
        (
            "CardPointe Desk2600",
            lambda s, r, t: not_gw(t)
            and ("desk 2600" in s or "desk2600" in s)
            and ("cardpointe" in s or "ingenico" in s),
        ),
        (
            "Ingenico Desk3500",
            lambda s, r, t: not_gw(t)
            and "ingenico" in s
            and ("desk 3500" in s or "desk3500" in s),
        ),
        (
            "CardPointe Desk3500",
            lambda s, r, t: not_gw(t)
            and ("cardpointe desk3500" in s or "cardpointe desk 3500" in s),
        ),
        ("Ingenico ISC 250 Touch", lambda s, r, t: not_gw(t) and ("isc 250" in s or "isc250" in s) and "touch" in s),
        ("Ingenico iCT250", lambda s, r, t: not_gw(t) and ("ict250" in s or "ict 250" in s)),
        ("Ingenico iCT220", lambda s, r, t: not_gw(t) and ("ict220" in s or "ict 220" in s)),
        ("Ingenico Lane Series", lambda s, r, t: not_gw(t) and "ingenico" in s and "lane" in s),
        ("Ingenico Desk Series", lambda s, r, t: not_gw(t) and "ingenico" in s and "desk" in s),
        ("Ingenico iCT Series", lambda s, r, t: not_gw(t) and "ingenico" in s and "ict" in s),
        ("Verifone VX520", lambda s, r, t: not_gw(t) and ("vx520" in s or "vx 520" in s)),
        ("Verifone VX510", lambda s, r, t: not_gw(t) and ("vx510" in s or "vx 510" in s)),
        ("VeriFone V200c", lambda s, r, t: not_gw(t) and ("v200c" in s or "v200 c" in s)),
        ("Verifone V400m", lambda s, r, t: not_gw(t) and ("v400m" in s or "v400 m" in s)),
        ("Verifone Terminal", lambda s, r, t: not_gw(t) and ("verifone" in s or "verifone" in r.lower())),
        ("PAX A80", lambda s, r, t: not_gw(t) and "pax" in s and "a80" in s),
        ("PAX S90", lambda s, r, t: not_gw(t) and "pax" in s and "s90" in s),
        ("PAX Terminal", lambda s, r, t: not_gw(t) and "pax" in s),
        ("Dejavoo Z11", lambda s, r, t: not_gw(t) and "dejavoo" in s and "z11" in s),
        ("Dejavoo Z8", lambda s, r, t: not_gw(t) and "dejavoo" in s and "z8" in s),
        ("Dejavoo Z9", lambda s, r, t: not_gw(t) and "dejavoo" in s and "z9" in s),
        ("Dejavoo P1", lambda s, r, t: not_gw(t) and "dejavoo" in s and "p1" in s),
        ("Dejavoo P3", lambda s, r, t: not_gw(t) and "dejavoo" in s and "p3" in s),
        (
            "Dejavoo P iPOS Virtual Terminal",
            lambda s, r, t: not_gw(t)
            and "dejavoo" in s
            and (
                "p i pos" in s
                or "p-ipos" in s
                or "p ipos" in s
                or "pi pos" in s
                or "pipos" in gwc(s)
            ),
        ),
        ("Dejavoo Terminal", lambda s, r, t: not_gw(t) and "dejavoo" in s),
        ("FD130", lambda s, r, t: not_gw(t) and ("fd130" in s or "fd 130" in s)),
        ("FD150", lambda s, r, t: not_gw(t) and "fd150" in s),
        ("FD200TI", lambda s, r, t: not_gw(t) and ("fd200ti" in s or "fd200 ti" in s)),
        ("FD200", lambda s, r, t: not_gw(t) and "fd200" in s),
        ("FD400 Wireless", lambda s, r, t: not_gw(t) and "fd400" in s),
        ("FD410", lambda s, r, t: not_gw(t) and "fd410" in s),
        ("FD50TI", lambda s, r, t: not_gw(t) and "fd50" in s and "ti" in s),
        ("FD50", lambda s, r, t: not_gw(t) and "fd50" in s),
        ("FD55", lambda s, r, t: not_gw(t) and "fd55" in s),
        ("FD100TI", lambda s, r, t: not_gw(t) and ("fd100ti" in s or "fd100 ti" in s)),
        ("FD100", lambda s, r, t: not_gw(t) and "fd100" in s),
        ("FD300TI", lambda s, r, t: not_gw(t) and "fd300ti" in s),
        ("Magtek Reader", lambda s, r, t: not_gw(t) and ("magtek" in s or "idynamo" in s)),
        ("IDTech Reader", lambda s, r, t: not_gw(t) and ("idtech" in s or "vp3350" in s)),
        ("Credit Card Machine - General", lambda s, r, t: not_gw(t) and ("idtech" in s or "vp3350" in s or "usb wedge" in s or ("encrypt" in s and "usb" in s))),
        ("Hypercom", lambda s, r, t: not_gw(t) and "hypercom" in s),
        ("Poynt", lambda s, r, t: not_gw(t) and "poynt" in s),
        ("SpotOn POS", lambda s, r, t: not_gw(t) and ("spoton" in s or "spot on" in s)),
        ("Payroc Terminal +", lambda s, r, t: not_gw(t) and "payroc" in s),
        ("Genius POS", lambda s, r, t: not_gw(t) and "genius" in s and "pos" in s),
        ("Toast", lambda s, r, t: not_gw(t) and "toast" in s),
        ("Shopify", lambda s, r, t: not_gw(t) and "shopify" in s),
        ("Square POS System", lambda s, r, t: not_gw(t) and "square" in s and "pos" in s),
        ("Square", lambda s, r, t: not_gw(t) and "square" in s),
        ("Lightspeed POS System", lambda s, r, t: not_gw(t) and "lightspeed" in s),
        ("TouchBistro", lambda s, r, t: not_gw(t) and "touchbistro" in s),
        ("Revel POS", lambda s, r, t: not_gw(t) and "revel" in s),
        ("ShopKeep POS", lambda s, r, t: not_gw(t) and "shopkeep" in s),
        ("WooCommerce", lambda s, r, t: not_gw(t) and "woocommerce" in s),
        ("Magento", lambda s, r, t: not_gw(t) and "magento" in s),
        ("MindBody", lambda s, r, t: not_gw(t) and "mindbody" in s),
        ("CardPointe API", lambda s, r, t: not_gw(t) and "clover" not in s and "cardpointe api" in s),
        ("CardPointe Mobile", lambda s, r, t: not_gw(t) and "cardpointe mobile" in s),
    ]


_HUBSPOT_POS_RULES_CACHE = None


def _get_hubspot_pos_rules():
    global _HUBSPOT_POS_RULES_CACHE
    if _HUBSPOT_POS_RULES_CACHE is None:
        _HUBSPOT_POS_RULES_CACHE = _hubspot_pos_rules()
    return _HUBSPOT_POS_RULES_CACHE


def logical_pos_label_for_equipment(equipment_name: str, equipment_type_cd: str = "") -> str:
    """
    Map catalog line → HubSpot ``point_of_sale`` option label (1:1 rules only).

    If no rule matches, returns **Other** (resolved to the portal option via
    ``_resolve_point_of_sale_other_value`` in the multiselect builder — not guessed into
    random gateway buckets).
    """
    raw = equipment_name or ""
    s = raw.lower()
    t = (equipment_type_cd or "").upper()
    for label, pred in _get_hubspot_pos_rules():
        try:
            if pred(s, raw, t):
                return label
        except Exception:
            continue
    return "Other"


def _pos_match_candidates_for_line(equipment_name: str, equipment_type_cd: str) -> list:
    """
    Single primary label from 1:1 rules (or **Other**). Matcher uses **exact** HubSpot
    option label/value only — no fuzzy collision across thousands of gateways.
    """
    primary = logical_pos_label_for_equipment(equipment_name, equipment_type_cd)
    return [primary] if primary else []


def _hubspot_option_rows(property_definition: Optional[dict]) -> list:
    if not property_definition:
        return []
    opts = property_definition.get("options") or []
    rows = []
    for o in opts:
        if not isinstance(o, dict):
            continue
        lab = (o.get("label") or "").strip()
        val = (o.get("value") or lab or "").strip()
        if not lab and not val:
            continue
        rows.append({"value": val, "label": lab, "label_l": lab.lower(), "value_l": val.lower()})
    return rows


def _resolve_point_of_sale_other_value(property_definition: Optional[dict]) -> Optional[str]:
    """
    HubSpot option **value** for the catch-all bucket when CoPilot text has no 1:1 rule.

    Tries labels in order; add **Other** (or ``Other / Software - Not Listed``) to the
    ``point_of_sale`` property in HubSpot so ops can refine options over time.
    """
    prefer = _POINT_OF_SALE_OTHER_LABELS + _POINT_OF_SALE_LEGACY_FALLBACK
    hub_rows = _hubspot_option_rows(property_definition)
    if not hub_rows:
        return prefer[0]

    def pick_value(row):
        return row["value"] or row["label"]

    for lbl in prefer:
        c = lbl.strip().lower()
        for row in hub_rows:
            if row["label_l"] == c or row["value_l"] == c:
                return pick_value(row)
        for row in hub_rows:
            if c in row["label_l"] or row["label_l"] in c:
                return pick_value(row)
    return pick_value(hub_rows[0]) if hub_rows else prefer[0]


def match_candidates_to_hubspot_option_value(
    candidates: list,
    property_definition: Optional[dict],
    *,
    equipment_is_gateway: bool = False,
) -> Optional[str]:
    """
    Pick HubSpot enumeration **value** for PATCH (multi-select uses values, semicolon-joined).

    **Exact label/value match only** for each candidate — avoids accidentally mapping
    unknown gateways into the wrong checkbox. Callers should send **Other** when rules
    do not match; if the portal has no matching option, use
    ``_resolve_point_of_sale_other_value``.

    ``equipment_is_gateway`` is kept for call-site compatibility; matching no longer
    uses gateway-specific fuzzy logic.
    """
    _ = equipment_is_gateway
    rows = _hubspot_option_rows(property_definition)
    if not rows:
        return (candidates[0] if candidates else None)

    def pick_value(row):
        return row["value"] or row["label"]

    for cand in candidates:
        if not cand:
            continue
        c = cand.strip().lower()
        for row in rows:
            if row["label_l"] == c or row["value_l"] == c:
                return pick_value(row)
    return None


def _is_gateway_equipment_line(equipment_name: str, equipment_type_cd: str) -> bool:
    """CoPilot line behaves like a gateway for HubSpot matching (type or CardPointe gateway name)."""
    t = (equipment_type_cd or "").upper()
    if t == "GATEWAY":
        return True
    s = (equipment_name or "").lower()
    if "gateway" in s and "cardpointe" in s and "integrated terminal" not in s:
        return True
    return False


def _is_cardpointe_gateway_line(equipment_name: str, equipment_type_cd: str) -> bool:
    """
    True only for **CardPointe Gateway** SKUs (e.g. RapidConnect North), not every CoPilot
    ``type=GATEWAY`` row (e.g. Clover Software RC stays even when we omit other gateways).
    """
    s = (equipment_name or "").lower()
    if "integrated terminal" in s:
        return False
    return "cardpointe" in s and "gateway" in s


def _filter_equipment_lines_for_pos_display(
    lines: list,
) -> list:
    """
    Per-merchant rule:
    - If **only** CardPointe Gateway lines exist → keep them (map to CardPointe Virtual Terminal, etc.).
    - If CardPointe Gateway **and** any other line → **drop** CardPointe Gateway rows only;
      map everything else (other gateways, terminals, Clover software, …).
    """
    if not lines:
        return []
    cpe = [ln for ln in lines if _is_cardpointe_gateway_line(ln[1], ln[2])]
    rest = [ln for ln in lines if not _is_cardpointe_gateway_line(ln[1], ln[2])]
    if rest:
        return rest
    return cpe


def _order_lines_unique_by_eid(merchant_api, mid: str, catalog: dict) -> list:
    """Non-canceled order rows for one merchant → unique (eid, name, type)."""
    lines = []
    try:
        rows = merchant_api.list_all_orders(mid)
    except Exception:
        rows = []
    by_eid = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if (row.get("orderStatusCd") or "").upper() == "CANCELED":
            continue
        eid = row.get("equipmentId")
        if eid is None:
            continue
        try:
            eid_int = int(eid)
        except (TypeError, ValueError):
            continue
        meta = catalog.get(eid_int) or {}
        name = meta.get("name") or f"Equipment {eid_int}"
        typ = meta.get("type") or ""
        by_eid[eid_int] = (eid_int, name, typ)
    return list(by_eid.values())


def _profile_equipment_lines(mdata, synth_start: int) -> tuple:
    """Fallback when no orders: (eid, make/model label, type guess)."""
    out = []
    synth = synth_start
    merchant = (mdata or {}).get("merchant") or {}
    for item in merchant.get("equipment") or []:
        if not isinstance(item, dict):
            continue
        label = f"{item.get('make', '')} {item.get('model', '')}".strip()
        if not label:
            continue
        s = label.lower()
        typ_guess = "GATEWAY" if ("gateway" in s and "cardpointe" in s and "integrated terminal" not in s) else ""
        out.append((-(synth + 1), label, typ_guess))
        synth += 1
    return out, synth


def build_point_of_sale_multiselect_value(
    merchant_api,
    copilot_id_merchant_data_pairs,
    point_of_sale_property_definition: Optional[dict] = None,
) -> Optional[str]:
    """
    HubSpot multi-select checkbox: semicolon-separated **option values** from schema.

    **CardPointe Gateway rule (per CoPilot merchant ID):** If orders include a **CardPointe
    Gateway** SKU and **any** other line, those CardPointe Gateway rows are **omitted** for POS
    mapping. Other ``type=GATEWAY`` products (e.g. Clover Software RC) are kept. If only
    CardPointe Gateway lines exist, map to CardPointe Virtual Terminal as before.
    Multiple merchant IDs are evaluated **independently**, then merged.
    """
    if not merchant_api or not copilot_id_merchant_data_pairs:
        return None
    try:
        catalog = merchant_api.get_equipment_catalog_map()
    except Exception:
        catalog = {}

    merged_by_eid = {}  # eid -> (name, typ) after per-merchant filter

    for copilot_id, mdata in copilot_id_merchant_data_pairs:
        mid = str(copilot_id).strip()
        lines = _order_lines_unique_by_eid(merchant_api, mid, catalog)
        if not lines:
            continue
        filtered = _filter_equipment_lines_for_pos_display(lines)
        for eid_int, name, typ in filtered:
            merged_by_eid[eid_int] = (name, typ)

    synth = 0
    if not merged_by_eid:
        for _cid, mdata in copilot_id_merchant_data_pairs:
            plines, synth = _profile_equipment_lines(mdata, synth)
            filtered = _filter_equipment_lines_for_pos_display(plines)
            for eid_int, name, typ in filtered:
                merged_by_eid[eid_int] = (name, typ)

    if not merged_by_eid:
        return None

    hubspot_values = []
    seen_vals = set()
    for _eid, (name, typ) in sorted(merged_by_eid.items(), key=lambda x: (x[1][0] or "").lower()):
        candidates = _pos_match_candidates_for_line(name, typ)
        is_gw = _is_gateway_equipment_line(name, typ)
        v = match_candidates_to_hubspot_option_value(
            candidates, point_of_sale_property_definition, equipment_is_gateway=is_gw
        )
        if not v:
            v = _resolve_point_of_sale_other_value(point_of_sale_property_definition)
        if v and v not in seen_vals:
            seen_vals.add(v)
            hubspot_values.append(v)

    if not hubspot_values:
        return None
    return ";".join(hubspot_values)


def extract_equipment_text_from_order_list(resp):
    """
    Pull product / equipment descriptions from GET /order/list response.
    Schema may vary; handles common list + line-item shapes.
    """
    if not resp or not isinstance(resp, dict):
        return None
    names = []
    seen = set()

    def add(val):
        if isinstance(val, str) and len(val.strip()) > 2:
            t = val.strip()
            if t not in seen:
                seen.add(t)
                names.append(t)

    for key in ("orders", "orderList", "content", "results", "records", "data", "elements"):
        items = resp.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            for nk in (
                "productName",
                "productDesc",
                "productDescription",
                "itemDescription",
                "description",
                "name",
            ):
                add(item.get(nk))
            for sub in ("orderLines", "lineItems", "items", "orderLineItems", "orderLine"):
                lines = item.get(sub)
                if not isinstance(lines, list):
                    continue
                for line in lines:
                    if not isinstance(line, dict):
                        continue
                    for nk in (
                        "productName",
                        "productDesc",
                        "description",
                        "name",
                    ):
                        add(line.get(nk))
    return " ; ".join(names) if names else None


def build_ordered_hardware_display(merchant_api, copilot_id_merchant_data_pairs):
    """
    Resolve all ordered hardware across one or more CoPilot merchant IDs.

    Uses GET /order/list (all pages) + equipmentCatalog/list to map equipmentId
    to equipmentName. Skips CANCELED orders. Same **CardPointe Gateway omission** rule as POS.

    If no order lines exist anywhere, falls back to merchant.equipment make/model per business.

    Args:
        merchant_api: copilot MerchantAPI instance
        copilot_id_merchant_data_pairs: list of (copilot_merchant_id_str, merchant_payload)

    Returns:
        Semicolon-separated string for HubSpot Point of Sale / Software, or None.
    """
    if not merchant_api or not copilot_id_merchant_data_pairs:
        return None
    try:
        catalog = merchant_api.get_equipment_catalog_map()
    except Exception:
        catalog = {}
    name_qty = {}

    for copilot_id, mdata in copilot_id_merchant_data_pairs:
        mid = str(copilot_id).strip()
        try:
            rows = merchant_api.list_all_orders(mid)
        except Exception:
            rows = []
        # Build per-line qty, then filter gateway rows per merchant before aggregating
        eid_qty = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            st = (row.get("orderStatusCd") or "").upper()
            if st == "CANCELED":
                continue
            eid = row.get("equipmentId")
            if eid is None:
                continue
            try:
                eid_int = int(eid)
            except (TypeError, ValueError):
                continue
            try:
                q = int(row.get("quantity") or 1)
            except (TypeError, ValueError):
                q = 1
            eid_qty[eid_int] = eid_qty.get(eid_int, 0) + q

        line_tuples = []
        for eid_int, qty in eid_qty.items():
            meta = catalog.get(eid_int) or {}
            name = meta.get("name") or f"Equipment {eid_int}"
            typ = meta.get("type") or ""
            line_tuples.append((eid_int, name, typ))
        kept = _filter_equipment_lines_for_pos_display(line_tuples)
        kept_eids = {t[0] for t in kept}
        for eid_int, qty in eid_qty.items():
            if eid_int not in kept_eids:
                continue
            meta = catalog.get(eid_int) or {}
            name = meta.get("name") or f"Equipment {eid_int}"
            name_qty[name] = name_qty.get(name, 0) + qty

    if not name_qty:
        synth = 0
        for _cid, mdata in copilot_id_merchant_data_pairs:
            plines, synth = _profile_equipment_lines(mdata, synth)
            kept = _filter_equipment_lines_for_pos_display(plines)
            for eid_int, label, _typ in kept:
                name_qty[label] = name_qty.get(label, 0) + 1

    if not name_qty:
        return None

    def sort_key(item):
        name, qty = item
        return (-qty, name.lower())

    parts = []
    for name, q in sorted(name_qty.items(), key=sort_key):
        parts.append(f"{name} (×{q})" if q > 1 else name)
    display = " ; ".join(parts)
    if len(display) > 500:
        display = display[:497] + "..."
    return display


def map_copilot_to_hubspot(
    merchant_data,
    current_contact_props=None,
    exclude_business_specific=False,
    order_list_response=None,
    point_of_sale_value=None,
):
    """
    Map CoPilot merchant data to HubSpot contact fields.
    Uses existing HubSpot properties (no custom fields needed).
    
    Args:
        merchant_data: Full merchant data from CoPilot API
        current_contact_props: Existing HubSpot contact properties (to check what not to overwrite)
        exclude_business_specific: If True, omit company/merchant_id/monthly_processing_volume
            (use map_business_to_hubspot for numbered fields instead)
        order_list_response: Optional GET /order/list JSON when merchant.equipment is empty
        point_of_sale_value: Pre-built HubSpot multi-select (option values, semicolon-separated)
        
    Returns:
        dict: HubSpot field updates
    """
    if current_contact_props is None:
        current_contact_props = {}
    
    merchant = merchant_data.get("merchant", {})
    ownership = merchant.get("ownership", {}).get("owner", {})
    demographic = merchant.get("demographic", {})
    processing = merchant.get("processing", {})
    
    updates = {}
    
    # ========== BASIC INFO ==========
    
    # Company Name (DBA) — title case per word (CoPilot often ALL CAPS)
    if merchant.get("dbaName"):
        updates["company"] = _normalize_company_name(str(merchant["dbaName"]).strip())

    # Owner Name - split into first/last (title case per word; CoPilot often ALL CAPS)
    owner_name = ownership.get("ownerName")
    if owner_name:
        parts = owner_name.strip().split(None, 1)  # Split on first space
        if len(parts) >= 1:
            updates["firstname"] = _normalize_company_name(parts[0])
        if len(parts) >= 2:
            updates["lastname"] = _normalize_company_name(parts[1])
    
    # Email / phone now overwrite from the primary (oldest) merchant
    if ownership.get('ownerEmail'):
        updates['email'] = ownership['ownerEmail']
    
    if ownership.get('ownerPhone'):
        updates['phone'] = ownership['ownerPhone']
    
    # Mobile Phone
    if ownership.get('ownerMobilePhone'):
        updates['mobilephone'] = ownership['ownerMobilePhone']
    
    # ========== ADDRESS ==========
    
    business_address = demographic.get("businessAddress", {})
    
    # State - convert code to full name
    state_code = business_address.get('stateCd')
    if state_code:
        updates['state'] = STATE_CODE_TO_NAME.get(state_code, state_code)
    
    # Zip
    if business_address.get('zip'):
        updates['zip'] = business_address['zip']
    
    # City
    if business_address.get('city'):
        updates["city"] = _title_case_address_line(business_address["city"])

    # Address (full formatted)
    if business_address:
        address_parts = []
        if business_address.get("address1"):
            address_parts.append(_title_case_address_line(business_address["address1"]))
        if business_address.get("address2"):
            address_parts.append(_title_case_address_line(business_address["address2"]))
        if address_parts:
            updates["address"] = ", ".join(address_parts)
    
    # ========== BUSINESS INFO ==========
    
    # Website
    website = demographic.get("websiteAddress")
    if website:
        updates['website'] = website
    
    platform_details = processing.get("platformDetails", {})
    
    # MCC → HubSpot ``industry_mcc`` (Industry (MCC) multi-checkbox only)
    mcc_id = platform_details.get("mccId")
    if mcc_id:
        updates["industry_mcc"] = get_industry_from_mcc(mcc_id)
    
    # Platform - Map backend platform code to HubSpot values
    backend_platform = platform_details.get("backEndPlatformCd")
    if backend_platform:
        # Map CoPilot backend platform codes to HubSpot values
        # FDNOB = First Data North, FDOMA = First Data Omaha, etc.
        platform_map = {
            "FDNOB": "North",      # First Data North
            "RCNOF": "North",      # RapidConnect North  
            "FDOMA": "Omaha",      # First Data Omaha
            "RCOMA": "Omaha",      # RapidConnect Omaha
            "TSYS": "TSYS CardConnect",
            "BUYPASS": "Buypass"
        }
        # Use backend platform, default to North if not found
        updates['platform'] = platform_map.get(backend_platform, "North")
    
    # Point of Sale / Software (HubSpot multi-select checkbox)
    if point_of_sale_value:
        updates["point_of_sale"] = point_of_sale_value
    else:
        equipment_str = None
        equipment = merchant.get("equipment") or []
        if equipment:
            first_item = equipment[0]
            make = first_item.get("make", "")
            model = first_item.get("model", "")
            equipment_str = f"{make} {model}".strip()
        elif order_list_response:
            equipment_str = extract_equipment_text_from_order_list(order_list_response)
        if equipment_str:
            pos = point_of_sale_from_equipment_text(equipment_str)
            if pos:
                updates["point_of_sale"] = pos
    
    # ========== FINANCIAL INFO ==========
    
    # Monthly Processing Volume - map to HubSpot ranges
    volume_details = processing.get("volumeDetails", {})
    avg_volume = volume_details.get("averageMonthlyVolume")
    if avg_volume:
        updates['monthly_processing_volume'] = volume_to_range(avg_volume)
    
    # ========== IDs (IMPORTANT DISTINCTION) ==========
    
    # Merchant Account Number - The PROCESSING MID (assigned after boarding)
    # NOTE: Three different IDs exist:
    #   1. copilot_account (HubSpot) = Manually entered to FIND merchant (e.g., 170761464)
    #   2. merchantId (CoPilot) = Internal CoPilot ID (e.g., 170761464)
    #   3. merchant_id (HubSpot) = PROCESSING MID from backEndMid (e.g., 496615994886)
    #
    # We sync the PROCESSING MID (backEndMid), not the internal merchantId
    backend_mid = platform_details.get("backEndMid")
    if backend_mid:
        updates['merchant_id'] = str(backend_mid)
    
    # ACH Provider → HubSpot ``ach_provider``: see ``get_ach_provider_hubspot_value`` in sync scripts
    # Contact / deal owner ``hubspot_owner_id``: resolved in sync from ``data/owner_mapping.csv`` (or legacy JSON) via ``sales_code_owners``.

    # ========== PERSONAL INFO ==========
    
    # Date of Birth - Convert to proper format if needed
    owner_dob = ownership.get("ownerDob")
    if owner_dob:
        # CoPilot format: "05/01/1961"
        # HubSpot might need YYYY-MM-DD, but let's try as-is first
        updates['date_of_birth'] = owner_dob
    
    if exclude_business_specific:
        for f in BUSINESS_SPECIFIC_FIELDS:
            updates.pop(f, None)
    
    return updates


def extract_address_updates_from_merchant_data(merchant_data):
    """
    Newest-merchant fields (CSV): street address, city, state, zip only.

    ``mobilephone`` / ``phone`` stay on the OG merchant via ``map_copilot_to_hubspot``.
    """
    merchant = merchant_data.get("merchant", {}) if isinstance(merchant_data, dict) else {}
    demographic = merchant.get("demographic", {})
    business_address = demographic.get("businessAddress", {}) or {}
    out = {}

    state_code = business_address.get("stateCd")
    if state_code:
        out["state"] = STATE_CODE_TO_NAME.get(state_code, state_code)
    if business_address.get("zip"):
        out["zip"] = business_address["zip"]
    if business_address.get("city"):
        out["city"] = _title_case_address_line(business_address["city"])
    parts = []
    if business_address.get("address1"):
        parts.append(_title_case_address_line(business_address["address1"]))
    if business_address.get("address2"):
        parts.append(_title_case_address_line(business_address["address2"]))
    if parts:
        out["address"] = ", ".join(parts)
    return out


def extract_deal_amount(merchant_data):
    """Extract deal amount from CoPilot data"""
    processing = merchant_data.get("merchant", {}).get("processing", {})
    volume_details = processing.get("volumeDetails", {})
    return volume_details.get("averageMonthlyVolume", 0)


def extract_sales_code(merchant_data: dict) -> Optional[str]:
    """CoPilot ``merchant.salesCode`` → HubSpot ``sales_code`` (plain string)."""
    if not merchant_data:
        return None
    code = (merchant_data.get("merchant") or {}).get("salesCode")
    if code is None:
        return None
    s = str(code).strip()
    return s or None


def extract_equipment_list(merchant_data):
    """Extract equipment/POS from CoPilot"""
    equipment = merchant_data.get("merchant", {}).get("equipment", [])
    if not equipment:
        return None
    
    equipment_list = []
    for item in equipment:
        make = item.get("make", "")
        model = item.get("model", "")
        if make and model:
            equipment_list.append(f"{make} {model}")
        elif make:
            equipment_list.append(make)
        elif model:
            equipment_list.append(model)
    
    return "; ".join(equipment_list) if equipment_list else None


def has_flat_rate_pricing(merchant_data: dict) -> bool:
    """
    True if the merchant uses flat-rate pricing in CoPilot.

    Source: ``GET /merchant/{merchantId}`` → ``merchant.pricing.flatPricing``.
    When this key is present and truthy (object with rates, etc.), flat pricing applies.
    """
    pricing = merchant_data.get("merchant", {}).get("pricing") or {}
    return bool(pricing.get("flatPricing"))


def get_pricing_type(merchant_data):
    """
    Extract pricing type from CoPilot (flat vs IC+ vs swipe/non-swipe).

    Returned strings match HubSpot ``pricing_type`` option **labels** in this portal:
    Flat Rate, IC Plus, Swiped/Non-Swiped (not legacy names like ``Flat Pricing``).
    """
    pricing = merchant_data.get("merchant", {}).get("pricing", {})

    if pricing.get("flatPricing"):
        return "Flat Rate"
    if pricing.get("icPlusPricing"):
        return "IC Plus"
    if pricing.get("swipeNonSwipePricing"):
        return "Swiped/Non-Swiped"

    return None


def _truthy_bluechex_flag(value) -> bool:
    """CoPilot sometimes returns booleans; treat common string/number sentinels as on."""
    if value is True:
        return True
    if value in (False, None, "", 0, 0.0):
        return False
    if isinstance(value, str) and value.strip().upper() in ("Y", "YES", "TRUE", "1", "ON"):
        return True
    if isinstance(value, (int, float)) and value != 0:
        return True
    return bool(value)


def check_ach_enabled(merchant_data: dict) -> bool:
    """
    Infer whether ACH (BlueChex / “ACH from Fiserv”) is in use for this merchant.

    CoPilot generally **omits** ``processing.blueChexSecOptions`` and
    ``processing.blueChexSecVolume`` when ACH is not set up; when either appears with
    real content, we treat that as a reliable proxy for ACH being enabled.

    Signals (first match wins):
    - ``blueChexSecOptions``: non-empty dict and at least one flag is truthy, **or**
      non-empty dict with keys (enrollment/toggles present even if currently false).
    - ``blueChexSecVolume``: non-empty list/tuple, or non-empty dict (volume / activity rows).

    Missing keys, ``null``, or ``{}`` / empty collections → not enabled.
    """
    processing = merchant_data.get("merchant", {}).get("processing")
    if not isinstance(processing, dict):
        return False

    opts = processing.get("blueChexSecOptions")
    if isinstance(opts, dict) and opts:
        if any(_truthy_bluechex_flag(v) for v in opts.values()):
            return True
        # Non-empty options object (toggles / enrollment) but all flags off — still on ACH product
        return True
    elif opts is not None and not isinstance(opts, dict) and bool(opts):
        return True

    vol = processing.get("blueChexSecVolume")
    if isinstance(vol, (list, tuple)) and len(vol) > 0:
        return True
    if isinstance(vol, dict) and len(vol) > 0:
        return True

    return False


def get_ach_provider_hubspot_value(
    merchant_data_list: list,
    property_definition: Optional[dict] = None,
) -> Optional[str]:
    """
    Value for HubSpot single-select ``ach_provider`` when BlueChex ACH is inferred.

    CSV rule: if ACH from Fiserv is enabled → option **Fiserv ACH**.
    Uses property definition option ``value`` when it matches the label.
    """
    if not merchant_data_list:
        return None
    if not any(check_ach_enabled(md) for md in merchant_data_list):
        return None
    v = _option_value_for_label("Fiserv ACH", property_definition)
    return v if v else "Fiserv ACH"


def get_copilot_accounts_from_contact(contact_props):
    """
    Parse CoPilot merchant IDs from HubSpot ``copilot_account`` only (single property).

    Format: ``x / y / z`` where **x = newest**, **z = oldest (OG)**. Split on ``/`` (spaces optional).
    Returns IDs with **oldest first** for processing (OG personal fields, etc.).
    """
    ids = []
    single = (contact_props.get("copilot_account") or "").strip()
    if not single:
        return ids
    for part in re.split(r"\s*/\s*", single):
        part = part.strip()
        if part:
            ids.append(part)
    ids.reverse()
    return ids


def _title_case_address_token(token: str) -> str:
    """One word token: 123 stays 123; MARION-WALDO -> Marion-Waldo; N -> N."""
    if not token:
        return token
    if token.isdigit():
        return token
    if "-" in token:
        return "-".join(_title_case_address_token(p) for p in token.split("-"))
    if len(token) == 1:
        return token.upper()
    return token[0].upper() + token[1:].lower()


def _title_case_address_line(line: str) -> str:
    """Street or city line from CoPilot (often ALL CAPS) → easier to read title-style."""
    if not line:
        return line
    return " ".join(_title_case_address_token(t) for t in str(line).strip().split())


def _normalize_company_name(name: str) -> str:
    """
    Normalize company/DBA name to Title Case per word.
    Example: 'PRESTON AUTO GROUP' -> 'Preston Auto Group'
    """
    if not name:
        return name
    # Split on whitespace and capitalize each token
    parts = []
    for token in name.split():
        parts.append(token[0].upper() + token[1:].lower() if len(token) > 1 else token.upper())
    return " ".join(parts)


def get_company_names_slash_separated(merchant_data_list):
    """
    Build slash-separated company names for multi-business.
    e.g. "Preston A / Preston B / Preston C"
    
    Args:
        merchant_data_list: List of merchant data dicts from CoPilot
        
    Returns:
        str or None: "Name1 / Name2 / Name3" or None if empty
    """
    names = []
    for md in merchant_data_list:
        dba = md.get("merchant", {}).get("dbaName")
        if dba:
            names.append(_normalize_company_name(str(dba).strip()))
    return " / ".join(names) if names else None


def volume_to_range(avg_volume):
    """Map numeric volume to HubSpot dropdown range."""
    if not avg_volume:
        return None
    if avg_volume < 5000:
        return "Under 5K"
    elif avg_volume < 20000:
        return "5-20K"
    elif avg_volume < 50000:
        return "20-50K"
    elif avg_volume < 100000:
        return "50-100K"
    elif avg_volume < 500000:
        return "100-500K"
    else:
        return "500K+"


def get_mcc_code(merchant_data):
    """Extract MCC code"""
    processing = merchant_data.get("merchant", {}).get("processing", {})
    platform_details = processing.get("platformDetails", {})
    return str(platform_details.get("mccId", "")) if platform_details.get("mccId") else None


def get_cash_discount(merchant_data):
    """
    Determine if merchant uses Cash Discount / flat-rate style pricing (per CoPilot ``flatPricing``).
    
    Returns:
        str: "Yes" if ``merchant.pricing.flatPricing`` is set, "No" otherwise
        
    Note: HubSpot field 'cash_discount' doesn't exist yet - needs to be created as custom field
    """
    return "Yes" if has_flat_rate_pricing(merchant_data) else "No"


def get_pci_compliance_info(merchant_data):
    """
    Extract PCI compliance-related information from CoPilot.
    
    Returns:
        dict: PCI-related fields from CoPilot
        
    Note: 
    - HubSpot field 'pci_non_compliance' doesn't exist yet - needs to be created
    - Logic for determining compliance status is unclear from CoPilot data
    - Available fields: pciProgramCd, pciAnnualFee, pciComplianceServiceFee
    """
    merchant = merchant_data.get("merchant", {})
    fees = merchant.get("fees", {})
    clover = merchant.get("cloverSecurityAndTransarmor", {})
    
    return {
        "pciProgramCd": fees.get("pciProgramCd"),  # e.g., "ANNUAL"
        "pciAnnualFee": fees.get("pciAnnualFee"),  # e.g., 0
        "pciComplianceServiceFee": clover.get("pciComplianceServiceFee"),  # Usually null
        "pciComplianceServiceFeeFrequency": clover.get("pciComplianceServiceFeeFrequency"),
        # TODO: Unclear what indicates non-compliance
        # Possible interpretations:
        # - pciAnnualFee > 0 means non-compliant?
        # - pciComplianceServiceFee exists means non-compliant?
        # Need clarification on business logic
    }


def _option_value_for_label(label: str, property_definition: Optional[dict]) -> Optional[str]:
    """Resolve a HubSpot option value by exact/fuzzy label matching."""
    if not label:
        return None
    rows = _hubspot_option_rows(property_definition)
    if not rows:
        return label
    l = label.strip().lower()
    for row in rows:
        if row["label_l"] == l or row["value_l"] == l:
            return row["value"] or row["label"]
    for row in rows:
        if l in row["label_l"] or row["label_l"] in l:
            return row["value"] or row["label"]
    return None


def merge_multiselect_values(existing_value: str, additions: list) -> str:
    """Merge semicolon-delimited HubSpot checkbox values without dropping existing selections."""
    out = []
    seen = set()
    if existing_value:
        for part in str(existing_value).split(";"):
            p = part.strip()
            if p and p not in seen:
                seen.add(p)
                out.append(p)
    for part in additions or []:
        p = str(part).strip()
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return ";".join(out)


def remove_multiselect_options(existing_value: str, remove_tokens: list) -> str:
    """Drop exact-matched (stripped) tokens from a semicolon-separated HubSpot multi-select value."""
    drop = {str(x).strip() for x in (remove_tokens or []) if str(x).strip()}
    if not drop:
        return str(existing_value or "").strip()
    out = []
    seen = set()
    for part in str(existing_value or "").split(";"):
        p = part.strip()
        if not p or p in drop or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return ";".join(out)


def build_pricing_type_multiselect_value(
    merchant_data_list: list,
    pricing_property_definition: Optional[dict] = None,
) -> Optional[str]:
    """
    Build Pricing Type multi-checkbox value across all merchants.
    CoPilot source: merchant.pricing.{flatPricing|icPlusPricing|swipeNonSwipePricing}
    """
    labels = []
    for md in merchant_data_list or []:
        p = get_pricing_type(md)
        if p:
            labels.append(p)
    if not labels:
        return None
    vals = []
    for lbl in labels:
        v = _option_value_for_label(lbl, pricing_property_definition)
        # Legacy aliases if older callers still pass pre-HubSpot labels
        if not v and lbl in ("IC Plus Pricing", "IC+ Pricing"):
            v = _option_value_for_label("IC Plus", pricing_property_definition)
        if not v and lbl in ("Swipe/Non-Swipe Pricing", "Swipe Non-Swipe Pricing"):
            v = _option_value_for_label("Swiped/Non-Swiped", pricing_property_definition)
        if not v and lbl == "Flat Pricing":
            v = _option_value_for_label("Flat Rate", pricing_property_definition)
        if not v:
            v = lbl
        if v not in vals:
            vals.append(v)
    return ";".join(vals) if vals else None


def build_industry_mcc_multiselect_value(
    merchant_data_list: list,
    industry_mcc_property_definition: Optional[dict] = None,
) -> Optional[str]:
    """Build ``industry_mcc`` (Industry (MCC)) multi-checkbox; one option per distinct merchant MCC."""
    labels = []
    for md in merchant_data_list or []:
        mcc = get_mcc_code(md)
        if not mcc:
            continue
        lbl = get_industry_from_mcc(mcc)
        if lbl and lbl not in labels:
            labels.append(lbl)
    if not labels:
        return None
    vals = []
    for lbl in labels:
        v = _option_value_for_label(lbl, industry_mcc_property_definition) or lbl
        if v not in vals:
            vals.append(v)
    return ";".join(vals) if vals else None


def build_pci_compliance_multiselect_value(
    merchant_data_list: list,
    pci_property_definition: Optional[dict] = None,
) -> Optional[str]:
    """
    Best-effort PCI checkbox mapping.
    Rule: if any merchant appears non-compliant, include 'PCI Non-Compliant', else 'PCI Compliant'.
    """
    any_non_compliant = False
    for md in merchant_data_list or []:
        info = get_pci_compliance_info(md)
        fee = info.get("pciAnnualFee")
        try:
            fee_num = float(fee) if fee is not None else 0.0
        except (TypeError, ValueError):
            fee_num = 0.0
        if fee_num > 0:
            any_non_compliant = True
            break
    label = "PCI Non-Compliant" if any_non_compliant else "PCI Compliant"
    v = _option_value_for_label(label, pci_property_definition) or label
    return v


def extract_mtd_ytd_totals(merchant_data_list: list) -> dict:
    """
    Sum MTD/YTD values if present in CoPilot volumeDetails.
    Returns {'mtd_volume': float|None, 'ytd_volume': float|None}
    """
    mtd_keys = ["mtdVolume", "monthToDateVolume", "mtdProcessingVolume"]
    ytd_keys = ["ytdVolume", "yearToDateVolume", "ytdProcessingVolume"]
    mtd_total = None
    ytd_total = None

    def _extract_num(dct, keys):
        for k in keys:
            if k in dct and dct.get(k) is not None:
                try:
                    return float(dct.get(k))
                except (TypeError, ValueError):
                    return None
        return None

    for md in merchant_data_list or []:
        vol = (
            md.get("merchant", {})
            .get("processing", {})
            .get("volumeDetails", {})
        )
        mv = _extract_num(vol, mtd_keys)
        yv = _extract_num(vol, ytd_keys)
        if mv is not None:
            mtd_total = (mtd_total or 0.0) + mv
        if yv is not None:
            ytd_total = (ytd_total or 0.0) + yv
    return {"mtd_volume": mtd_total, "ytd_volume": ytd_total}
