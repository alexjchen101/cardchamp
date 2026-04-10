#!/usr/bin/env python3
"""
Export CoPilot equipment catalog + HubSpot ``point_of_sale`` schema for unified mapping work.

Embedded HubSpot labels (``get_point_of_sale_embedded_labels()`` / gwc-exact prefix of ``_hubspot_pos_rules``); CoPilot
can expose thousands of catalog rows, and HubSpot admins add checkboxes over time. Run this
with valid ``.env`` (CoPilot + HubSpot) to refresh into ``data/pos_analysis/`` (gitignored — safe to delete and re-run):

  - ``copilot_equipment_catalog.json`` — all equipmentId / name / type
  - ``hubspot_point_of_sale_options.json`` — live labels + internal values

Printed summary highlights drift between embedded labels, HubSpot live options, and CoPilot
names (for building a single CoPilot→HubSpot mapping table).

Usage:
  python3 tools/export_pos_mapping_sources.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

OUT_DIR = ROOT / "data" / "pos_analysis"


def main():
    from copilot import MerchantAPI
    from hubspot.client import HubSpotClient
    from field_mappings import get_point_of_sale_embedded_labels, logical_pos_label_for_equipment

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    copilot = MerchantAPI()
    hub = HubSpotClient()

    catalog = copilot.get_equipment_catalog_map()
    catalog_rows = [
        {"equipmentId": int(eid), "name": meta["name"], "type": meta["type"]}
        for eid, meta in sorted(catalog.items(), key=lambda x: int(x[0]))
    ]

    copilot_path = OUT_DIR / "copilot_equipment_catalog.json"
    with open(copilot_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "count": len(catalog_rows),
                "items": catalog_rows,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    pos_def = hub.get_contact_property_definition("point_of_sale")
    raw_opts = pos_def.get("options") or []
    hub_rows = []
    for o in raw_opts:
        if not isinstance(o, dict):
            continue
        lab = (o.get("label") or "").strip()
        val = (o.get("value") or lab or "").strip()
        if not lab and not val:
            continue
        hub_rows.append({"label": lab, "value": val})

    hub_path = OUT_DIR / "hubspot_point_of_sale_options.json"
    with open(hub_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "count": len(hub_rows),
                "label": pos_def.get("label"),
                "name": pos_def.get("name"),
                "fieldType": pos_def.get("fieldType"),
                "options": hub_rows,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    embedded = set(get_point_of_sale_embedded_labels())
    hub_labels = {r["label"] for r in hub_rows if r["label"]}
    in_hub_not_embedded = sorted(hub_labels - embedded)
    in_embedded_not_hub = sorted(embedded - hub_labels)

    mapped_rows = []
    for row in catalog_rows:
        name = row["name"]
        typ = row["type"]
        lbl = logical_pos_label_for_equipment(name, typ)
        mapped_rows.append(
            {
                "equipmentId": row["equipmentId"],
                "name": name,
                "type": typ,
                "logical_pos_label": lbl,
            }
        )

    mapped_path = OUT_DIR / "copilot_equipment_with_logical_pos.json"
    with open(mapped_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "note": "Every catalog row; logical_pos_label uses current field_mappings rules.",
                "count": len(mapped_rows),
                "items": mapped_rows,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print("=== point_of_sale / equipment export ===")
    print(f"CoPilot equipment rows: {len(catalog_rows)}  →  {copilot_path}")
    print(f"HubSpot point_of_sale options: {len(hub_rows)}  →  {hub_path}")
    print(f"Embedded HubSpot POS labels (get_point_of_sale_embedded_labels): {len(embedded)}")
    print()
    print("Drift: HubSpot live labels NOT in embedded tuple:")
    print(f"  ({len(in_hub_not_embedded)} labels)")
    for x in in_hub_not_embedded[:40]:
        print(f"    + {x}")
    if len(in_hub_not_embedded) > 40:
        print(f"    ... and {len(in_hub_not_embedded) - 40} more")
    print()
    print("Drift: Embedded tuple labels NOT in HubSpot response (renamed/removed in portal):")
    print(f"  ({len(in_embedded_not_hub)} labels)")
    for x in in_embedded_not_hub[:40]:
        print(f"    - {x}")
    if len(in_embedded_not_hub) > 40:
        print(f"    ... and {len(in_embedded_not_hub) - 40} more")
    print()
    print(f"CoPilot → logical POS label (full catalog, rules as-of today): {mapped_path}")


if __name__ == "__main__":
    main()
