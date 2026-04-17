#!/usr/bin/env python3
"""
Report duplicate HubSpot deals grouped by ``copilot_account_number``.

This does NOT delete anything. It prints a keep-candidate recommendation so you
can manually review before archiving duplicates.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from hubspot.client import HubSpotClient


def _rank(deal: dict) -> tuple:
    p = deal.get("properties") or {}
    # Prefer progressed stage, then most recently modified.
    stage = (p.get("dealstage") or "")
    modified = (p.get("hs_lastmodifieddate") or "")
    created = (p.get("createdate") or "")
    return (stage, modified, created)


def main() -> int:
    hs = HubSpotClient()
    prop = "copilot_account_number"
    if prop not in hs.get_deal_property_names():
        print(f"Missing deal property {prop!r}. Run tools/ensure_deal_copilot_property.py first.")
        return 1

    # Pull deals by paging through the deals list endpoint (v3).
    # HubSpot caps per page; we keep it simple and stop when paging.next is absent.
    after = None
    all_deals: list[dict] = []
    while True:
        endpoint = "/crm/v3/objects/deals?limit=100"
        if after:
            endpoint += f"&after={after}"
        resp = hs._request(
            "GET",
            endpoint + f"&properties=dealname,dealstage,createdate,hs_lastmodifieddate,{prop}",
        )
        rows = resp.get("results") or []
        all_deals.extend(rows)
        after = ((resp.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            break

    by_val: dict[str, list[dict]] = defaultdict(list)
    for d in all_deals:
        p = d.get("properties") or {}
        v = (p.get(prop) or "").strip()
        if not v:
            continue
        by_val[v].append(d)

    dups = {k: v for k, v in by_val.items() if len(v) > 1}
    if not dups:
        print("No duplicates found (by copilot_account_number).")
        return 0

    print(f"Duplicate groups: {len(dups)}")
    for copilot_id, deals in sorted(dups.items(), key=lambda kv: kv[0]):
        deals_sorted = sorted(deals, key=_rank, reverse=True)
        keep = deals_sorted[0]
        print("\n" + "=" * 60)
        print(f"copilot_account_number={copilot_id}  deals={len(deals)}")
        for idx, d in enumerate(deals_sorted, 1):
            p = d.get("properties") or {}
            mark = "KEEP" if d.get("id") == keep.get("id") else "DUP "
            print(
                f"{mark} {idx}. id={d.get('id')} stage={p.get('dealstage')} "
                f"created={p.get('createdate')} modified={p.get('hs_lastmodifieddate')} "
                f"name={p.get('dealname')!r}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

