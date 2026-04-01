#!/usr/bin/env python3
"""
List HubSpot CRM owner ids for validating / maintaining sales-code owner CSV mappings.

Usage (from repo root):
    python3 tools/list_hubspot_owners.py

Requires: HUBSPOT_ACCESS_TOKEN in .env
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from hubspot.client import HubSpotClient  # noqa: E402


def main() -> None:
    client = HubSpotClient()
    owners = client.list_owners(limit=500)
    for o in owners:
        oid = o.get("id")
        email = o.get("email") or ""
        first = o.get("firstName") or ""
        last = o.get("lastName") or ""
        name = f"{first} {last}".strip()
        line = {"id": oid, "email": email, "name": name}
        print(json.dumps(line, ensure_ascii=False))


if __name__ == "__main__":
    main()
