#!/usr/bin/env python3
"""
Ensure HubSpot Deal property exists: ``copilot_account_number``.

This property is used to de-duplicate deals across multiple contacts that share
the same CoPilot Account #.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from hubspot.client import HubSpotClient


def main() -> int:
    hs = HubSpotClient()
    created = hs.create_deal_property_if_missing(
        name="copilot_account_number",
        label="CoPilot Account Number",
        description="CoPilot account id used by CardChamp sync to de-duplicate deals across contacts.",
    )
    print("Created" if created else "Already exists")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

