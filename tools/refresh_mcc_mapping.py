#!/usr/bin/env python3
"""
Refresh MCC mapping from HubSpot Industry (MCC) options.
Run when HubSpot adds/updates ``industry_mcc`` dropdown options.

Usage: python3 tools/refresh_mcc_mapping.py
Requires: HUBSPOT_ACCESS_TOKEN in .env
"""

import os
import re
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

def main():
    token = os.getenv("HUBSPOT_ACCESS_TOKEN")
    if not token:
        print("Error: HUBSPOT_ACCESS_TOKEN not found in .env")
        sys.exit(1)

    url = "https://api.hubapi.com/crm/v3/properties/contacts/industry_mcc"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    opts = r.json().get("options", [])

    mcc_to_value = {}
    for o in opts:
        value = o.get("value", "")
        label = o.get("label", "")
        match = re.match(r"^(\d{3,5})\s*[-–]?\s*", label)
        if match:
            mcc = match.group(1).lstrip("0") or "0"
            if mcc not in mcc_to_value:
                mcc_to_value[mcc] = value

    out_path = Path(__file__).parent.parent / "mcc_mapping.py"
    with open(out_path, "w") as f:
        f.write('"""\nMCC Code to HubSpot Industry Mapping\n')
        f.write("Auto-generated from HubSpot industry_mcc property options.\n")
        f.write('Maps CoPilot mccId to HubSpot industry_mcc option value.\n"""\n\n')
        f.write("MCC_TO_INDUSTRY = {\n")
        for mcc in sorted(mcc_to_value.keys(), key=lambda x: int(x) if x.isdigit() else 0):
            val = mcc_to_value[mcc].replace("\\", "\\\\").replace('"', '\\"')
            f.write(f'    "{mcc}": "{val}",\n')
        f.write("}\n\n")
        f.write('''def get_industry_from_mcc(mcc_code):
    """
    Get HubSpot industry value from CoPilot MCC code.

    Args:
        mcc_code: MCC code as string or int from CoPilot

    Returns:
        str: HubSpot industry value, or "Other" if not found
    """
    mcc_str = str(int(mcc_code)) if mcc_code is not None else ""
    if mcc_str in MCC_TO_INDUSTRY:
        return MCC_TO_INDUSTRY[mcc_str]
    return "Other"
''')
    print(f"Wrote {len(mcc_to_value)} MCC mappings to {out_path}")

if __name__ == "__main__":
    main()
