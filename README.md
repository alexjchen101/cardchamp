# CardChamp CoPilot -> HubSpot Integration

Sync merchant data from CoPilot to HubSpot CRM.

## Run

Production jobs:

```bash
python3 jobs/sync_with_status.py test@test.com
```

Batch / daily sync:

```bash
python3 jobs/batch_sync.py --mode allowlist
python3 jobs/batch_sync.py --mode all
```

Manual tools / testing:

```bash
python3 tools/sync_initial_setup.py test@test.com
```

Scripts pull CoPilot IDs from HubSpot `copilot_account`.

## Layout

```text
jobs/sync_with_status.py       production sync for one contact
jobs/batch_sync.py             batch runner for daily sync
tools/sync_initial_setup.py    status-blind setup sync
field_mappings.py              mapping + transforms
status_logic.py                deal/status rules
sales_code_owners.py           sales code -> HubSpot owner resolver
data/owner_mapping.csv            sales code → HubSpot owner (primary)
data/legacy/sales_code_owner_map.json legacy fallback mapping
hubspot/client.py              HubSpot API client
copilot/                       CoPilot API client
tools/list_hubspot_owners.py   helper to list HubSpot owner ids
tools/check_sales_code_owner_map.py validate sales code owner CSV
tools/refresh_mcc_mapping.py   refresh MCC options from HubSpot
config/live_allowlist.txt      starter allowlist for daily live sync
docs/FIELD_MAPPING.md          source-of-truth field behavior
docs/reference/                archived client/reference files
```

## Docs

- `docs/FIELD_MAPPING.md` is the source of truth for synced fields and behavior.
- `docs/reference/CoPilot - HubSpot Data Flow - Field Mapping.csv` is the original client requirement sheet.
- `docs/DEPLOYMENT.md` explains a cheap VM + cron deployment path for daily live sync.
