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

CardConnect Data Services (SFTP → SQLite → HubSpot: PCI, MTD/YTD volume, last deposit date):

```bash
python3 jobs/sync_data_services.py
python3 jobs/sync_data_services.py --allowlist
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
jobs/sync_data_services.py     SFTP daily files → HubSpot (PCI, volumes, deposit date)
data_services/                 SFTP client, parser, SQLite rollups, HubSpot push
tools/sync_initial_setup.py    status-blind setup sync
field_mappings.py              mapping + transforms
status_logic.py                deal/status rules
sales_code_owners.py           sales code -> HubSpot owner resolver
data/owner_mapping.csv            sales code → HubSpot owner (primary)
data/legacy/sales_code_owner_map.json  owner map if CSV missing/empty
hubspot/client.py              HubSpot API client
copilot/                       CoPilot API client
tools/list_hubspot_owners.py   helper to list HubSpot owner ids
tools/check_sales_code_owner_map.py validate sales code owner CSV
tools/data_services_import_status.py  which DS CSVs are ingested in SQLite (optional --compare-remote)
tools/refresh_mcc_mapping.py   refresh MCC options from HubSpot
config/live_allowlist.txt      starter allowlist for daily live sync
docs/FIELD_MAPPING.md          source-of-truth field behavior
docs/POINT_OF_SALE_MAPPING.md  CoPilot → HubSpot POS equipment mapping (for maintainers)
docs/reference/                archived client/reference files
```

## Docs

- `docs/FIELD_MAPPING.md` — synced fields and behavior.
- `docs/POINT_OF_SALE_MAPPING.md` — how to maintain `point_of_sale` equipment mapping in `field_mappings.py`.
- `docs/reference/CoPilot - HubSpot Data Flow - Field Mapping.csv` — original client requirement sheet.
- `docs/DEPLOYMENT.md` — cheap VM + cron deployment for daily live sync.
