# Deployment

Use a small VM first. This is the simplest reliable way to run the daily sync.

## Recommended setup

- 1 small Linux VM
- Python 3.9+
- this repo checked out
- `.env` with HubSpot and CoPilot credentials
- cron for scheduling

Good cheap options:
- AWS Lightsail
- small EC2 instance
- any small VPS

## Server setup

```bash
git clone <your-repo-url>
cd CardChamp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `.env` with the existing credentials used locally.

Optional retry/timeouts can be tuned with:

```bash
HUBSPOT_TIMEOUT_SECONDS=30
HUBSPOT_MAX_RETRIES=4
HUBSPOT_RETRY_BACKOFF_SECONDS=2
COPILOT_TIMEOUT_SECONDS=30
COPILOT_MAX_RETRIES=4
COPILOT_RETRY_BACKOFF_SECONDS=2
```

## Operator entrypoints

One-off production sync:

```bash
python3 jobs/sync_with_status.py someone@example.com
```

**Recommended:** CoPilot first, then SFTP (one command):

```bash
python3 jobs/run_go_live_pipeline.py
```

Daily batch (default: **every contact** with `copilot_account`):

```bash
python3 jobs/batch_sync.py
```

Manual/testing setup sync:

```bash
python3 tools/sync_initial_setup.py someone@example.com
```

### Refresh SFTP-backed HubSpot fields without a new drop

The daily job can show **Files processed: 0** when nothing new arrived on SFTP; the HubSpot step still runs and **re-reads the local SQLite rollups** (last ingested MERCHANT / FUNDING / TRANSACTION files), so **mtd_volume**, **ytd_volume**, **last_deposit_date**, and **pci_compliance** stay aligned with whatever is already in the DB.

To **only** push those metrics from SQLite to HubSpot (no SFTP download, no new ingest):

```bash
python3 jobs/sync_data_services.py --hubspot-only
```

Default HubSpot scope for data services is the same: **all contacts with `copilot_account`**.

To re-pull files that were already imported (e.g. fix a bad ingest), use **`--force-download`** on a normal `sync_data_services.py` run, or **`--file`** to re-process a specific CSV.

## CSV update workflow

The live owner mapping reads:

1. `data/owner_mapping.csv` (primary)
2. `data/legacy/sales_code_owner_map.json` (fallback if the CSV is missing or empty)

Rows with a sales code but no owner id/email/name are treated as inactive/cancelled and do not set `hubspot_owner_id`.

Normal update flow:

1. Replace `data/owner_mapping.csv`
2. Optional review:

```bash
python3 tools/check_sales_code_owner_map.py
```

3. Run the batch sync

Each new batch process reads the current CSV fresh.

## Cron example

Run every day at 2:15 AM server time:

```cron
15 2 * * * cd /path/to/CardChamp && /path/to/CardChamp/.venv/bin/python jobs/run_go_live_pipeline.py >> /path/to/CardChamp/sync/go_live_pipeline.log 2>&1
```

The batch runner also writes a JSON summary by default:

- `sync/last_batch_run.json`

## Scope

- Put **CoPilot Account #** on each HubSpot contact that should sync. Each run processes **all** such contacts.

## At larger scale

- confirm HubSpot and CoPilot API rate limits are acceptable for your tenant
