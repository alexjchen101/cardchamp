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

Allowlist daily batch:

```bash
python3 jobs/batch_sync.py --mode allowlist
```

All contacts with `copilot_account`:

```bash
python3 jobs/batch_sync.py --mode all
```

Manual/testing setup sync:

```bash
python3 tools/sync_initial_setup.py someone@example.com
```

## CSV update workflow

The live owner mapping reads these in order:

1. `data/CoPilot - HubSpot Data Flow - Sales Codes.csv`
2. `data/sales_code_owner_map.csv`
3. `data/legacy/sales_code_owner_map.json`

Normal update flow:

1. Replace `data/CoPilot - HubSpot Data Flow - Sales Codes.csv`
2. Optional review:

```bash
python3 tools/check_sales_code_owner_map.py
```

3. Run the batch sync

Each new batch process reads the current CSV fresh.

## Cron example

Run every day at 2:15 AM server time:

```cron
15 2 * * * cd /path/to/CardChamp && /path/to/CardChamp/.venv/bin/python jobs/batch_sync.py --mode allowlist >> /path/to/CardChamp/sync/batch_sync.log 2>&1
```

Later, when you are ready:

```cron
15 2 * * * cd /path/to/CardChamp && /path/to/CardChamp/.venv/bin/python jobs/batch_sync.py --mode all >> /path/to/CardChamp/sync/batch_sync.log 2>&1
```

The batch runner also writes a JSON summary by default:

- `sync/last_batch_run.json`

## Recommended rollout

Phase 1:
- keep `config/live_allowlist.txt`
- run `jobs/batch_sync.py --mode allowlist`

Phase 2:
- switch cron to `jobs/batch_sync.py --mode all`

## Before switching to all contacts

- confirm the allowlist run is stable for several days
- confirm HubSpot rate limits are acceptable
