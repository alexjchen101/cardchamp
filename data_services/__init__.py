"""
Data Services — CardConnect SFTP drop processor.

Daily **three** CSVs under ``incoming/`` (merchant, funding, transaction) — same
columns as the reference Excel. Parsed with ``read_flat_csv_dict_rows``; stored
compactly in SQLite; HubSpot fields updated from rollups.

Pipeline:
  1. sftp_client  – download MERCHANT / FUNDING / TRANSACTION only
  2. parser       – one header + rows per file
  3. db           – ``data/data_services.db`` (daily_txn_totals, mid_last_funding, merchant_pci)
  4. aggregator   – MTD / YTD / last deposit / PCI
  5. hubspot_sync – push to contacts
"""
