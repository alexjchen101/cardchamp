#!/usr/bin/env python3
"""
Daily pipeline — **order matters**:

1. **CoPilot → HubSpot** (`jobs/batch_sync.py`): every contact with **CoPilot Account #** set.
2. **CardConnect SFTP → SQLite → HubSpot** (`jobs/sync_data_services.py`): same contact set for PCI / volumes / deposit fields.

Control who gets synced by setting **CoPilot Account #** on contacts in HubSpot; no separate ID list file.

Run this for production cron instead of calling the two jobs separately.

Examples::

    python3 jobs/run_go_live_pipeline.py
    python3 jobs/run_go_live_pipeline.py --continue-on-batch-error
    python3 jobs/run_go_live_pipeline.py --skip-copilot-batch
    python3 jobs/run_go_live_pipeline.py --dry-run
    python3 jobs/run_go_live_pipeline.py --hubspot-only
    python3 jobs/run_go_live_pipeline.py --skip-copilot-batch --hubspot-only

Any other arguments are forwarded to ``sync_data_services.py`` (e.g. ``--dry-run``, ``--hubspot-only``).
Pipeline-only flags: ``--skip-copilot-batch``, ``--continue-on-batch-error``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _run(py: str, script: Path, args: list[str]) -> int:
    cmd = [py, str(script)] + args
    print("→", " ".join(cmd))
    return subprocess.run(cmd, cwd=str(ROOT)).returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description="CoPilot batch sync, then SFTP data services sync (correct order)",
    )
    parser.add_argument(
        "--skip-copilot-batch",
        action="store_true",
        help="Only run sync_data_services (SFTP + HubSpot metrics)",
    )
    parser.add_argument(
        "--continue-on-batch-error",
        action="store_true",
        help="Run SFTP sync even if batch_sync exits non-zero",
    )
    args, ds_args = parser.parse_known_args()
    ds_args = [a for a in ds_args if a != "--"]

    py = sys.executable
    batch_script = ROOT / "jobs" / "batch_sync.py"
    ds_script = ROOT / "jobs" / "sync_data_services.py"

    if not args.skip_copilot_batch:
        print("=" * 60, flush=True)
        print("STEP 1: CoPilot API → HubSpot (batch_sync)", flush=True)
        print("=" * 60, flush=True)
        rc = _run(py, batch_script, [])
        if rc != 0 and not args.continue_on_batch_error:
            print(f"\n✗ batch_sync exited {rc}; skipping SFTP step (use --continue-on-batch-error to override)")
            return rc
        if rc != 0:
            print(f"\n⚠️  batch_sync exited {rc}; continuing with data services (--continue-on-batch-error)")

    print(flush=True)
    print("=" * 60, flush=True)
    print("STEP 2: SFTP drop → SQLite → HubSpot (sync_data_services)", flush=True)
    print("=" * 60, flush=True)
    return _run(py, ds_script, ds_args)


if __name__ == "__main__":
    raise SystemExit(main())
