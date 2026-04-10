"""
SFTP Client — CardConnect Data Services

Downloads daily data drop files from ftp.cardconnect.com into
data/sftp_downloads/.  Tracks which files have already been processed
in data/sftp_processed.json so we never import the same file twice.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import paramiko
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = ROOT / "data" / "sftp_downloads"
PROCESSED_LOG = ROOT / "data" / "sftp_processed.json"

# CardConnect drops daily CSVs under ``incoming/``. We only **download** the three
# files needed for HubSpot (PCI, deposits, volume). Adjustment/chargeback are skipped.
SFTP_REMOTE_DIR = os.getenv("DS_SFTP_REMOTE_DIR", "incoming")


def is_required_hubspot_data_file(filename: str) -> bool:
    """True for MERCHANT / FUNDING / TRANSACTION drops only."""
    u = filename.upper()
    if not u.startswith("DS_CARDCHAMP_"):
        return False
    return "_MERCHANT_" in u or "_FUNDING_" in u or "_TRANSACTION_" in u


class DataServicesSFTPClient:
    """
    Downloads CardConnect Data Services daily drop files via SFTP.

    Credentials are loaded from the environment:
        DS_SFTP_HOST     – hostname  (default: ftp.cardconnect.com)
        DS_SFTP_PORT     – SSH port  (default: 22)
        DS_SFTP_USERNAME – username
        DS_SFTP_PASSWORD – password
    """

    def __init__(self):
        self.host = os.getenv("DS_SFTP_HOST", "ftp.cardconnect.com")
        self.port = int(os.getenv("DS_SFTP_PORT", "22"))
        self.username = os.getenv("DS_SFTP_USERNAME", "cardchamp")
        self.password = os.getenv("DS_SFTP_PASSWORD")
        if not self.password:
            raise ValueError("DS_SFTP_PASSWORD not set in environment")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self):
        """Return an open (ssh, sftp) pair."""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            timeout=30,
            banner_timeout=30,
        )
        return ssh, ssh.open_sftp()

    def _load_processed(self) -> set:
        if PROCESSED_LOG.is_file():
            try:
                data = json.loads(PROCESSED_LOG.read_text(encoding="utf-8"))
                return set(data.get("processed", []))
            except Exception:
                pass
        return set()

    def _save_processed(self, processed: set) -> None:
        PROCESSED_LOG.parent.mkdir(parents=True, exist_ok=True)
        PROCESSED_LOG.write_text(
            json.dumps(
                {
                    "processed": sorted(processed),
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_remote_files(self) -> list[str]:
        """Return filenames available in the remote drop directory."""
        ssh, sftp = self._connect()
        try:
            return sftp.listdir(SFTP_REMOTE_DIR)
        finally:
            sftp.close()
            ssh.close()

    def download_new_files(self, force: bool = False) -> list[Path]:
        """
        Download any files in the remote drop directory that have not been
        processed yet.

        Args:
            force: Re-download even if already processed (for debugging).

        Returns:
            List of local Path objects for the downloaded files.
        """
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        processed = self._load_processed()
        downloaded: list[Path] = []

        ssh, sftp = self._connect()
        try:
            remote_files = sftp.listdir(SFTP_REMOTE_DIR)
            print(f"   Remote files in {SFTP_REMOTE_DIR}/: {remote_files or '(none)'}")

            for filename in remote_files:
                if not is_required_hubspot_data_file(filename):
                    print(f"   Skipping (not merchant/funding/transaction): {filename}")
                    continue
                if not force and filename in processed:
                    print(f"   ↩  Already processed: {filename}")
                    continue

                remote_path = f"{SFTP_REMOTE_DIR}/{filename}"
                local_path = DOWNLOAD_DIR / filename
                print(f"   ↓  Downloading: {filename} → {local_path}")
                sftp.get(remote_path, str(local_path))
                downloaded.append(local_path)
                processed.add(filename)

        finally:
            sftp.close()
            ssh.close()

        self._save_processed(processed)
        return downloaded

    def peek_remote_file(self, filename: str, max_bytes: int = 4096) -> str:
        """
        Read the first ``max_bytes`` of a remote file without downloading it.
        Useful for inspecting format before committing to a full download.
        """
        ssh, sftp = self._connect()
        try:
            remote_path = f"{SFTP_REMOTE_DIR}/{filename}"
            with sftp.open(remote_path, "r") as f:
                return f.read(max_bytes).decode("utf-8", errors="replace")
        finally:
            sftp.close()
            ssh.close()
