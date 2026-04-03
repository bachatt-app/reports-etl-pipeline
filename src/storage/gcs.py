"""
Async GCS upload service.

Mirrors the reference Go architecture:
  - Partitioned paths: {prefix}/year={YYYY}/month={MM}/day={DD}/{msg_id}_{filename}.csv
  - CSV validated before upload (non-empty, has header)
  - Failures logged with full context; exception re-raised so caller can alert

UPLOAD_TO_GCS (env var)
-----------------------
  true  (default) — upload to GCS bucket as normal.
  false           — save CSV to local ./output/ directory instead; no GCS call.
                    Useful for local dev / testing without a real bucket.

Usage
-----
    svc = GcsUploadService(bucket="my-bucket", prefix="r2_cams")
    gcs_uri = await svc.upload_csv(csv_text, msg_id="abc123",
                                   filename="report.csv", upload_date=date.today())
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
from datetime import date
from pathlib import Path

from src.logging_context import get_request_id

logger = logging.getLogger(__name__)

# Resolved once at import time so every call sees the same value.
_UPLOAD_TO_GCS: bool = os.getenv("UPLOAD_TO_GCS", "true").lower() == "true"
_LOCAL_OUTPUT_DIR: str = os.getenv("LOCAL_OUTPUT_DIR", "output")


class GcsUploadService:
    """Thin async wrapper around google-cloud-storage for CSV uploads."""

    def __init__(self, bucket: str, prefix: str) -> None:
        self._bucket = bucket
        self._prefix = prefix
        self._client = None  # lazy — created on first use inside executor

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def upload_csv(
        self,
        csv_text: str,
        msg_id: str,
        filename: str,
        upload_date: date,
        prefix_override: str = "",
        blob_path: str = "",
    ) -> str:
        """
        Upload *csv_text* to GCS (or save locally when UPLOAD_TO_GCS=false).
        Returns the full gs:// URI or local file path.

        prefix_override — if given, replaces self._prefix for this upload.
        blob_path       — if given, used as the full GCS object path directly,
                          bypassing _make_path entirely (for custom path formats).
        Raises on upload failure (caller should catch and send alert).
        """
        _validate_csv(csv_text, filename)

        if not blob_path:
            blob_path = self._make_path(msg_id, filename, upload_date, prefix_override or self._prefix)
        content = csv_text.encode("utf-8")

        if not _UPLOAD_TO_GCS:
            return await self._save_local(blob_path, content)

        if not self._bucket:
            raise ValueError("GCS bucket name is not configured (GCS_CAMS_BUCKET / GCS_KFINTECH_BUCKET)")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            functools.partial(self._upload_sync, blob_path, content),
        )

        uri = f"gs://{self._bucket}/{blob_path}"
        logger.info("[req_id=%s] Uploaded %s (%d bytes)", get_request_id(), uri, len(content))
        return uri

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _make_path(self, msg_id: str, filename: str, d: date, prefix: str) -> str:
        """
        Build the partitioned object path.

        Example:
            R2/year=2026/month=03/day=31/abc123_report.csv
        """
        stem = filename.rsplit(".", 1)[0]
        safe_stem = "".join(c if c.isalnum() or c in "-_." else "_" for c in stem)
        name = f"{msg_id}_{safe_stem}.csv"
        return (
            f"{prefix}"
            f"/year={d.year:04d}"
            f"/month={d.month:02d}"
            f"/day={d.day:02d}"
            f"/{name}"
        )

    async def _save_local(self, blob_path: str, content: bytes) -> str:
        """Write CSV to ./output/<blob_path> and return the local path."""
        local_path = Path(_LOCAL_OUTPUT_DIR) / blob_path
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, functools.partial(_write_local, local_path, content))
        uri = str(local_path)
        logger.info("[req_id=%s] Saved locally %s (%d bytes)", get_request_id(), uri, len(content))
        return uri

    def _upload_sync(self, blob_path: str, content: bytes) -> None:
        """Blocking GCS upload — called inside run_in_executor."""
        from google.cloud import storage  # lazy import

        if self._client is None:
            self._client = storage.Client()

        bucket = self._client.bucket(self._bucket)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content, content_type="text/csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_local(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _validate_csv(csv_text: str, filename: str) -> None:
    """Raise ValueError if the CSV is clearly malformed."""
    if not csv_text or not csv_text.strip():
        raise ValueError(f"CSV for '{filename}' is empty — skipping upload.")
    first_line = csv_text.splitlines()[0].strip()
    if not first_line:
        raise ValueError(f"CSV for '{filename}' has an empty header row.")
