"""
BSE processor — CSV report attachment from kartikeya.mishra@bachatt.app.

Flow
----
.csv attachment bytes
    │
    ▼
clean CSV bytes (strip NULLs, control chars)
    │
    ▼
GcsUploadService.upload_csv()
    gs://bse_report_bucket/bse_report/year={YYYY}/month={MM}/day={DD}/{YYYYMMDD}_{msg_id}.csv
"""

import logging
from datetime import date

from src.logging_context import get_request_id
from src.processors.base import FileProcessorStrategy

logger = logging.getLogger(__name__)
_TAG = "BSE"


class BseProcessor(FileProcessorStrategy):
    """Async strategy for BSE CSV report attachments."""

    SUPPORTED_EXTENSIONS = (".csv",)

    async def extract(
        self,
        attachment_data: bytes,
        attachment_filename: str,
        msg_id: str = "",
        upload_date: date | None = None,
        report_type: str = "",
        gcs_prefix: str = "",
        zip_password: str | None = None,
    ) -> list[str]:
        rid = get_request_id()
        up_date = upload_date or date.today()
        logger.info(
            "[req_id=%s] [%s] Processing attachment: '%s'",
            rid, _TAG, attachment_filename,
        )

        if not self._is_direct_target(attachment_filename):
            logger.warning(
                "[req_id=%s] [%s] Unsupported attachment type: '%s'", rid, _TAG, attachment_filename
            )
            return []

        uri = await self._process_file(attachment_filename, attachment_data, msg_id, up_date)
        return [uri] if uri else []

    async def _process_file(
        self, filename: str, raw_bytes: bytes, msg_id: str, up_date: date,
    ) -> str | None:
        rid = get_request_id()
        logger.info("[req_id=%s] [%s] Processing file: '%s'", rid, _TAG, filename)

        csv_bytes = self.clean_csv_bytes(raw_bytes)
        output_csv = csv_bytes.decode("utf-8", errors="replace")
        if not output_csv.strip():
            logger.warning("[req_id=%s] [%s] CSV is empty for '%s'", rid, _TAG, filename)
            return None

        if self._gcs_service is None:
            logger.info("[req_id=%s] [%s] No GCS service — dry run for '%s'", rid, _TAG, filename)
            return filename

        # Path mirrors Go: bse_report/year={Y}/month={M}/day={D}/{YYYYMMDD}_{msg_id}.csv
        date_prefix = up_date.strftime("%Y%m%d")
        blob_path = (
            f"bse_report"
            f"/year={up_date.year:04d}"
            f"/month={up_date.month:02d}"
            f"/day={up_date.day:02d}"
            f"/{date_prefix}_{msg_id}.csv"
        )

        try:
            uri = await self._gcs_service.upload_csv(
                output_csv, msg_id=msg_id, filename=filename,
                upload_date=up_date, blob_path=blob_path,
            )
        except Exception as exc:
            logger.error("[req_id=%s] [%s] GCS upload failed for '%s': %s", rid, _TAG, filename, exc)
            await self._fire_alert(
                f"[ETL] {_TAG} — GCS upload failed for '{filename}'",
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None

        return uri
