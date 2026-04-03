"""
Razorpay processor — CSV reports sent by ashutosh.kashyap@bachatt.app.

Flow
----
attachment bytes  (or Drive-downloaded bytes)
    │
    ▼
clean CSV bytes (strip NULLs, control chars)
    │
    ▼
GcsUploadService.upload_csv()
    gs://razorpay_reports_bucket/{stem}/{YYYYMMDD}_{msg_id}_{filename}.csv
"""

import logging
from datetime import date

from src.logging_context import get_request_id
from src.processors.base import FileProcessorStrategy

logger = logging.getLogger(__name__)
_TAG = "Razorpay"


class RazorpayProcessor(FileProcessorStrategy):
    """
    Async strategy for Razorpay report attachments / Drive files.
    Accepts any file — forces .csv extension on upload.
    """

    # Accept any file type (Razorpay sends CSV reports without a fixed extension list)
    SUPPORTED_EXTENSIONS = (".csv", ".xlsx", ".xls", ".txt", ".dbf")

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

        if attachment_filename.lower().endswith(".zip"):
            file_map = await self._extract_zip_files(attachment_data, attachment_filename, zip_password=zip_password)
            if file_map is None:
                return []
        elif self._is_direct_target(attachment_filename):
            file_map = {attachment_filename: attachment_data}
        else:
            # Accept unknown extensions — treat raw bytes as CSV passthrough
            file_map = {attachment_filename: attachment_data}

        results: list[str] = []
        for filename, raw_bytes in file_map.items():
            uri = await self._process_file(filename, raw_bytes, msg_id, up_date)
            if uri:
                results.append(uri)
        return results

    async def _process_file(
        self, filename: str, raw_bytes: bytes, msg_id: str, up_date: date,
    ) -> str | None:
        rid = get_request_id()
        logger.info("[req_id=%s] [%s] Processing file: '%s'", rid, _TAG, filename)

        csv_bytes = self.clean_csv_bytes(raw_bytes)
        output_csv = csv_bytes.decode("utf-8", errors="replace")
        if not output_csv.strip():
            logger.warning("[req_id=%s] [%s] File is empty: '%s'", rid, _TAG, filename)
            return None

        if self._gcs_service is None:
            logger.info("[req_id=%s] [%s] No GCS service — dry run for '%s'", rid, _TAG, filename)
            return filename

        # Path mirrors Go: {stem}/{YYYYMMDD}_{msg_id}_{filename}.csv
        stem = filename.rsplit(".", 1)[0]
        safe_stem = "".join(c if c.isalnum() or c in "-_." else "_" for c in stem)
        date_prefix = up_date.strftime("%Y%m%d")
        escaped = filename.replace(" ", "_")
        blob_path = f"{safe_stem}/{date_prefix}_{msg_id}_{escaped}"
        if not blob_path.endswith(".csv"):
            blob_path = blob_path.rsplit(".", 1)[0] + ".csv"

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
