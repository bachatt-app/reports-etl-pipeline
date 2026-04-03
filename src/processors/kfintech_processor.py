"""
KFintech processor — full ETL pipeline per attachment.

Flow
----
attachment bytes
    │
    ├─ .zip  → decrypt (password) → extract csv/dbf files
    └─ .csv / .dbf  → use directly
    │
    ▼
convert DBF → CSV (if needed)
    │
    ▼
clean CSV bytes (strip NULLs, control chars)
    │
    ▼
passthrough — original CSV headers preserved as-is
(BigQuery external table auto-converts spaces → underscores when reading)
    │
    ▼
GcsUploadService.upload_csv()  →  gs://bucket/r2_kfintech/year=.../...csv
    │
    ▼
BigQueryMergeService.run_merge(KFINTECH_MERGE_SQL)
"""

import logging
from datetime import date

from src.logging_context import get_request_id
from src.processors.base import FileProcessorStrategy
from src.processors.schema.kfintech_schema import KFINTECH_MERGE_SQL

logger = logging.getLogger(__name__)
_TAG = "KFintech"


class KFintechProcessor(FileProcessorStrategy):
    """
    Async strategy for KFintech (Karvy Fintech) attachments.

    Expects:
      - Direct .dbf / .csv attachment, or
      - .zip archive (optionally password-protected) containing csv/dbf files.
    """

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
            "[req_id=%s] [%s] Processing attachment: '%s' (report_type=%r)",
            rid, _TAG, attachment_filename, report_type,
        )

        # ── 1. Gather (filename → raw bytes) ─────────────────────────────────
        if attachment_filename.lower().endswith(".zip"):
            file_map = await self._extract_zip_files(attachment_data, attachment_filename, zip_password=zip_password)
            if file_map is None:
                return []
            if not file_map:
                await self._fire_alert(
                    f"[ETL] {_TAG} — no csv/dbf files in zip",
                    f"Zip '{attachment_filename}' opened successfully but contained "
                    f"no csv/dbf files.\nreq_id: {rid}",
                )
                return []
        elif self._is_direct_target(attachment_filename):
            file_map = {attachment_filename: attachment_data}
        else:
            logger.warning(
                "[req_id=%s] [%s] Unsupported attachment type: '%s'", rid, _TAG, attachment_filename
            )
            return []

        # ── 2. Process each extracted file ───────────────────────────────────
        results: list[str] = []
        for filename, raw_bytes in file_map.items():
            gcs_uri = await self._process_file(
                filename, raw_bytes, msg_id, up_date,
                report_type=report_type, gcs_prefix=gcs_prefix,
            )
            if gcs_uri:
                results.append(gcs_uri)

        return results

    # ------------------------------------------------------------------
    # Internal per-file pipeline
    # ------------------------------------------------------------------

    async def _process_file(
        self, filename: str, raw_bytes: bytes, msg_id: str, up_date: date,
        report_type: str = "", gcs_prefix: str = "",
    ) -> str | None:
        rid = get_request_id()
        logger.info("[req_id=%s] [%s] Processing file: '%s'", rid, _TAG, filename)

        # Convert DBF → CSV if needed
        csv_bytes = await self._to_csv_bytes(filename, raw_bytes)

        # Clean
        csv_bytes = self.clean_csv_bytes(csv_bytes)

        # Passthrough for all report types — original headers preserved.
        # BigQuery external table auto-converts "Product Code" → Product_Code etc.
        output_csv = csv_bytes.decode("utf-8", errors="replace")
        if not output_csv.strip():
            logger.warning("[req_id=%s] [%s] CSV is empty for '%s'", rid, _TAG, filename)
            return None

        # Short-circuit if no GCS service (test / dry-run mode)
        if self._gcs_service is None:
            logger.info("[req_id=%s] [%s] No GCS service — dry run for '%s'", rid, _TAG, filename)
            return filename

        # Upload to GCS
        try:
            gcs_uri = await self._gcs_service.upload_csv(
                output_csv, msg_id=msg_id, filename=filename, upload_date=up_date,
                prefix_override=gcs_prefix,
            )
        except Exception as exc:
            logger.error("[req_id=%s] [%s] GCS upload failed for '%s': %s", rid, _TAG, filename, exc)
            await self._fire_alert(
                f"[ETL] {_TAG} — GCS upload failed for '{filename}'",
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None

        # Run BigQuery MERGE (MFSD201 only — other report types have no MERGE SQL defined)
        if self._bq_service is not None and report_type == "MFSD201":
            try:
                await self._bq_service.run_merge(KFINTECH_MERGE_SQL)
            except Exception as exc:
                logger.error("[req_id=%s] [%s] BQ MERGE failed after upload of '%s': %s",
                             rid, _TAG, filename, exc)
                await self._fire_alert(
                    f"[ETL] {_TAG} — BigQuery MERGE failed after '{filename}' upload",
                    f"File was uploaded to GCS ({gcs_uri}) but the MERGE query failed.\n"
                    f"Error: {exc}\nreq_id: {rid}",
                )

        return gcs_uri
