"""
NAV processor — HTML attachment from navbroadcast@camsonline.com.

Flow
----
.html attachment bytes
    │
    ▼
parse first HTML <table> → CSV rows
    │
    ▼
GcsUploadService.upload_csv()
    gs://nav_reports_bucket/nav_data/{YYYYMMDD}_{msg_id}.csv
"""

import csv
import io
import logging
from datetime import date
from html.parser import HTMLParser

from src.logging_context import get_request_id
from src.processors.base import FileProcessorStrategy

logger = logging.getLogger(__name__)
_TAG = "NAV"


class NavProcessor(FileProcessorStrategy):
    """Async strategy for NAV HTML attachments from CAMS (navbroadcast@camsonline.com)."""

    SUPPORTED_EXTENSIONS = (".html", ".htm")

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

        html_text = raw_bytes.decode("utf-8", errors="replace")
        try:
            output_csv = _html_table_to_csv(html_text)
        except Exception as exc:
            logger.error("[req_id=%s] [%s] HTML→CSV conversion failed for '%s': %s", rid, _TAG, filename, exc)
            await self._fire_alert(
                f"[ETL] {_TAG} — HTML→CSV failed for '{filename}'",
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None

        if not output_csv.strip():
            logger.warning("[req_id=%s] [%s] Converted CSV is empty for '%s'", rid, _TAG, filename)
            return None

        if self._gcs_service is None:
            logger.info("[req_id=%s] [%s] No GCS service — dry run for '%s'", rid, _TAG, filename)
            return filename

        # Path mirrors Go: nav_data/{YYYYMMDD}_{msg_id}.csv
        date_prefix = up_date.strftime("%Y%m%d")
        blob_path = f"nav_data/{date_prefix}_{msg_id}.csv"

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


# ---------------------------------------------------------------------------
# HTML table → CSV
# Mirrors Go's ConvertHTMLTableToCSV: finds the first <table>, extracts all
# <tr> rows, collects text from <td>/<th> cells recursively.
# ---------------------------------------------------------------------------

class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_table = False
        self._in_cell = False
        self._done = False
        self.rows: list[list[str]] = []
        self._current_row: list[str] = []
        self._cell_buf: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if self._done:
            return
        tag = tag.lower()
        if tag == "table" and not self._in_table:
            self._in_table = True
        elif tag == "tr" and self._in_table:
            self._current_row = []
        elif tag in ("td", "th") and self._in_table:
            self._in_cell = True
            self._cell_buf = []

    def handle_endtag(self, tag: str) -> None:
        if self._done:
            return
        tag = tag.lower()
        if tag == "table" and self._in_table:
            self._in_table = False
            self._done = True
        elif tag == "tr" and self._in_table:
            if self._current_row:
                self.rows.append(self._current_row)
        elif tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._current_row.append(" ".join(self._cell_buf).strip())
            self._cell_buf = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            stripped = data.strip()
            if stripped:
                self._cell_buf.append(stripped)


def _html_table_to_csv(html: str) -> str:
    parser = _TableParser()
    parser.feed(html)
    if not parser.rows:
        raise ValueError("No <table> found in HTML attachment")
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerows(parser.rows)
    return buf.getvalue()
