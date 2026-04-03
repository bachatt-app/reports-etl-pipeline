"""
Entry point — concurrently fetches emails from CAMS and KFintech,
follows Drive links found in email bodies, extracts csv/dbf attachments,
and prints their names.  Sends alert emails on any download/extraction failure.
"""

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.config.settings import (
    ALERT_EMAIL_TO,
    ALLOW_ALL_SENDERS,
    ALLOWED_SENDERS,
    BQ_DATASET,
    BQ_PROJECT_ID,
    CAMS_ZIP_PASSWORD,
    GCS_CAMS_BUCKET,
    GCS_CAMS_PREFIX,
    GCS_BSE_BUCKET,
    GCS_KFINTECH_BUCKET,
    GCS_KFINTECH_PREFIX,
    GCS_NAV_BUCKET,
    GCS_RAZORPAY_BUCKET,
    GMAIL_LOOKBACK_DAYS,
    KFINTECH_R9_AUTO_ZIP_PASSWORD,
    KFINTECH_ZIP_PASSWORD,
)
from src.storage.bigquery import BigQueryMergeService
from src.storage.gcs import GcsUploadService
from src.gmail.client import GmailClient, _header
from src.gmail.drive import (
    extract_cams_download_url,
    extract_drive_file_ids,
    extract_kfintech_download_url,
)
from src.logging_context import (
    bind_request_id,
    configure_logging,
    get_request_id,
    new_request_id,
    reset_request_id,
)
from src.processors.factory import FileProcessorFactory, Provider

configure_logging(logging.INFO)
logger = logging.getLogger(__name__)

# Alert recipients parsed from comma-separated env var
_ALERT_RECIPIENTS = [e.strip() for e in ALERT_EMAIL_TO.split(",") if e.strip()]

# Single source of truth: maps each allowed sender address → provider.
# Replaces the individual *_SENDER env vars — ALLOWED_SENDERS drives both
# the Gmail query filter and the per-email provider routing.
_SENDER_TO_PROVIDER: dict[str, Provider] = {
    "donotreply@camsonline.com":    Provider.CAMS,
    "distributorcare@kfintech.com": Provider.KFINTECH,
    "navbroadcast@camsonline.com":  Provider.NAV,
    "ashutosh.kashyap@bachatt.app": Provider.RAZORPAY,
    "kartikeya.mishra@bachatt.app": Provider.BSE,
}


def _provider_for_sender(sender: str) -> "Provider | None":
    """Return the provider for an email's From header, or None if unrecognised."""
    s = sender.lower()
    for addr, provider in _SENDER_TO_PROVIDER.items():
        if addr in s:
            return provider
    return None


# ---------------------------------------------------------------------------
# Alert callback (shared across all processors in a run)
# ---------------------------------------------------------------------------

async def _send_alert(subject: str, body: str) -> None:
    client = GmailClient()
    await client.send_alert(_ALERT_RECIPIENTS, subject, body)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _make_services(provider: Provider):
    """Build GCS + BQ service pair for the given provider (None if bucket unconfigured)."""
    bucket_map = {
        Provider.CAMS:     (GCS_CAMS_BUCKET,     GCS_CAMS_PREFIX),
        Provider.KFINTECH: (GCS_KFINTECH_BUCKET,  GCS_KFINTECH_PREFIX),
        Provider.RAZORPAY: (GCS_RAZORPAY_BUCKET,  "razorpay"),
        Provider.NAV:      (GCS_NAV_BUCKET,        "nav_data"),
        Provider.BSE:      (GCS_BSE_BUCKET,        "bse_report"),
    }
    bucket, prefix = bucket_map.get(provider, ("", ""))
    gcs = GcsUploadService(bucket=bucket, prefix=prefix) if bucket else None
    bq = BigQueryMergeService(project=BQ_PROJECT_ID, dataset=BQ_DATASET) if BQ_PROJECT_ID else None
    return gcs, bq


async def _process_one_email(
    provider: Provider,
    client: GmailClient,
    stub: dict,
    message: dict,
) -> None:
    """Process a single email routed to the given provider."""
    rid = new_request_id()
    token = bind_request_id(rid)
    try:
        gcs_svc, bq_svc = _make_services(provider)
        processor = FileProcessorFactory.get_processor(
            provider, alert_callback=_send_alert, gcs_service=gcs_svc, bq_service=bq_svc,
        )

        subject = _header(message, "Subject")
        sender  = _header(message, "From")
        logger.info(
            "[req_id=%s] Processing | provider=%s | from=%r | subject=%r",
            rid, provider.value.upper(), sender, subject,
        )

        body_text = _decode_body(message)
        body_html = _decode_html_body(message)

        report_type, gcs_prefix, zip_password = _detect_report_type(
            provider, subject, body_text + "\n" + body_html
        )
        if not report_type:
            logger.warning(
                "[req_id=%s] Could not determine report type for %s | subject=%r — skipping",
                rid, provider.value, subject,
            )
            return
        logger.info("[req_id=%s] report_type=%r  gcs_prefix=%r", rid, report_type, gcs_prefix)

        # -- 1. Regular Gmail attachments ------------------------------------
        attachment_tasks = [
            _fetch_and_extract(client, processor, stub["id"], part,
                               report_type, gcs_prefix, zip_password)
            for part in _iter_attachments(message)
        ]

        # -- 2. Drive links in email body ------------------------------------
        drive_ids = extract_drive_file_ids(body_text)
        if drive_ids:
            logger.info("[req_id=%s] Found %d Drive link(s) in email body.", rid, len(drive_ids))
        drive_tasks = [
            _download_drive_and_extract(client, processor, fid,
                                        report_type, gcs_prefix, zip_password)
            for fid in drive_ids
        ]

        # -- 3. KFintech pre-signed delivery URL (scdelivery.kfintech.com) ---
        kfintech_url = extract_kfintech_download_url(body_html)
        kfintech_tasks = []
        if kfintech_url:
            logger.info("[req_id=%s] Found KFintech delivery URL in email body.", rid)
            kfintech_tasks = [
                _download_url_and_extract(
                    client, processor, kfintech_url, stub["id"],
                    report_type, gcs_prefix, zip_password,
                )
            ]

        # -- 4. CAMS delivery URL (mailback.camsonline.com) ------------------
        cams_url = extract_cams_download_url(body_html) if provider == Provider.CAMS else None
        if cams_url:
            logger.info("[req_id=%s] Found CAMS delivery URL in email body.", rid)
            attachment_tasks.append(
                _download_url_and_extract(
                    client, processor, cams_url, stub["id"],
                    report_type, gcs_prefix, zip_password,
                )
            )

        await asyncio.gather(*attachment_tasks, *drive_tasks, *kfintech_tasks, return_exceptions=False)
    finally:
        reset_request_id(token)


# ---------------------------------------------------------------------------
# Attachment helpers
# ---------------------------------------------------------------------------

async def _fetch_and_extract(
    client: GmailClient, processor, message_id: str, part: dict,
    report_type: str = "", gcs_prefix: str = "", zip_password: str | None = None,
) -> None:
    rid = get_request_id()
    filename = part.get("filename", "")
    attachment_id = part.get("body", {}).get("attachmentId")

    if not filename or not attachment_id:
        return

    try:
        raw_data = await client.get_attachment(message_id, attachment_id)
    except Exception as exc:
        logger.error("[req_id=%s] Download failed for attachment '%s': %s", rid, filename, exc)
        await _send_alert(
            f"[ETL] Attachment download failed — {filename}",
            f"Could not download attachment '{filename}' from message {message_id}.\n"
            f"Error: {exc}\nreq_id: {rid}",
        )
        return

    try:
        await processor.extract(
            raw_data, filename, msg_id=message_id, upload_date=date.today(),
            report_type=report_type, gcs_prefix=gcs_prefix, zip_password=zip_password,
        )
    except Exception as exc:
        logger.error("[req_id=%s] Extraction failed for '%s': %s", rid, filename, exc)
        await _send_alert(
            f"[ETL] Extraction failed — {filename}",
            f"Processor raised an unexpected error on '{filename}'.\n"
            f"Error: {exc}\nreq_id: {rid}",
        )


async def _download_drive_and_extract(
    client: GmailClient, processor, file_id: str,
    report_type: str = "", gcs_prefix: str = "", zip_password: str | None = None,
) -> None:
    rid = get_request_id()

    try:
        filename = await client.drive_filename(file_id)
    except Exception as exc:
        logger.error("[req_id=%s] Could not get Drive filename for id=%s: %s", rid, file_id, exc)
        filename = file_id  # fall back to id as name for the alert

    try:
        raw_data = await client.drive_download(file_id)
    except Exception as exc:
        logger.error("[req_id=%s] Drive download failed for '%s' (id=%s): %s", rid, filename, file_id, exc)
        await _send_alert(
            f"[ETL] Drive download failed — {filename}",
            f"Could not download Drive file '{filename}' (id={file_id}).\n"
            f"Error: {exc}\nreq_id: {rid}",
        )
        return

    try:
        await processor.extract(
            raw_data, filename, msg_id=file_id, upload_date=date.today(),
            report_type=report_type, gcs_prefix=gcs_prefix, zip_password=zip_password,
        )
    except Exception as exc:
        logger.error("[req_id=%s] Extraction failed for Drive file '%s': %s", rid, filename, exc)
        await _send_alert(
            f"[ETL] Extraction failed — {filename}",
            f"Processor raised an unexpected error on Drive file '{filename}'.\n"
            f"Error: {exc}\nreq_id: {rid}",
        )


async def _download_url_and_extract(
    client: GmailClient, processor, url: str, msg_id: str,
    report_type: str = "", gcs_prefix: str = "", zip_password: str | None = None,
) -> None:
    """Download from a pre-signed delivery URL (KFintech or CAMS) and extract."""
    rid = get_request_id()
    filename = url.split("/")[-1].split("?")[0] or "report.zip"

    try:
        raw_data = await client.download_from_url(url)
    except Exception as exc:
        logger.error("[req_id=%s] URL download failed for '%s': %s", rid, url, exc)
        await _send_alert(
            f"[ETL] URL download failed — {filename}",
            f"Could not download report from delivery URL.\n"
            f"URL: {url}\nError: {exc}\nreq_id: {rid}",
        )
        return

    try:
        await processor.extract(
            raw_data, filename, msg_id=msg_id, upload_date=date.today(),
            report_type=report_type, gcs_prefix=gcs_prefix, zip_password=zip_password,
        )
    except Exception as exc:
        logger.error("[req_id=%s] Extraction failed for '%s': %s", rid, filename, exc)
        await _send_alert(
            f"[ETL] Extraction failed — {filename}",
            f"Processor raised an unexpected error on '{filename}'.\n"
            f"Error: {exc}\nreq_id: {rid}",
        )


# ---------------------------------------------------------------------------
# Report type detection
# ---------------------------------------------------------------------------

_CAMS_WBR_RE = re.compile(r'WBR(\d+[a-zA-Z]*)', re.IGNORECASE)

def _detect_report_type(
    provider: "Provider", subject: str, body: str
) -> tuple[str, str, str | None]:
    """
    Return (report_type, gcs_prefix, zip_password) for an email.

    zip_password is None → use the processor's default password from settings.

    CAMS  — type extracted from subject via WBR<N> (e.g. WBR2 → "R2").
            GCS prefix = "R2", "R9", "R108", etc.
            Password always from CAMS_ZIP_PASSWORD env var (None = use default).

    KFintech — type detected from email body text keywords (Go reference).
               GCS prefix = "r2_kfintech", "r1_kfintech", etc.
               Password varies: R9 automated uses KFINTECH_R9_AUTO_ZIP_PASSWORD.

    Returns ("", "", None) if type cannot be determined (email should be skipped).
    """
    if provider == Provider.CAMS:
        m = _CAMS_WBR_RE.search(subject)
        if m:
            r_num = m.group(1).upper()
            report_type = f"R{r_num}"
            return report_type, report_type, None  # e.g. ("R2", "R2", None)
        return "", "", None

    if provider == Provider.RAZORPAY:
        return "razorpay", "razorpay", None

    if provider == Provider.NAV:
        return "nav", "nav_data", None

    if provider == Provider.BSE:
        return "bse", "bse_report", None

    # KFintech — match on body keywords (mirrors Go reference switch/case)
    if "Transaction Report" in body:
        return "MFSD201", "r2_kfintech", KFINTECH_ZIP_PASSWORD
    if "NAV Report" in body:
        return "R1", "r1_kfintech", KFINTECH_ZIP_PASSWORD
    if "ClientWise AUM Report" in body:
        # Automated subscription — different password (no @ in "Bachatt2025")
        return "R9", "r9_kfintech", KFINTECH_R9_AUTO_ZIP_PASSWORD
    if "Client-wise AUM Report" in body:
        # Manual request — same password as other reports
        return "R9", "r9_kfintech", KFINTECH_ZIP_PASSWORD
    if "Investor Master Information" in body:
        return "MFSD211", "mfsd211_kfintech", KFINTECH_ZIP_PASSWORD
    return "", "", None


# ---------------------------------------------------------------------------
# Message parsing helpers
# ---------------------------------------------------------------------------

def _iter_attachments(message: dict):
    """Yield all parts that carry a downloadable attachment."""
    payload = message.get("payload", {})
    parts = payload.get("parts") or [payload]
    for part in parts:
        if part.get("filename") and part.get("body", {}).get("attachmentId"):
            yield part
        for sub in part.get("parts", []):
            if sub.get("filename") and sub.get("body", {}).get("attachmentId"):
                yield sub


def _decode_body(message: dict) -> str:
    """Extract all plaintext body parts, base64-decoded."""
    import base64
    texts: list[str] = []

    def _walk(part: dict) -> None:
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data", "")
        if mime == "text/plain" and data:
            try:
                texts.append(base64.urlsafe_b64decode(data).decode(errors="replace"))
            except Exception:
                pass
        for sub in part.get("parts", []):
            _walk(sub)

    _walk(message.get("payload", {}))
    return "\n".join(texts)


def _decode_html_body(message: dict) -> str:
    """Extract all HTML body parts, base64-decoded (used for KFintech href extraction)."""
    import base64
    parts: list[str] = []

    def _walk(part: dict) -> None:
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data", "")
        if mime == "text/html" and data:
            try:
                parts.append(base64.urlsafe_b64decode(data).decode(errors="replace"))
            except Exception:
                pass
        for sub in part.get("parts", []):
            _walk(sub)

    _walk(message.get("payload", {}))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Sync state — persists last successful run time to avoid re-processing old mail
#
# Local dev  : saved to LAST_SYNC_FILE (default: last_sync.txt)
# Cloud Run  : use /tmp/last_sync.txt — resets on cold start (ephemeral).
#              For true cross-invocation persistence, point LAST_SYNC_FILE
#              at a GCS-mounted path or switch to Firestore/GCS object.
# ---------------------------------------------------------------------------

_LAST_SYNC_FILE = Path(os.getenv("LAST_SYNC_FILE", "last_sync.txt"))


def _read_last_sync() -> datetime | None:
    """Return the last successful sync time (UTC-aware), or None if not recorded."""
    try:
        text = _LAST_SYNC_FILE.read_text().strip()
        if text and text.lower() != "none":
            dt = datetime.fromisoformat(text)
            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    except FileNotFoundError:
        pass
    except Exception as exc:
        logger.warning("Could not read last sync file %s: %s", _LAST_SYNC_FILE, exc)
    return None


def _write_last_sync(dt: datetime) -> None:
    """Persist the sync time so the next run knows where to start from."""
    try:
        _LAST_SYNC_FILE.write_text(dt.isoformat())
        logger.info("Last sync time updated → %s", dt.isoformat())
    except Exception as exc:
        logger.warning("Could not write last sync file %s: %s", _LAST_SYNC_FILE, exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    rid = new_request_id()
    bind_request_id(rid)

    now = datetime.now(tz=timezone.utc)
    last_sync = _read_last_sync()

    if last_sync is None:
        since_dt = now - timedelta(days=GMAIL_LOOKBACK_DAYS)
        logger.info(
            "[req_id=%s] No last sync recorded — fetching from %s (%d-day lookback)",
            rid, since_dt.isoformat(), GMAIL_LOOKBACK_DAYS,
        )
    else:
        since_dt = last_sync - timedelta(minutes=1)   # 1-min overlap to avoid gaps
        logger.info(
            "[req_id=%s] Last sync: %s — fetching from %s (1-min overlap)",
            rid, last_sync.isoformat(), since_dt.isoformat(),
        )

    # Gmail supports after:<unix_epoch> for second-level precision
    since_epoch = int(since_dt.timestamp())

    client = GmailClient()

    if ALLOW_ALL_SENDERS:
        query = f"after:{since_epoch}"
    else:
        senders_filter = " OR ".join(ALLOWED_SENDERS)
        query = f"from:({senders_filter}) after:{since_epoch}"

    logger.info("[req_id=%s] Fetching emails | query=%r", rid, query)

    try:
        stubs = await client.get_messages(query=query, max_results=5)
    except Exception as exc:
        logger.error("[req_id=%s] Failed to list messages: %s", rid, exc)
        await _send_alert(
            "[ETL] inbox fetch failed",
            f"Query: {query}\nError: {exc}\nreq_id: {rid}",
        )
        return

    if not stubs:
        logger.info("[req_id=%s] No messages found.", rid)
        _write_last_sync(now)
        return

    # Fetch full messages sequentially — httplib2 is not thread-safe
    pairs: list[tuple[dict, dict]] = []
    for stub in stubs:
        try:
            pairs.append((stub, await client.get_message(stub["id"])))
        except Exception as exc:
            logger.error("[req_id=%s] Could not fetch message %s: %s", rid, stub["id"], exc)

    # Route each email by sender and process all concurrently
    tasks = []
    for stub, msg in pairs:
        sender = _header(msg, "From")
        provider = _provider_for_sender(sender)
        if provider is None:
            logger.debug("[req_id=%s] No provider for sender %r — skipping", rid, sender)
            continue
        tasks.append(_process_one_email(provider, client, stub, msg))

    if tasks:
        await asyncio.gather(*tasks)

    # Persist sync time only after all processing is done
    _write_last_sync(now)


# ---------------------------------------------------------------------------
# Cloud Run Functions entry point (HTTP trigger)
# ---------------------------------------------------------------------------

def run_pipeline(request=None):
    """
    HTTP entry point for Cloud Run Functions.
    Triggered by Cloud Scheduler every hour.

    Resets the GmailClient singleton on each invocation so that the
    asyncio.Lock is re-created for the fresh event loop that asyncio.run()
    creates — avoids "Future attached to a different loop" on warm instances.
    """
    GmailClient._instance = None
    asyncio.run(main())
    return ("Pipeline complete", 200)


if __name__ == "__main__":
    asyncio.run(main())
