"""
Entry point — concurrently fetches emails from CAMS and KFintech,
follows Drive links found in email bodies, extracts csv/dbf attachments,
and prints their names.  Sends alert emails on any download/extraction failure.
"""

import asyncio
import logging

from src.config.settings import (
    ALERT_EMAIL_TO,
    CAMS_SENDER_EMAIL,
    KFINTECH_SENDER_EMAIL,
)
from src.gmail.client import GmailClient, _header
from src.gmail.drive import extract_drive_file_ids
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


# ---------------------------------------------------------------------------
# Alert callback (shared across all processors in a run)
# ---------------------------------------------------------------------------

async def _send_alert(subject: str, body: str) -> None:
    client = GmailClient()
    await client.send_alert(_ALERT_RECIPIENTS, subject, body)


# ---------------------------------------------------------------------------
# Per-provider orchestration
# ---------------------------------------------------------------------------

async def process_provider_emails(provider: Provider, sender_email: str) -> None:
    rid = new_request_id()
    token = bind_request_id(rid)
    try:
        await _process(provider, sender_email)
    finally:
        reset_request_id(token)


async def _process(provider: Provider, sender_email: str) -> None:
    rid = get_request_id()
    client = GmailClient()
    processor = FileProcessorFactory.get_processor(provider, alert_callback=_send_alert)

    query = f"from:{sender_email} has:attachment"
    logger.info("[req_id=%s] ── %s ── query=%r", rid, provider.value.upper(), query)

    try:
        messages = await client.get_messages(query=query, max_results=5)
    except Exception as exc:
        logger.error("[req_id=%s] Failed to list messages for %s: %s", rid, provider.value, exc)
        await _send_alert(
            f"[ETL] {provider.value.upper()} — inbox fetch failed",
            f"Could not fetch emails for provider {provider.value}.\n"
            f"Query: {query}\nError: {exc}\nreq_id: {rid}",
        )
        return

    if not messages:
        logger.info("[req_id=%s] No messages found for %s.", rid, provider.value)
        return

    # Fetch all full messages concurrently
    full_messages = await asyncio.gather(
        *[client.get_message(m["id"]) for m in messages],
        return_exceptions=True,
    )

    for msg_stub, result in zip(messages, full_messages):
        if isinstance(result, Exception):
            logger.error("[req_id=%s] Could not fetch message %s: %s", rid, msg_stub["id"], result)
            continue

        message: dict = result
        subject = _header(message, "Subject")
        sender = _header(message, "From")
        logger.info("[req_id=%s] Processing email | from=%r | subject=%r", rid, sender, subject)

        # -- 1. Regular Gmail attachments ------------------------------------
        attachment_tasks = [
            _fetch_and_extract(client, processor, msg_stub["id"], part)
            for part in _iter_attachments(message)
        ]

        # -- 2. Drive links in email body ------------------------------------
        body_text = _decode_body(message)
        drive_ids = extract_drive_file_ids(body_text)
        if drive_ids:
            logger.info("[req_id=%s] Found %d Drive link(s) in email body.", rid, len(drive_ids))

        drive_tasks = [
            _download_drive_and_extract(client, processor, fid)
            for fid in drive_ids
        ]

        await asyncio.gather(*attachment_tasks, *drive_tasks, return_exceptions=False)


# ---------------------------------------------------------------------------
# Attachment helpers
# ---------------------------------------------------------------------------

async def _fetch_and_extract(client: GmailClient, processor, message_id: str, part: dict) -> None:
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
        await processor.extract(raw_data, filename)
    except Exception as exc:
        logger.error("[req_id=%s] Extraction failed for '%s': %s", rid, filename, exc)
        await _send_alert(
            f"[ETL] Extraction failed — {filename}",
            f"Processor raised an unexpected error on '{filename}'.\n"
            f"Error: {exc}\nreq_id: {rid}",
        )


async def _download_drive_and_extract(client: GmailClient, processor, file_id: str) -> None:
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
        await processor.extract(raw_data, filename)
    except Exception as exc:
        logger.error("[req_id=%s] Extraction failed for Drive file '%s': %s", rid, filename, exc)
        await _send_alert(
            f"[ETL] Extraction failed — {filename}",
            f"Processor raised an unexpected error on Drive file '{filename}'.\n"
            f"Error: {exc}\nreq_id: {rid}",
        )


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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    await asyncio.gather(
        process_provider_emails(Provider.CAMS, CAMS_SENDER_EMAIL),
        process_provider_emails(Provider.KFINTECH, KFINTECH_SENDER_EMAIL),
    )


if __name__ == "__main__":
    asyncio.run(main())
