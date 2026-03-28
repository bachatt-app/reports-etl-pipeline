"""
Async Gmail client — Singleton with robust token lifecycle management.

Auth modes
----------
USE_SECRET_MANAGER=false (default / local dev)
    Credentials loaded from credentials.json + token.json on disk.
    OAuth consent flow launched in browser on first run.

USE_SECRET_MANAGER=true (production / GCP)
    OAuth client credentials fetched from Secret Manager secret
    ``gmail-client-credentials``.  Refresh token fetched from
    ``gmail-refresh-token``.  Access token is auto-refreshed by the
    oauth2 TokenSource — no local files needed.

Token expiry scenarios (local mode)
-------------------------------------
  1. Valid token              → used as-is.
  2. Expired + refresh_token  → silently refreshed, token file updated.
  3. Expired + no refresh     → OAuth consent flow re-launched.
  4. Revoked / invalid token  → stale file deleted, OAuth re-launched.
  5. Network error on refresh → retried with exponential back-off.
  6. Concurrent coroutines    → asyncio.Lock ensures one refresh at a time.
"""

import asyncio
import base64
import functools
import json
import logging
import os
import threading
from email.mime.text import MIMEText
from pathlib import Path

import aiofiles
from google.auth.exceptions import RefreshError, TransportError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config.settings import (
    ALERT_EMAIL_FROM,
    GMAIL_CREDENTIALS_FILE,
    GMAIL_CREDENTIALS_SECRET,
    GMAIL_REFRESH_TOKEN_SECRET,
    GMAIL_SCOPES,
    GMAIL_TOKEN_FILE,
    GMAIL_USER,
    GCP_PROJECT_ID,
    USE_SECRET_MANAGER,
)
from src.logging_context import get_request_id

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry policy — transient network / server errors only (not auth errors)
# ---------------------------------------------------------------------------
_RETRY_POLICY = dict(
    retry=retry_if_exception_type((TransportError, HttpError)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=16),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


class GmailClient:
    """
    Thread-safe, async-ready Singleton Gmail API client.

    Usage::

        client = GmailClient()
        messages = await client.get_messages(query="from:x has:attachment")
    """

    _instance: "GmailClient | None" = None
    _thread_lock = threading.Lock()          # singleton creation only

    # ------------------------------------------------------------------
    # Singleton
    # ------------------------------------------------------------------

    def __new__(cls) -> "GmailClient":
        if cls._instance is None:
            with cls._thread_lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._service = None
                    inst._drive_service = None
                    inst._creds: Credentials | None = None
                    inst._user = GMAIL_USER
                    inst._auth_lock = asyncio.Lock()
                    cls._instance = inst
        return cls._instance

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _run(func, *args, **kwargs):
        """Offload a blocking call to the default thread-pool executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

    def _log(self, level: int, msg: str, *args) -> None:
        logger.log(level, "[req_id=%s] %s", get_request_id(), msg % args if args else msg)

    # ------------------------------------------------------------------
    # Auth — Secret Manager path (production)
    # ------------------------------------------------------------------

    async def _load_creds_from_secret_manager(self) -> Credentials:
        """Fetch OAuth credentials + refresh token from GCP Secret Manager."""
        from google.cloud import secretmanager  # lazy import

        self._log(logging.INFO, "Loading credentials from Secret Manager (project=%s)", GCP_PROJECT_ID)
        sm = secretmanager.SecretManagerServiceAsyncClient()

        def _secret_name(secret_id: str) -> str:
            return f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"

        creds_resp, token_resp = await asyncio.gather(
            sm.access_secret_version(name=_secret_name(GMAIL_CREDENTIALS_SECRET)),
            sm.access_secret_version(name=_secret_name(GMAIL_REFRESH_TOKEN_SECRET)),
        )

        creds_json = json.loads(creds_resp.payload.data.decode())
        token_data = json.loads(token_resp.payload.data.decode())
        refresh_token = token_data.get("refresh_token", "")

        if not refresh_token:
            raise ValueError("Secret Manager: 'refresh_token' field is empty or missing.")

        # Support both "installed" and "web" OAuth client types
        client_cfg = creds_json.get("installed") or creds_json.get("web", {})
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri=client_cfg.get("token_uri", "https://oauth2.googleapis.com/token"),
            client_id=client_cfg["client_id"],
            client_secret=client_cfg["client_secret"],
            scopes=GMAIL_SCOPES,
        )

        self._log(logging.INFO, "Refreshing access token via Secret Manager credentials…")
        await self._run(creds.refresh, Request())
        self._log(logging.INFO, "Access token obtained successfully.")
        return creds

    # ------------------------------------------------------------------
    # Auth — local file path (development)
    # ------------------------------------------------------------------

    async def _persist_token(self, creds: Credentials) -> None:
        async with aiofiles.open(GMAIL_TOKEN_FILE, "w") as fh:
            await fh.write(creds.to_json())
        self._log(logging.DEBUG, "Token persisted to %s", GMAIL_TOKEN_FILE)

    async def _run_oauth_flow(self) -> Credentials:
        self._log(logging.INFO, "Launching OAuth consent flow…")
        flow = InstalledAppFlow.from_client_secrets_file(GMAIL_CREDENTIALS_FILE, GMAIL_SCOPES)
        creds: Credentials = await self._run(flow.run_local_server, port=0)
        await self._persist_token(creds)
        return creds

    async def _refresh_file_creds(self, creds: Credentials) -> Credentials:
        try:
            self._log(logging.INFO, "Refreshing expired access token…")
            await self._run(creds.refresh, Request())
            await self._persist_token(creds)
            self._log(logging.INFO, "Token refreshed successfully.")
            return creds
        except RefreshError as exc:
            self._log(logging.WARNING, "Refresh token invalid/revoked (%s). Re-authenticating…", exc)
            _delete_token_file()
            return await self._run_oauth_flow()

    async def _load_creds_from_file(self) -> Credentials:
        creds: Credentials | None = None

        if Path(GMAIL_TOKEN_FILE).exists():
            try:
                creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, GMAIL_SCOPES)
                self._log(logging.DEBUG, "Loaded token from %s", GMAIL_TOKEN_FILE)
            except Exception as exc:
                self._log(logging.WARNING, "Corrupt token file (%s) — deleting.", exc)
                _delete_token_file()
                creds = None

        if creds and creds.expired and creds.refresh_token:
            return await self._refresh_file_creds(creds)

        if not creds or not creds.valid:
            return await self._run_oauth_flow()

        return creds

    # ------------------------------------------------------------------
    # Authentication entry-point
    # ------------------------------------------------------------------

    @retry(**_RETRY_POLICY)
    async def _ensure_authenticated(self) -> None:
        """
        Guarantee self._creds is valid.  asyncio.Lock prevents concurrent
        refreshes — other coroutines wait and reuse already-valid credentials.
        """
        async with self._auth_lock:
            if self._creds and self._creds.valid:
                return

            if USE_SECRET_MANAGER:
                self._creds = await self._load_creds_from_secret_manager()
            else:
                self._creds = await self._load_creds_from_file()

            # Build (or rebuild) both API services after creds change
            self._service = await self._run(build, "gmail", "v1", credentials=self._creds)
            self._drive_service = await self._run(build, "drive", "v3", credentials=self._creds)
            self._log(logging.INFO, "Gmail + Drive services ready (user=%s, mode=%s).",
                      self._user, "secret-manager" if USE_SECRET_MANAGER else "local-file")

    async def _gmail(self):
        await self._ensure_authenticated()
        return self._service

    async def _drive(self):
        await self._ensure_authenticated()
        return self._drive_service

    # ------------------------------------------------------------------
    # Gmail — read
    # ------------------------------------------------------------------

    @retry(**_RETRY_POLICY)
    async def get_messages(self, query: str = "", max_results: int = 10) -> list[dict]:
        """Return message stubs matching a Gmail search query."""
        svc = await self._gmail()
        result = await self._run(
            svc.users().messages().list(
                userId=self._user, q=query, maxResults=max_results
            ).execute
        )
        msgs = result.get("messages", [])
        self._log(logging.INFO, "Fetched %d message stubs | query=%r", len(msgs), query)
        return msgs

    @retry(**_RETRY_POLICY)
    async def get_message(self, message_id: str) -> dict:
        """Fetch a full message payload by ID."""
        svc = await self._gmail()
        msg = await self._run(
            svc.users().messages().get(
                userId=self._user, id=message_id, format="full"
            ).execute
        )
        subject = _header(msg, "Subject")
        sender = _header(msg, "From")
        self._log(logging.INFO, "Fetched message id=%s | from=%r | subject=%r",
                  message_id, sender, subject)
        return msg

    @retry(**_RETRY_POLICY)
    async def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """Download and base64-decode a Gmail attachment."""
        svc = await self._gmail()
        raw = await self._run(
            svc.users().messages().attachments().get(
                userId=self._user, messageId=message_id, id=attachment_id
            ).execute
        )
        data = base64.urlsafe_b64decode(raw.get("data", ""))
        self._log(logging.DEBUG, "Downloaded attachment id=%s (%d bytes)", attachment_id, len(data))
        return data

    # ------------------------------------------------------------------
    # Drive — download
    # ------------------------------------------------------------------

    @retry(**_RETRY_POLICY)
    async def drive_download(self, file_id: str) -> bytes:
        """Download raw bytes of a Google Drive file."""
        svc = await self._drive()
        self._log(logging.INFO, "Downloading Drive file id=%s", file_id)
        raw: bytes = await self._run(_drive_download_sync, svc, file_id)
        self._log(logging.INFO, "Drive file id=%s downloaded (%d bytes)", file_id, len(raw))
        return raw

    @retry(**_RETRY_POLICY)
    async def drive_filename(self, file_id: str) -> str:
        """Fetch the filename metadata of a Drive file."""
        svc = await self._drive()
        meta = await self._run(
            svc.files().get(fileId=file_id, fields="name").execute
        )
        name: str = meta.get("name", file_id)
        self._log(logging.DEBUG, "Drive file id=%s → name=%r", file_id, name)
        return name

    # ------------------------------------------------------------------
    # Gmail — send alert
    # ------------------------------------------------------------------

    async def send_alert(self, to: list[str], subject: str, body: str) -> None:
        """Send a plain-text alert email via the Gmail API."""
        if not to:
            self._log(logging.WARNING, "send_alert called with empty recipient list — skipped.")
            return

        mime = MIMEText(body, "plain")
        mime["To"] = ", ".join(to)
        mime["Subject"] = subject

        raw_msg = base64.urlsafe_b64encode(mime.as_bytes()).decode()
        svc = await self._gmail()

        try:
            await self._run(
                svc.users().messages().send(
                    userId=ALERT_EMAIL_FROM, body={"raw": raw_msg}
                ).execute
            )
            self._log(logging.INFO, "Alert sent | to=%s | subject=%r", to, subject)
        except HttpError as exc:
            self._log(logging.ERROR, "Failed to send alert email: %s", exc)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _header(message: dict, name: str) -> str:
    for h in message.get("payload", {}).get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def _drive_download_sync(svc, file_id: str) -> bytes:
    import io as _io
    request = svc.files().get_media(fileId=file_id)
    buf = _io.BytesIO()
    from googleapiclient.http import MediaIoBaseDownload
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


def _delete_token_file() -> None:
    try:
        os.remove(GMAIL_TOKEN_FILE)
        logger.info("Deleted stale token file: %s", GMAIL_TOKEN_FILE)
    except FileNotFoundError:
        pass
