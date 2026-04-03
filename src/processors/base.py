"""
Base async strategy — zip extraction with password support, file conversion,
CSV cleaning, and alert callbacks.

Zip handling
------------
- Plain zip (no password)         → extracted with pyzipper.
- AES / ZipCrypto encrypted zip   → extracted with pyzipper + provider password.
- Wrong password                  → AlertCallback fired, returns {}.
- Corrupt archive                 → AlertCallback fired, returns {}.

File conversion (DBF → CSV)
---------------------------
simpledbf requires a file path, so DBF bytes are written to a NamedTemporaryFile,
converted, then cleaned up automatically.

CSV cleaning
------------
Strips NULL bytes, normalises line endings, removes non-printable control chars.
"""

import asyncio
import io
import logging
import os
import tempfile
import zipfile
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from datetime import date

import pyzipper

from src.logging_context import get_request_id

logger = logging.getLogger(__name__)

# A coroutine that accepts (subject: str, body: str) and sends the alert.
AlertCallback = Callable[[str, str], Awaitable[None]]


class _WrongPasswordError(Exception):
    pass


class _CorruptZipError(Exception):
    pass


class FileProcessorStrategy(ABC):
    """Async strategy interface for provider-specific attachment processing."""

    SUPPORTED_EXTENSIONS = (".csv", ".dbf")

    # Override in subclass to supply a zip decryption password.
    zip_password: str | None = None

    # Injected by the factory.
    alert_callback: AlertCallback | None = None

    # Injected by the factory for GCS + BQ pipeline.
    # If None the pipeline short-circuits after mapping (safe for tests).
    _gcs_service = None
    _bq_service = None

    @abstractmethod
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
        """
        Process an attachment end-to-end.

        report_type  — e.g. "R2", "R9", "MFSD201"; used to decide whether
                       column mapping applies and to build the GCS path prefix.
        gcs_prefix   — overrides the service default prefix for this upload.

        Returns GCS URIs of uploaded files (empty list on failure).
        If _gcs_service is None, returns filenames only (test/dry-run mode).
        """

    # ------------------------------------------------------------------
    # Zip helpers
    # ------------------------------------------------------------------

    async def _extract_zip_files(
        self, data: bytes, source_name: str, zip_password: str | None = None
    ) -> dict[str, bytes] | None:
        """
        Extract all target files from a zip archive.

        Returns:
            dict[filename, content]  — may be empty if zip has no csv/dbf.
            None                     — fatal error; alert already fired.
        """
        loop = asyncio.get_running_loop()
        rid = get_request_id()
        effective_password = zip_password if zip_password is not None else self.zip_password
        try:
            return await loop.run_in_executor(None, self._parse_zip, data, source_name, effective_password)
        except _WrongPasswordError as exc:
            logger.error("[req_id=%s] Wrong password for zip '%s': %s", rid, source_name, exc)
            await self._fire_alert(
                f"[ETL] Wrong zip password — {source_name}",
                f"The zip archive '{source_name}' could not be decrypted.\n"
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None
        except _CorruptZipError as exc:
            logger.error("[req_id=%s] Corrupt or invalid zip '%s': %s", rid, source_name, exc)
            await self._fire_alert(
                f"[ETL] Corrupt zip — {source_name}",
                f"Failed to open zip archive '{source_name}'.\n"
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None

    def _parse_zip(self, data: bytes, source_name: str, zip_password: str | None = None) -> dict[str, bytes]:
        """
        Sync zip parser (runs in executor).

        Returns {filename: raw_bytes} for every csv/dbf file inside the archive.
        Raises _WrongPasswordError or _CorruptZipError — never fires async callbacks.
        """
        effective = zip_password if zip_password is not None else self.zip_password
        password_bytes = effective.encode() if effective else None

        try:
            with pyzipper.AESZipFile(io.BytesIO(data)) as zf:
                if password_bytes:
                    zf.setpassword(password_bytes)

                result: dict[str, bytes] = {}
                for name in zf.namelist():
                    if name.lower().endswith(self.SUPPORTED_EXTENSIONS):
                        try:
                            result[name] = zf.read(name)
                        except RuntimeError as exc:
                            msg = str(exc).lower()
                            if "password" in msg or "bad password" in msg:
                                raise _WrongPasswordError(str(exc)) from exc
                            raise _CorruptZipError(str(exc)) from exc

                if not result:
                    logger.warning(
                        "[req_id=%s] Zip '%s' contains no csv/dbf files.",
                        get_request_id(), source_name,
                    )
                return result

        except (_WrongPasswordError, _CorruptZipError):
            raise
        except RuntimeError as exc:
            msg = str(exc).lower()
            if "password" in msg or "bad password" in msg:
                raise _WrongPasswordError(str(exc)) from exc
            raise _CorruptZipError(str(exc)) from exc
        except (zipfile.BadZipFile, Exception) as exc:
            raise _CorruptZipError(str(exc)) from exc

    # ------------------------------------------------------------------
    # File conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _to_csv_bytes(filename: str, raw: bytes) -> bytes:
        """
        Convert a file's raw bytes to CSV bytes.

        - .csv  → cleaned as-is
        - .dbf  → converted via simpledbf (requires temp file on disk)
        """
        loop = asyncio.get_running_loop()
        if filename.lower().endswith(".dbf"):
            return await loop.run_in_executor(None, _dbf_to_csv_bytes, raw)
        # Already CSV — just return
        return raw

    @staticmethod
    def clean_csv_bytes(raw: bytes) -> bytes:
        """
        Strip NULL bytes, normalise line endings, remove non-printable control chars.
        Mirrors the reference Go cleanCSVData / cleanFieldValue logic.
        """
        text = raw.decode("utf-8", errors="replace")
        # Remove NULL bytes
        text = text.replace("\x00", "")
        # Normalise CRLF → LF
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        # Strip other non-printable control chars (keep tab and newline)
        text = "".join(ch for ch in text if ch >= " " or ch in ("\t", "\n"))
        return text.encode("utf-8")

    # ------------------------------------------------------------------
    # Direct-file helper
    # ------------------------------------------------------------------

    def _is_direct_target(self, filename: str) -> bool:
        return filename.lower().endswith(self.SUPPORTED_EXTENSIONS)

    # ------------------------------------------------------------------
    # Alert helper
    # ------------------------------------------------------------------

    async def _fire_alert(self, subject: str, body: str) -> None:
        if self.alert_callback:
            try:
                await self.alert_callback(subject, body)
            except Exception as exc:
                logger.error(
                    "[req_id=%s] Alert callback itself failed: %s",
                    get_request_id(), exc,
                )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _dbf_to_csv_bytes(raw: bytes) -> bytes:
    """
    Convert DBF bytes → CSV bytes using simpledbf.
    simpledbf requires a file path, so we use a temp file.
    """
    import csv as _csv
    import io as _io

    import simpledbf  # already in requirements.txt

    suffix = ".dbf"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(raw)
        tmp_path = tmp.name

    try:
        dbf = simpledbf.Dbf5(tmp_path)
        df = dbf.to_dataframe()
        buf = _io.StringIO()
        df.to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
