"""
Base async strategy — zip extraction with password support and alert callbacks.

Zip handling
------------
- Plain zip (no password)         → extracted with standard zipfile.
- AES / ZipCrypto encrypted zip   → extracted with pyzipper + provider password.
- Wrong password                  → AlertCallback fired, returns [].
- Corrupt archive                 → AlertCallback fired, returns [].
"""

import asyncio
import io
import logging
import zipfile
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

import pyzipper

from src.logging_context import get_request_id

logger = logging.getLogger(__name__)

# A coroutine that accepts (subject: str, body: str) and sends the alert.
AlertCallback = Callable[[str, str], Awaitable[None]]


# Typed exceptions raised by the sync _parse_zip so the async layer can handle them cleanly.
class _WrongPasswordError(Exception):
    pass


class _CorruptZipError(Exception):
    pass


class FileProcessorStrategy(ABC):
    """Async strategy interface for provider-specific attachment processing."""

    SUPPORTED_EXTENSIONS = (".csv", ".dbf")

    # Override in subclass to supply a zip decryption password.
    zip_password: str | None = None

    # Injected by the orchestrator so processors can fire alerts.
    alert_callback: AlertCallback | None = None

    @abstractmethod
    async def extract(self, attachment_data: bytes, attachment_filename: str) -> list[str]:
        """Return names of csv/dbf files found in the attachment."""

    # ------------------------------------------------------------------
    # Zip helpers
    # ------------------------------------------------------------------

    async def _extract_from_zip(self, data: bytes, source_name: str) -> list[str] | None:
        """
        Parse a zip archive in a thread-pool executor (CPU-bound).

        Returns:
            list[str]  — filenames found (may be empty if zip has no csv/dbf).
            None       — a fatal error occurred; alert was already fired.
        """
        loop = asyncio.get_running_loop()
        rid = get_request_id()
        try:
            return await loop.run_in_executor(None, self._parse_zip, data, source_name)
        except _WrongPasswordError as exc:
            logger.error("[req_id=%s] Wrong password for zip '%s': %s", rid, source_name, exc)
            await self._fire_alert(
                f"[ETL] Wrong zip password — {source_name}",
                f"The zip archive '{source_name}' could not be decrypted.\n"
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None   # alert already fired
        except _CorruptZipError as exc:
            logger.error("[req_id=%s] Corrupt or invalid zip '%s': %s", rid, source_name, exc)
            await self._fire_alert(
                f"[ETL] Corrupt zip — {source_name}",
                f"Failed to open zip archive '{source_name}'.\n"
                f"Error: {exc}\nreq_id: {rid}",
            )
            return None   # alert already fired

    def _parse_zip(self, data: bytes, source_name: str) -> list[str]:
        """
        Sync zip parser (runs in executor). Raises _WrongPasswordError or
        _CorruptZipError — never fires async callbacks directly.
        """
        password_bytes = self.zip_password.encode() if self.zip_password else None

        try:
            with pyzipper.AESZipFile(io.BytesIO(data)) as zf:
                if password_bytes:
                    zf.setpassword(password_bytes)

                names = self._collect_targets(zf, source_name)

                # AES zip filenames are listed without decryption; attempt a read
                # on the first target file to eagerly validate the password.
                if names and password_bytes:
                    zf.read(names[0])

                return names

        except RuntimeError as exc:
            msg = str(exc).lower()
            if "password" in msg or "bad password" in msg:
                raise _WrongPasswordError(str(exc)) from exc
            raise _CorruptZipError(str(exc)) from exc

        except (zipfile.BadZipFile, Exception) as exc:
            raise _CorruptZipError(str(exc)) from exc

    def _collect_targets(self, zf, source_name: str) -> list[str]:
        found = []
        for name in zf.namelist():
            if name.lower().endswith(self.SUPPORTED_EXTENSIONS):
                found.append(name)
        if not found:
            logger.warning(
                "[req_id=%s] Zip '%s' contains no csv/dbf files.",
                get_request_id(), source_name,
            )
        return found

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
