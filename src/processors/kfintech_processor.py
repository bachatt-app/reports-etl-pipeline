import logging

from src.logging_context import get_request_id
from src.processors.base import FileProcessorStrategy

logger = logging.getLogger(__name__)

_TAG = "KFintech"


class KFintechProcessor(FileProcessorStrategy):
    """
    Async strategy for KFintech (Karvy Fintech) attachments.

    Expects:
      - Direct .dbf attachment, or
      - .zip archive (optionally password-protected) containing csv/dbf files.
    """

    async def extract(self, attachment_data: bytes, attachment_filename: str) -> list[str]:
        rid = get_request_id()
        logger.info("[req_id=%s] [%s] Processing attachment: '%s'", rid, _TAG, attachment_filename)

        if attachment_filename.lower().endswith(".zip"):
            # None → error already alerted; [] → valid zip but no csv/dbf inside
            names = await self._extract_from_zip(attachment_data, attachment_filename)
            if names is None:
                return []
            for name in names:
                logger.info("[req_id=%s] [%s] Found file inside zip: '%s'", rid, _TAG, name)
                print(f"[{_TAG}] [req_id={rid}] Found file inside zip: '{name}'")
            if not names:
                await self._fire_alert(
                    f"[ETL] {_TAG} — no csv/dbf files in zip",
                    f"The zip '{attachment_filename}' opened successfully but contained "
                    f"no csv/dbf files.\nreq_id: {rid}",
                )
            return names

        if self._is_direct_target(attachment_filename):
            logger.info("[req_id=%s] [%s] Found file: '%s'", rid, _TAG, attachment_filename)
            print(f"[{_TAG}] [req_id={rid}] Found file: '{attachment_filename}'")
            return [attachment_filename]

        logger.warning("[req_id=%s] [%s] Unsupported attachment type: '%s'", rid, _TAG, attachment_filename)
        print(f"[{_TAG}] [req_id={rid}] Unsupported attachment type: '{attachment_filename}'")
        return []
