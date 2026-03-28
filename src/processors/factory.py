from enum import Enum

from src.config.settings import CAMS_ZIP_PASSWORD, KFINTECH_ZIP_PASSWORD
from src.processors.base import AlertCallback, FileProcessorStrategy
from src.processors.cams_processor import CamsProcessor
from src.processors.kfintech_processor import KFintechProcessor


class Provider(str, Enum):
    CAMS = "cams"
    KFINTECH = "kfintech"


class FileProcessorFactory:
    """
    Factory that returns a configured FileProcessorStrategy for a given provider.

    Each processor is wired with:
    - zip_password  — per-provider decryption key (from settings / env)
    - alert_callback — coroutine to call on failure events
    """

    _registry: dict[Provider, type[FileProcessorStrategy]] = {
        Provider.CAMS: CamsProcessor,
        Provider.KFINTECH: KFintechProcessor,
    }

    _zip_passwords: dict[Provider, str | None] = {
        Provider.CAMS: CAMS_ZIP_PASSWORD,
        Provider.KFINTECH: KFINTECH_ZIP_PASSWORD,
    }

    @classmethod
    def get_processor(
        cls,
        provider: "Provider | str",
        alert_callback: AlertCallback | None = None,
    ) -> FileProcessorStrategy:
        key = Provider(provider) if isinstance(provider, str) else provider
        processor_cls = cls._registry.get(key)
        if processor_cls is None:
            raise ValueError(f"No processor registered for provider '{provider}'")

        processor = processor_cls()
        processor.zip_password = cls._zip_passwords.get(key)
        processor.alert_callback = alert_callback
        return processor

    @classmethod
    def register(cls, provider: Provider, processor_cls: type[FileProcessorStrategy]) -> None:
        """Register an additional provider at runtime."""
        cls._registry[provider] = processor_cls
