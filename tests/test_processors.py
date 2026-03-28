"""
Unit tests — no Gmail / GCP credentials required.

Run:
    pytest tests/test_processors.py -v
"""

import asyncio
import io
import pytest
import pyzipper

from src.logging_context import configure_logging, bind_request_id, new_request_id, reset_request_id
from src.processors.factory import FileProcessorFactory, Provider
from src.gmail.drive import extract_drive_file_ids

configure_logging()

# ── Alert capture fixture ─────────────────────────────────────────────────────

@pytest.fixture
def alerts():
    fired = []
    async def callback(subject: str, body: str):
        fired.append(subject)
    return fired, callback


def make(provider, callback=None, **kw):
    p = FileProcessorFactory.get_processor(provider, alert_callback=callback)
    for k, v in kw.items():
        setattr(p, k, v)
    return p


# ── Helpers ───────────────────────────────────────────────────────────────────

def _plain_zip(*members: tuple[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with pyzipper.AESZipFile(buf, "w") as zf:
        for name, data in members:
            zf.writestr(name, data)
    return buf.getvalue()


def _aes_zip(password: str, *members: tuple[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with pyzipper.AESZipFile(buf, "w",
                             compression=pyzipper.ZIP_DEFLATED,
                             encryption=pyzipper.WZ_AES) as zf:
        zf.setpassword(password.encode())
        for name, data in members:
            zf.writestr(name, data)
    return buf.getvalue()


# ── CAMS tests ────────────────────────────────────────────────────────────────

class TestCamsProcessor:

    @pytest.mark.asyncio
    async def test_direct_csv(self):
        r = await make(Provider.CAMS).extract(b"a,b\n1,2", "report.csv")
        assert r == ["report.csv"]

    @pytest.mark.asyncio
    async def test_direct_dbf(self):
        r = await make(Provider.CAMS).extract(b"\x03", "report.dbf")
        assert r == ["report.dbf"]

    @pytest.mark.asyncio
    async def test_plain_zip(self):
        data = _plain_zip(("cams.csv", b"x,y"), ("ignore.txt", b"skip"))
        r = await make(Provider.CAMS).extract(data, "cams.zip")
        assert r == ["cams.csv"]

    @pytest.mark.asyncio
    async def test_aes_zip_correct_password(self):
        data = _aes_zip("cams_secret", ("data.csv", b"a,b"))
        r = await make(Provider.CAMS, zip_password="cams_secret").extract(data, "secure.zip")
        assert r == ["data.csv"]

    @pytest.mark.asyncio
    async def test_aes_zip_wrong_password_fires_alert_returns_empty(self, alerts):
        fired, cb = alerts
        data = _aes_zip("cams_secret", ("data.csv", b"a,b"))
        r = await make(Provider.CAMS, cb, zip_password="wrongpass").extract(data, "secure.zip")
        assert r == []
        assert len(fired) == 1
        assert "password" in fired[0].lower()

    @pytest.mark.asyncio
    async def test_corrupt_zip_fires_alert_returns_empty(self, alerts):
        fired, cb = alerts
        r = await make(Provider.CAMS, cb).extract(b"not-a-zip", "bad.zip")
        assert r == []
        assert len(fired) == 1
        assert "corrupt" in fired[0].lower()

    @pytest.mark.asyncio
    async def test_empty_zip_fires_alert_returns_empty(self, alerts):
        fired, cb = alerts
        data = _plain_zip(("readme.txt", b"nothing"))
        r = await make(Provider.CAMS, cb).extract(data, "empty.zip")
        assert r == []
        assert len(fired) == 1

    @pytest.mark.asyncio
    async def test_unsupported_type_returns_empty(self):
        r = await make(Provider.CAMS).extract(b"<html/>", "report.html")
        assert r == []


# ── KFintech tests ────────────────────────────────────────────────────────────

class TestKFintechProcessor:

    @pytest.mark.asyncio
    async def test_direct_dbf(self):
        r = await make(Provider.KFINTECH).extract(b"\x03", "kf.dbf")
        assert r == ["kf.dbf"]

    @pytest.mark.asyncio
    async def test_direct_csv(self):
        r = await make(Provider.KFINTECH).extract(b"a,b", "kf.csv")
        assert r == ["kf.csv"]

    @pytest.mark.asyncio
    async def test_zip_r1_r2_password(self):
        """Simulates real KFintech R1/R2 report zip (password Bachatt@2025)."""
        data = _aes_zip("Bachatt@2025", ("R1report.dbf", b"\x03"), ("R2report.csv", b"x,y"))
        r = await make(Provider.KFINTECH, zip_password="Bachatt@2025").extract(data, "kf_r1r2.zip")
        assert set(r) == {"R1report.dbf", "R2report.csv"}

    @pytest.mark.asyncio
    async def test_zip_r9_password(self):
        """Simulates real KFintech R9 report zip (password Bachatt2025, no @)."""
        data = _aes_zip("Bachatt2025", ("R9report.dbf", b"\x03"))
        r = await make(Provider.KFINTECH, zip_password="Bachatt2025").extract(data, "kf_r9.zip")
        assert r == ["R9report.dbf"]

    @pytest.mark.asyncio
    async def test_wrong_password_fires_alert(self, alerts):
        fired, cb = alerts
        data = _aes_zip("Bachatt@2025", ("kf.dbf", b"\x03"))
        r = await make(Provider.KFINTECH, cb, zip_password="wrong").extract(data, "kf.zip")
        assert r == []
        assert any("password" in a.lower() for a in fired)

    @pytest.mark.asyncio
    async def test_corrupt_zip_fires_alert(self, alerts):
        fired, cb = alerts
        r = await make(Provider.KFINTECH, cb).extract(b"\xff\xfe garbage", "kf.zip")
        assert r == []
        assert any("corrupt" in a.lower() for a in fired)


# ── Drive URL extraction ──────────────────────────────────────────────────────

class TestDriveExtraction:

    def test_file_url(self):
        ids = extract_drive_file_ids("https://drive.google.com/file/d/ABC123/view")
        assert ids == ["ABC123"]

    def test_open_url(self):
        ids = extract_drive_file_ids("https://drive.google.com/open?id=XYZ456")
        assert ids == ["XYZ456"]

    def test_deduplication(self):
        text = (
            "https://drive.google.com/file/d/ABC123/view "
            "https://drive.google.com/open?id=XYZ456 "
            "https://drive.google.com/file/d/ABC123/view"   # duplicate
        )
        ids = extract_drive_file_ids(text)
        assert ids == ["ABC123", "XYZ456"]

    def test_no_urls(self):
        assert extract_drive_file_ids("plain text with no links") == []

    def test_mixed_content(self):
        text = "See report at https://drive.google.com/file/d/REPORT1/view and also http://example.com"
        assert extract_drive_file_ids(text) == ["REPORT1"]


# ── Factory ───────────────────────────────────────────────────────────────────

class TestFactory:

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError):
            FileProcessorFactory.get_processor("unknown_provider")

    def test_zip_password_injected_from_env(self, monkeypatch):
        monkeypatch.setenv("KFINTECH_ZIP_PASSWORD", "testpass")
        # Re-import settings to pick up monkeypatched env
        import importlib, src.config.settings as s
        importlib.reload(s)
        import src.processors.factory as f
        importlib.reload(f)
        from src.processors.factory import FileProcessorFactory as F, Provider as P
        p = F.get_processor(P.KFINTECH)
        assert p.zip_password == "testpass"

    @pytest.mark.asyncio
    async def test_concurrent_providers(self):
        results = await asyncio.gather(
            make(Provider.CAMS).extract(b"a,b", "c.csv"),
            make(Provider.KFINTECH).extract(b"\x03", "k.dbf"),
        )
        assert results == [["c.csv"], ["k.dbf"]]
