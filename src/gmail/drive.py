"""
Link extraction helpers for Google Drive, KFintech, and CAMS delivery URLs.

Google Drive — two share-link formats:
  - https://drive.google.com/file/d/<id>/view
  - https://drive.google.com/open?id=<id>

KFintech — pre-signed delivery URL embedded as an href in the HTML body:
  - https://scdelivery.kfintech.com/<path>
  No session/auth needed — direct HTTPS GET with browser headers works.

CAMS — two extraction strategies (matching reference Go architecture):
  1. Primary:  <td>DownloadURL</td> pattern — href in the adjacent <td>
  2. Fallback: regex for https://mailback{N}.camsonline.com/mailback_result/...
  No session/auth needed — pre-signed URL, direct download.
"""

import re

# ---------------------------------------------------------------------------
# Google Drive
# ---------------------------------------------------------------------------

_DRIVE_PATTERNS = [
    re.compile(r"https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)"),
]


def extract_drive_file_ids(text: str) -> list[str]:
    """Return all unique Drive file IDs found in *text*."""
    seen: set[str] = set()
    ids: list[str] = []
    for pattern in _DRIVE_PATTERNS:
        for match in pattern.finditer(text):
            fid = match.group(1)
            if fid not in seen:
                seen.add(fid)
                ids.append(fid)
    return ids


# ---------------------------------------------------------------------------
# KFintech delivery URL
# ---------------------------------------------------------------------------

# Matches href="https://scdelivery.kfintech.com/..." in HTML email bodies.
_KFINTECH_HREF = re.compile(
    r'href=["\']?(https://scdelivery\.kfintech\.com/[^"\'>\s]+)'
)


# ---------------------------------------------------------------------------
# CAMS delivery URL
# ---------------------------------------------------------------------------

# Primary: href containing a camsonline.com URL (in HTML body)
_CAMS_HREF = re.compile(
    r'href=["\']?(https://[^"\'>\s]*camsonline\.com[^"\'>\s]*)',
    re.IGNORECASE,
)
# Fallback: bare URL in decoded text (mailback servers)
_CAMS_MAILBACK = re.compile(
    r'https://mailback\d+\.camsonline\.com/mailback_result/[^\s\'"<>]+',
    re.IGNORECASE,
)


def extract_cams_download_url(text: str) -> str | None:
    """
    Return the first CAMS download URL found in *text*.

    Tries href attribute first (HTML emails), then bare URL fallback.
    Returns None if no URL is present.
    """
    m = _CAMS_HREF.search(text)
    if m:
        return m.group(1)
    m = _CAMS_MAILBACK.search(text)
    return m.group(0) if m else None


def extract_kfintech_download_url(text: str) -> str | None:
    """
    Return the first scdelivery.kfintech.com download URL found in *text*.

    KFintech embeds a pre-signed URL as an HTML href — no login required.
    Returns None if no such URL is present.
    """
    match = _KFINTECH_HREF.search(text)
    return match.group(1) if match else None
