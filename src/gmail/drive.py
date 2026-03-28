"""
Drive link extraction helpers.

Supports both share-link formats emitted by Google Drive:
  - https://drive.google.com/file/d/<id>/view
  - https://drive.google.com/open?id=<id>
"""

import re

_PATTERNS = [
    re.compile(r"https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)"),
]


def extract_drive_file_ids(text: str) -> list[str]:
    """Return all unique Drive file IDs found in *text*."""
    seen: set[str] = set()
    ids: list[str] = []
    for pattern in _PATTERNS:
        for match in pattern.finditer(text):
            fid = match.group(1)
            if fid not in seen:
                seen.add(fid)
                ids.append(fid)
    return ids
