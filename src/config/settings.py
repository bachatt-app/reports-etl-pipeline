import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Gmail OAuth
# ---------------------------------------------------------------------------
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",   # read + send alerts
    "https://www.googleapis.com/auth/drive.readonly",  # download Drive files
]
GMAIL_USER = os.getenv("GMAIL_USER", "me")

# ---------------------------------------------------------------------------
# Auth mode: Secret Manager (production) vs local file (development)
# ---------------------------------------------------------------------------
USE_SECRET_MANAGER = os.getenv("USE_SECRET_MANAGER", "false").lower() == "true"

# -- Secret Manager paths (used when USE_SECRET_MANAGER=true) ---------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GMAIL_CREDENTIALS_SECRET = os.getenv("GMAIL_CREDENTIALS_SECRET", "gmail-client-credentials")
GMAIL_REFRESH_TOKEN_SECRET = os.getenv("GMAIL_REFRESH_TOKEN_SECRET", "gmail-refresh-token")

# -- Local file paths (used when USE_SECRET_MANAGER=false) ------------------
GMAIL_CREDENTIALS_FILE = os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")
GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", "token.json")

# ---------------------------------------------------------------------------
# Provider sender-email filters
# ---------------------------------------------------------------------------
CAMS_SENDER_EMAIL = os.getenv("CAMS_SENDER_EMAIL", "reports@camsonline.com")
KFINTECH_SENDER_EMAIL = os.getenv("KFINTECH_SENDER_EMAIL", "reports@kfintech.com")

# ---------------------------------------------------------------------------
# Zip passwords (per provider, optional)
# ---------------------------------------------------------------------------
CAMS_ZIP_PASSWORD: str | None = os.getenv("CAMS_ZIP_PASSWORD")
KFINTECH_ZIP_PASSWORD: str | None = os.getenv("KFINTECH_ZIP_PASSWORD")

# ---------------------------------------------------------------------------
# Alert emails
# ---------------------------------------------------------------------------
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")          # comma-separated recipients
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "me")     # Gmail userId (usually "me")
