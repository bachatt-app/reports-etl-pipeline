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
# Auth mode priority (first true wins):
#   1. USE_SERVICE_ACCOUNT=true  → service-account key file (local dev, no expiry)
#   2. USE_SECRET_MANAGER=true   → GCP Secret Manager OAuth (production)
#   3. else                      → local service-account.json + token.json (legacy dev)
# ---------------------------------------------------------------------------
USE_SERVICE_ACCOUNT = os.getenv("USE_SERVICE_ACCOUNT", "false").lower() == "true"
USE_SECRET_MANAGER = os.getenv("USE_SECRET_MANAGER", "false").lower() == "true"

# -- Service account (used when USE_SERVICE_ACCOUNT=true) -------------------
# Requires Domain-Wide Delegation enabled for the service account in
# Google Workspace Admin → Security → API Controls.
GMAIL_SERVICE_ACCOUNT_FILE = os.getenv("GMAIL_SERVICE_ACCOUNT_FILE", "service-account.json")

# -- Secret Manager paths (used when USE_SECRET_MANAGER=true) ---------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GMAIL_CREDENTIALS_SECRET = os.getenv("GMAIL_CREDENTIALS_SECRET", "gmail-client-credentials")
GMAIL_REFRESH_TOKEN_SECRET = os.getenv("GMAIL_REFRESH_TOKEN_SECRET", "gmail-refresh-token")

# -- Local file paths (used when USE_SECRET_MANAGER=false) ------------------
GMAIL_CREDENTIALS_FILE = os.getenv("GMAIL_CREDENTIALS_FILE", "service-account.json")
GMAIL_TOKEN_FILE = os.getenv("GMAIL_TOKEN_FILE", "token.json")

# ---------------------------------------------------------------------------
# Gmail query date filter
# Number of calendar days to look back from today.
# Converted to a Gmail "after:YYYY/MM/DD" filter at runtime.
# ---------------------------------------------------------------------------
GMAIL_LOOKBACK_DAYS: int = int(os.getenv("GMAIL_LOOKBACK_DAYS", "30"))

# ---------------------------------------------------------------------------
# Provider sender-email filters
# Comma-separated list of allowed sender addresses.
# Use "*" as one of the values to allow emails from any sender.
# ---------------------------------------------------------------------------
_ALLOWED_SENDERS_RAW = os.getenv(
    "ALLOWED_SENDERS",
    "donotreply@camsonline.com,distributorcare@kfintech.com,secretarial@bachatt.app,bachattapp@gmail.com",
)
ALLOWED_SENDERS: list[str] = [s.strip() for s in _ALLOWED_SENDERS_RAW.split(",") if s.strip()]
ALLOW_ALL_SENDERS: bool = "*" in ALLOWED_SENDERS

# ---------------------------------------------------------------------------
# Zip passwords (per provider, optional)
# ---------------------------------------------------------------------------
CAMS_ZIP_PASSWORD: str | None = os.getenv("CAMS_ZIP_PASSWORD")
KFINTECH_ZIP_PASSWORD: str | None = os.getenv("KFINTECH_ZIP_PASSWORD")
# R9 automated (ClientWise AUM Report, MFSD310) uses a different password
KFINTECH_R9_AUTO_ZIP_PASSWORD: str | None = os.getenv("KFINTECH_R9_AUTO_ZIP_PASSWORD", "Bachatt2025")


# ---------------------------------------------------------------------------
# GCS buckets (upload destination for extracted CSVs)
# ---------------------------------------------------------------------------
GCS_CAMS_BUCKET: str = os.getenv("GCS_CAMS_BUCKET", "")
GCS_CAMS_PREFIX: str = os.getenv("GCS_CAMS_PREFIX", "r2_cams")
GCS_KFINTECH_BUCKET: str = os.getenv("GCS_KFINTECH_BUCKET", "")
GCS_KFINTECH_PREFIX: str = os.getenv("GCS_KFINTECH_PREFIX", "r2_kfintech")
GCS_RAZORPAY_BUCKET: str = os.getenv("GCS_RAZORPAY_BUCKET", "")
GCS_NAV_BUCKET: str = os.getenv("GCS_NAV_BUCKET", "")
GCS_BSE_BUCKET: str = os.getenv("GCS_BSE_BUCKET", "")

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
BQ_PROJECT_ID: str = os.getenv("BQ_PROJECT_ID", GCP_PROJECT_ID)
BQ_DATASET: str = os.getenv("BQ_DATASET", "cams_data")

# ---------------------------------------------------------------------------
# Alert emails
# ---------------------------------------------------------------------------
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")          # comma-separated recipients
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "me")     # Gmail userId (usually "me")
