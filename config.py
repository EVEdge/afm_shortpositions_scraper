import os

WP_BASE_URL = os.getenv("WP_BASE_URL", "https://dekoersen.nl")
WP_USERNAME = os.getenv("WP_USERNAME", "automation")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD")

AFM_URL = os.getenv(
    "AFM_URL",
    "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel",
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///afm.db")
CATEGORY_ID = int(os.getenv("WP_CATEGORY_ID", "777"))
PUBLISH_STATUS = os.getenv("WP_PUBLISH_STATUS", "draft")
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "10"))

# Daily overview. On every run the newest AFM position date is inspected.
# One article is kept per AFM position date and is only rewritten when the
# underlying set of filings changes.
DAILY_OVERVIEW_ENABLED = os.getenv("DAILY_OVERVIEW_ENABLED", "1").strip().lower() not in {
    "0", "false", "no", "off"
}

# Weekly editorial ranking. The article is refreshed once per week on Sunday
# during the 08:00 hour in Europe/Amsterdam. The editorial format is fixed at 10.
WEEKLY_RANKING_ENABLED = os.getenv("WEEKLY_RANKING_ENABLED", "1").strip().lower() not in {
    "0", "false", "no", "off"
}
WEEKLY_RANKING_TOP_N = 10

# Verified internal-link enrichment for all short-position articles.
INTERNAL_LINKS_ENABLED = os.getenv("INTERNAL_LINKS_ENABLED", "1").strip().lower() not in {
    "0", "false", "no", "off"
}
INTERNAL_LINK_TIMEOUT = float(os.getenv("INTERNAL_LINK_TIMEOUT", "8"))
