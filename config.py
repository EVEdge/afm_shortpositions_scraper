import os

WP_BASE_URL = os.getenv("WP_BASE_URL", "https://pennywatch.nl")
WP_USERNAME  = os.getenv("WP_USERNAME", "automation")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD")

AFM_URL = os.getenv(
    "AFM_URL",
    "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/substantiele-deelnemingen",
)

DATABASE_URL   = os.getenv("DATABASE_URL", "sqlite:///afm.db")
CATEGORY_ID    = int(os.getenv("WP_CATEGORY_ID", "775"))
PUBLISH_STATUS = os.getenv("WP_PUBLISH_STATUS", "publish")  # ← default to draft
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "10"))
