# main.py
from afm_scraper import fetch_afm_table
from article_builder import build_article
from publisher import publish_to_wordpress
from config import MAX_POSTS_PER_RUN, WP_BASE_URL, WP_USERNAME, WP_APP_PASSWORD

import os
import logging

def assert_wp_env():
    missing = [k for k, v in {
        "WP_BASE_URL": WP_BASE_URL,
        "WP_USERNAME": WP_USERNAME,
        "WP_APP_PASSWORD": WP_APP_PASSWORD
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing WordPress credentials: {', '.join(missing)}")

def process_new_entries():
    assert_wp_env()

    scraped = fetch_afm_table()
    posted = 0

    for record in scraped:
        if posted >= MAX_POSTS_PER_RUN:
            break

        # Publish everything (no skip conditions)
        article = build_article(record)  # ← removed prev_from_db kwarg
        publish_to_wordpress(article)
        posted += 1

    print(f"Processed {posted} new AFM entries (max {MAX_POSTS_PER_RUN}).")

    # Transitional import bridge: reuses this already-running Pennywatch
    # shorts cron and its existing WordPress credentials. No new cron,
    # database or AFM fetch; the central Stockz publisher takes over later.
    if os.getenv("PW_AFM_PLUGIN_SYNC_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }:
        from afm_scraper import fetch_full_history_for_plugin
        from shortcode_sync import sync_records_to_wordpress

        history = fetch_full_history_for_plugin()
        if not history:
            raise RuntimeError("Pennywatch AFM plugin import refused: empty AFM history.")

        imported = sync_records_to_wordpress(
            history,
            base_url=WP_BASE_URL,
            username=WP_USERNAME,
            password=WP_APP_PASSWORD,
            endpoint_namespace="pw-afm",
            stock_resolver=lambda _issuer, _isin: None,
            holder_resolver=lambda _holder: None,
        )
        logging.info(
            "PW AFM historical plugin sync successful: source=%d transmitted=%d",
            len(history),
            imported,
        )

if __name__ == "__main__":
    process_new_entries()
