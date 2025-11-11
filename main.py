import re
from afm_scraper import fetch_afm_table
from article_builder import build_article
from publisher import publish_to_wordpress
from config import MAX_POSTS_PER_RUN, WP_BASE_URL, WP_USERNAME, WP_APP_PASSWORD

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
        article = build_article(record, prev_from_db=None)
        publish_to_wordpress(article)
        posted += 1

    print(f"Processed {posted} new AFM entries (max {MAX_POSTS_PER_RUN}).")

if __name__ == "__main__":
    process_new_entries()
