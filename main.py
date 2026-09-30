# main.py

import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from afm_scraper import fetch_afm_table
from article_builder import build_article

from config import (
    DAILY_OVERVIEW_ENABLED,
    MAX_POSTS_PER_RUN,
    WEEKLY_RANKING_ENABLED,
    WEEKLY_RANKING_TOP_N,
    WP_APP_PASSWORD,
    WP_BASE_URL,
    WP_USERNAME,
)

from daily_overview import (
    build_daily_article,
    daily_fingerprint,
    daily_slug,
    latest_report_date,
    select_daily_items,
)

from db import (
    get_daily_overview_fingerprint,
    get_short_ranking_snapshot,
    init_db,
    save_daily_overview_state,
    save_short_ranking_snapshot,
    short_ranking_snapshot_exists,
)

from publisher import (
    publish_to_wordpress,
    upsert_post_by_slug,
)

from weekly_ranking import (
    WEEKLY_ARTICLE_SLUG,
    aggregate_current_positions,
    attach_previous_snapshot,
    build_weekly_article,
    is_sunday_update_window,
    previous_week_uid,
    weekly_uid,
)


logger = logging.getLogger(__name__)

AMSTERDAM_TZ = ZoneInfo(
    "Europe/Amsterdam"
)


def assert_wp_env():

    missing = [
        key
        for key, value in {
            "WP_BASE_URL": WP_BASE_URL,
            "WP_USERNAME": WP_USERNAME,
            "WP_APP_PASSWORD": WP_APP_PASSWORD,
        }.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing WordPress credentials: "
            f"{', '.join(missing)}"
        )


def process_daily_overview(
    scraped,
    now=None,
):
    """
    Create/update one overview for the
    newest AFM position date.
    """

    if not DAILY_OVERVIEW_ENABLED:
        logger.info(
            "Daily short overview is disabled."
        )

        return 0

    now = (
        now
        or datetime.now(
            AMSTERDAM_TZ
        )
    )

    report_date = latest_report_date(
        scraped,
        now=now,
    )

    if not report_date:
        logger.warning(
            "Daily short overview skipped: "
            "no valid AFM position date found."
        )

        return 0

    daily_items = select_daily_items(
        scraped,
        report_date,
    )

    if not daily_items:
        logger.info(
            "Daily short overview skipped: "
            "no filings for %s.",
            report_date,
        )

        return 0

    fingerprint = daily_fingerprint(
        daily_items,
        report_date,
    )

    previous_fingerprint = (
        get_daily_overview_fingerprint(
            report_date
        )
    )

    if (
        previous_fingerprint
        == fingerprint
    ):
        logger.info(
            "Daily AFM overview for %s "
            "is unchanged; skipping "
            "WordPress update.",
            report_date,
        )

        return 0

    article = build_daily_article(
        daily_items,
        report_date,
        now=now,
    )

    slug = daily_slug(
        report_date
    )

    updated = upsert_post_by_slug(
        article,
        slug,
    )

    if updated:

        save_daily_overview_state(
            report_date,
            fingerprint,
            slug,
        )

        logger.info(
            "Created/updated daily AFM "
            "short overview for %s with "
            "%d filings.",
            report_date,
            len(daily_items),
        )

    else:
        logger.warning(
            "Daily AFM short overview "
            "was not published for %s.",
            report_date,
        )

    return updated


def process_weekly_ranking(
    scraped,
    now=None,
):
    """
    Refresh the fixed Top 10 article
    once on Sunday during the 08:00 hour.
    """

    if not WEEKLY_RANKING_ENABLED:
        logger.info(
            "Weekly short ranking "
            "is disabled."
        )

        return 0

    now = (
        now
        or datetime.now(
            AMSTERDAM_TZ
        )
    )

    if not is_sunday_update_window(
        now
    ):
        logger.info(
            "Weekly short ranking skipped: "
            "outside Sunday 08:00 "
            "update window."
        )

        return 0

    current_uid = weekly_uid(
        now
    )

    if short_ranking_snapshot_exists(
        current_uid
    ):
        logger.info(
            "Weekly AFM ranking already "
            "updated for %s; skipping.",
            current_uid,
        )

        return 0

    ranking = aggregate_current_positions(
        scraped
    )

    if not ranking:
        logger.warning(
            "Weekly short ranking skipped: "
            "no active >=0.50%% positions found."
        )

        return 0

    previous = get_short_ranking_snapshot(
        previous_week_uid(
            now
        )
    )

    ranking = attach_previous_snapshot(
        ranking,
        previous,
    )

    article = build_weekly_article(
        ranking,
        now=now,
        top_n=WEEKLY_RANKING_TOP_N,
    )

    updated = upsert_post_by_slug(
        article,
        WEEKLY_ARTICLE_SLUG,
    )

    if updated:

        save_short_ranking_snapshot(
            current_uid,
            now.date().isoformat(),
            ranking,
        )

        logger.info(
            "Updated weekly AFM "
            "short ranking for %s.",
            current_uid,
        )

    else:
        logger.warning(
            "Weekly AFM ranking was "
            "not updated for %s.",
            current_uid,
        )

    return updated


def process_new_entries():

    assert_wp_env()

    init_db()

    # Fetch the AFM source once so the daily overview,
    # weekly ranking and individual articles all use
    # exactly the same source state.
    scraped = fetch_afm_table()

    # Daily overview:
    # one article per newest AFM position date.
    # It is only rewritten if new/changed filings
    # for that same date appear later.
    process_daily_overview(
        scraped
    )

    # Weekly Top 10:
    # fixed article, refreshed Sunday
    # during the 08:00 hour.
    process_weekly_ranking(
        scraped
    )

    # Existing individual short-position
    # articles remain unchanged.
    posted = 0

    for record in scraped:

        if posted >= MAX_POSTS_PER_RUN:
            break

        article = build_article(
            record
        )

        posted += publish_to_wordpress(
            article
        )

    print(
        f"Processed {posted} new AFM entries "
        f"(max {MAX_POSTS_PER_RUN})."
    )


if __name__ == "__main__":
    process_new_entries()
