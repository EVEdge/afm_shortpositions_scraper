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

def _is_unknown(text: str | None) -> bool:
    if not text:
        return True
    t = text.strip().lower()
    return t.startswith("onbekende") or t in {"", "n.n.b.", "onbekend"}

_pct_re = re.compile(r"\d+(?:[.,]\d+)?%")

def _has_valid_pct(s: str | None) -> bool:
    """True als er een echt percentage in staat, anders False (n.n.b., leeg, etc.)."""
    if not s:
        return False
    return bool(_pct_re.search(str(s)))

def process_new_entries():
    assert_wp_env()

    scraped = fetch_afm_table()
    posted = 0

    for record in scraped:
        if posted >= MAX_POSTS_PER_RUN:
            break

        # 1) Skip onbekende melder/emittent
        if _is_unknown(record.get("melder")) or _is_unknown(record.get("emittent")):
            print(f"[SKIP] Unknown melder/emittent for afm_key={record.get('afm_key')}")
            continue

        # 2) Skip als kapitaalbelang niet bekend/valide is -> anders komt er 'n.n.b.' in de titel/tekst
        kap = record.get("kapitaal_pct")
        if not _has_valid_pct(kap):
            print(f"[SKIP] No valid kapitaalbelang (would be n.n.b.) for afm_key={record.get('afm_key')}")
            continue

        # (Optioneel) óók stemrechten verplichten:
        # stem = record.get("stem_pct")
        # if not _has_valid_pct(stem):
        #     print(f"[SKIP] No valid stemrechten (would be n.n.b.) for afm_key={record.get('afm_key')}")
        #     continue

        article = build_article(record, prev_from_db=None)
        publish_to_wordpress(article)
        posted += 1

    print(f"Processed {posted} new AFM entries (max {MAX_POSTS_PER_RUN}).")

if __name__ == "__main__":
    process_new_entries()
