import os
import json
import time
import logging
from typing import Dict, Optional, Tuple

import requests

from article_builder import build_article

WP_BASE_URL        = os.getenv("WP_BASE_URL", "").rstrip("/")
WP_USERNAME        = os.getenv("WP_USERNAME")
WP_APP_PASSWORD    = os.getenv("WP_APP_PASSWORD")
WP_CATEGORY_ID     = int(os.getenv("WP_CATEGORY_ID", "0") or 0)
WP_PUBLISH_STATUS  = os.getenv("WP_PUBLISH_STATUS", "publish")
MAX_POSTS_PER_RUN  = int(os.getenv("MAX_POSTS_PER_RUN", "10"))

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(os.getenv("PW_LOG_LEVEL", "INFO").upper())


def _auth() -> Tuple[str, str]:
    if not WP_USERNAME or not WP_APP_PASSWORD:
        raise RuntimeError("Missing WP credentials (WP_USERNAME / WP_APP_PASSWORD).")
    return WP_USERNAME, WP_APP_PASSWORD


def _posts_url() -> str:
    if not WP_BASE_URL:
        raise RuntimeError("Missing WP_BASE_URL.")
    return f"{WP_BASE_URL}/wp-json/wp/v2/posts"


def _ensure_list_category(payload: Dict) -> None:
    # WordPress expects a LIST of IDs.
    if WP_CATEGORY_ID:
        payload["categories"] = [WP_CATEGORY_ID]


def _derive_short_fields(item: Dict) -> Tuple[float, str]:
    """
    Permanent rule: if this is a short-position item (or looks like one),
    take the percentage from the short-position fields.
    Returns (numeric_pct, pretty_pct_str).
    """
    # Detect short-position items robustly
    is_short = (
        item.get("meldingstype") == "shortpositie"
        or "net_short_pct" in item
        or "net_short_pct_num" in item
        or "Netto Shortpositie" in item  # raw CSV key
    )

    if is_short:
        num = item.get("net_short_pct_num")
        if num is None:
            # try to parse string field
            s = str(item.get("net_short_pct") or item.get("Netto Shortpositie") or "").replace("%", "").replace(",", ".")
            try:
                num = float(s)
            except Exception:
                num = 0.0
        pretty = item.get("net_short_pct") or f"{num:.2f}%"
        # normalize back into the item so the rest of the pipeline can reuse it
        item["kapitaalbelang"] = num
        item["kapitaalbelang_str"] = pretty
        item["meldingstype"] = "shortpositie"
        return num, pretty

    # Fallback for legacy “meldingen”
    if "kapitaalbelang" in item or "kapitaalbelang_str" in item:
        try:
            num = float(str(item.get("kapitaalbelang")).replace(",", "."))
        except Exception:
            num = 0.0
        pretty = item.get("kapitaalbelang_str") or (f"{num:.2f}%" if num else "n.n.b.")
        return num, pretty

    return 0.0, "n.n.b."


def _dedupe_key(item: Dict) -> Optional[str]:
    # Stable key for DB/publisher dedupe
    return item.get("afm_key") or item.get("unique_id")


def _should_skip(item: Dict) -> Optional[str]:
    """
    Legacy skip rule modified:
    - NEVER skip if this is a short-position item (we’ll derive kapitaalbelang from net_short_pct[_num]).
    - For non-short items keep the old rule: skip only if kapitaalbelang still ends up n.n.b. / 0.
    """
    # This call also populates kapitaalbelang fields for short items
    num, pretty = _derive_short_fields(item)

    if item.get("meldingstype") == "shortpositie":
        # Do not skip short-positions
        return None

    # Legacy case (meldingen)
    if not num and (pretty.lower() == "n.n.b." or pretty == "" or pretty == "0%"):
        return f"No valid kapitaalbelang (would be n.n.b.) for afm_key={_dedupe_key(item)}"

    return None


def _resolve_tags_to_ids_if_needed(payload: Dict) -> None:
    """
    If your WP requires tag IDs, resolve/ensure them here.
    Current implementation sends tag names and lets WP auto-create/resolve.
    """
    # Intentionally left as no-op for simplicity/compatibility.
    return


def _post_to_wordpress(payload: Dict) -> Dict:
    url = _posts_url()
    auth = _auth()

    resp = requests.post(
        url,
        auth=auth,
        json=payload,
        timeout=45,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"WP POST failed [{resp.status_code}]: {resp.text[:500]}")
    return resp.json()


def publish_items(items: list[Dict]) -> int:
    """
    Publishes up to MAX_POSTS_PER_RUN items.
    Returns the number of posts created.
    """
    created = 0

    for item in items:
        if created >= MAX_POSTS_PER_RUN:
            break

        skip_reason = _should_skip(item)
        if skip_reason:
            logger.info("[SKIP] %s", skip_reason)
            continue

        payload = build_article(item, category_id=WP_CATEGORY_ID)
        payload["status"] = WP_PUBLISH_STATUS

        _ensure_list_category(payload)
        _resolve_tags_to_ids_if_needed(payload)

        # Optional: ensure dedupe key is present in meta
        afm_key = _dedupe_key(item)
        if afm_key:
            payload.setdefault("meta", {})
            payload["meta"]["afm_key"] = afm_key

        try:
            _post_to_wordpress(payload)
            created += 1
            # small pause to be nice to WP
            time.sleep(0.4)
        except Exception as e:
            logger.error("Failed to publish item (afm_key=%s): %s", afm_key, e)

    return created
