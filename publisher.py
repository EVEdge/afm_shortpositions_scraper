import os
import time
import logging
from typing import Dict, Tuple, Optional, List

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
        raise RuntimeError("Missing WP_USERNAME / WP_APP_PASSWORD.")
    return WP_USERNAME, WP_APP_PASSWORD


def _posts_url() -> str:
    if not WP_BASE_URL:
        raise RuntimeError("Missing WP_BASE_URL.")
    return f"{WP_BASE_URL}/wp-json/wp/v2/posts"


def _ensure_list_category(payload: Dict) -> None:
    # WordPress expects a LIST of IDs.
    if WP_CATEGORY_ID:
        payload["categories"] = [WP_CATEGORY_ID]


def _dedupe_key(item: Dict) -> Optional[str]:
    # Keep the key in meta for later if you want dedupe, but DO NOT skip on it.
    return item.get("afm_key") or item.get("unique_id")


def _resolve_tags_to_ids_if_needed(payload: Dict) -> None:
    # Send tag names; let WP create/resolve — keep as no-op unless you need IDs.
    return


def _post_to_wordpress(payload: Dict) -> Dict:
    resp = requests.post(
        _posts_url(),
        auth=_auth(),
        json=payload,
        timeout=45,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"WP POST failed [{resp.status_code}]: {resp.text[:500]}")
    return resp.json()


def publish_items(items: List[Dict]) -> int:
    """
    Publish EVERYTHING (no skip conditions), up to MAX_POSTS_PER_RUN.
    """
    created = 0

    for item in items:
        if created >= MAX_POSTS_PER_RUN:
            break

        # Build post payload
        payload = build_article(item, category_id=WP_CATEGORY_ID)
        payload["status"] = WP_PUBLISH_STATUS

        _ensure_list_category(payload)
        _resolve_tags_to_ids_if_needed(payload)

        # carry over a stable key if present (but don't use it to skip)
        afm_key = _dedupe_key(item)
        if afm_key:
            payload.setdefault("meta", {})
            payload["meta"]["afm_key"] = afm_key

        try:
            _post_to_wordpress(payload)
            created += 1
            time.sleep(0.3)  # be nice to WP
        except Exception as e:
            logger.error("Failed to publish item (afm_key=%s): %s", afm_key, e)

    logger.info("Published %d items.", created)
    return created


# ---- Backward-compat alias for main.py ----
def publish_to_wordpress(items: List[Dict]) -> int:
    return publish_items(items)
