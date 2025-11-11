# publisher.py
import os
import time
import logging
from typing import Dict, Tuple, Optional, List, Union

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
    if WP_CATEGORY_ID:
        payload["categories"] = [WP_CATEGORY_ID]


def _dedupe_key(item: Dict) -> Optional[str]:
    return item.get("afm_key") or item.get("unique_id")


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
    Expects a list of *source records* (not yet article payloads).
    """
    created = 0
    for item in items:
        if created >= MAX_POSTS_PER_RUN:
            break

        payload = build_article(item, category_id=WP_CATEGORY_ID)
        payload["status"] = WP_PUBLISH_STATUS

        _ensure_list_category(payload)

        afm_key = _dedupe_key(item)
        if afm_key:
            payload.setdefault("meta", {})
            payload["meta"]["afm_key"] = afm_key

        try:
            _post_to_wordpress(payload)
            created += 1
            time.sleep(0.3)
        except Exception as e:
            logger.error("Failed to publish item (afm_key=%s): %s", afm_key, e)

    logger.info("Published %d items.", created)
    return created


# ---- Backward-compat entrypoint used by main.py ----
def publish_to_wordpress(arg: Union[Dict, List[Dict]]) -> int:
    """
    Accepts EITHER:
      - a single already-built article payload (dict) -> posts it immediately
      - a list of source records (list[dict]) -> delegates to publish_items()
    Returns number of posts published.
    """
    # Case 1: single article payload
    if isinstance(arg, dict):
        payload = dict(arg)  # shallow copy
        payload.setdefault("status", WP_PUBLISH_STATUS)
        _ensure_list_category(payload)
        try:
            _post_to_wordpress(payload)
            logger.info("Published 1 item (single payload).")
            return 1
        except Exception as e:
            logger.error("Failed to publish single payload: %s", e)
            return 0

    # Case 2: list of source records
    if isinstance(arg, list):
        return publish_items(arg)

    raise TypeError("publish_to_wordpress expects a dict (single payload) or list[dict] (records).")
