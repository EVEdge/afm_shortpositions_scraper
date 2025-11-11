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

# ----------------- WP helpers -----------------

def _auth() -> Tuple[str, str]:
    if not WP_USERNAME or not WP_APP_PASSWORD:
        raise RuntimeError("Missing WP_USERNAME / WP_APP_PASSWORD.")
    return WP_USERNAME, WP_APP_PASSWORD

def _api(url_tail: str) -> str:
    if not WP_BASE_URL:
        raise RuntimeError("Missing WP_BASE_URL.")
    return f"{WP_BASE_URL}/wp-json/wp/v2/{url_tail.lstrip('/')}"

def _posts_url() -> str:
    return _api("posts")

def _tags_url() -> str:
    return _api("tags")

# cache tag name -> id per run
_TAG_CACHE: dict[str, int] = {}

def _get_or_create_tag_id(name: str) -> Optional[int]:
    """Resolve a tag name to an ID; create it if missing."""
    if not name:
        return None
    key = name.strip().lower()
    if key in _TAG_CACHE:
        return _TAG_CACHE[key]

    # 1) try search
    try:
        resp = requests.get(
            _tags_url(),
            auth=_auth(),
            params={"search": name, "per_page": 100},
            timeout=30,
        )
        resp.raise_for_status()
        for t in resp.json():
            if str(t.get("name", "")).strip().lower() == key:
                _TAG_CACHE[key] = int(t["id"])
                return _TAG_CACHE[key]
    except Exception as e:
        logger.warning("Tag search failed for '%s': %s", name, e)

    # 2) create
    try:
        resp = requests.post(
            _tags_url(),
            auth=_auth(),
            json={"name": name},
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        tid = int(resp.json()["id"])
        _TAG_CACHE[key] = tid
        return tid
    except Exception as e:
        logger.error("Tag create failed for '%s': %s", name, e)
        return None

def _ensure_categories(payload: Dict) -> None:
    if WP_CATEGORY_ID:
        payload["categories"] = [WP_CATEGORY_ID]

def _normalize_tags(payload: Dict) -> None:
    """
    Convert payload['tags'] (names or IDs) into a proper ID list for WP REST.
    Supports:
      - list[str] of names  -> resolve to IDs (create if needed)
      - list[int] already   -> pass through
      - missing/empty       -> remove
    """
    if "tags" not in payload or payload["tags"] is None:
        return

    raw = payload["tags"]
    ids: List[int] = []

    if isinstance(raw, list):
        for v in raw:
            if isinstance(v, int):
                ids.append(v)
            else:
                tid = _get_or_create_tag_id(str(v))
                if tid:
                    ids.append(tid)
    else:
        # single value
        if isinstance(raw, int):
            ids = [raw]
        else:
            tid = _get_or_create_tag_id(str(raw))
            if tid:
                ids = [tid]

    if ids:
        payload["tags"] = ids
    else:
        # nothing resolved — drop the field to avoid 400
        payload.pop("tags", None)

def _post_to_wordpress(payload: Dict) -> Dict:
    _ensure_categories(payload)
    _normalize_tags(payload)
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

# ----------------- Publishing entrypoints -----------------

def publish_items(items: List[Dict]) -> int:
    """Publish EVERYTHING (no skip logic), up to MAX_POSTS_PER_RUN."""
    created = 0
    for item in items:
        if created >= MAX_POSTS_PER_RUN:
            break

        payload = build_article(item, category_id=WP_CATEGORY_ID)
        payload.setdefault("status", WP_PUBLISH_STATUS)
        try:
            _post_to_wordpress(payload)
            created += 1
            time.sleep(0.3)
        except Exception as e:
            logger.error("Failed to publish item (afm_key=%s): %s", item.get("afm_key") or item.get("unique_id"), e)
    logger.info("Published %d items.", created)
    return created

def publish_to_wordpress(arg: Union[Dict, List[Dict]]) -> int:
    """
    Back-compat: accept either a single article payload (dict) or
    a list of source records (list[dict]).
    """
    if isinstance(arg, dict):
        payload = dict(arg)
        payload.setdefault("status", WP_PUBLISH_STATUS)
        try:
            _post_to_wordpress(payload)
            logger.info("Published 1 item (single payload).")
            return 1
        except Exception as e:
            logger.error("Failed to publish single payload: %s", e)
            return 0
    if isinstance(arg, list):
        return publish_items(arg)
    raise TypeError("publish_to_wordpress expects a dict (payload) or list[dict] (records).")
