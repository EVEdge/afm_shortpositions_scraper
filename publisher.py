import os
import time
import logging
from typing import Dict, Tuple, Optional, List, Union

import requests
from article_builder import build_article

WP_BASE_URL        = os.getenv("WP_BASE_URL", "").rstrip("/")
WP_USERNAME        = os.getenv("WP_USERNAME")
WP_APP_PASSWORD    = os.getenv("WP_APP_PASSWORD")

# Defaults per your workflow
WP_CATEGORY_ID     = int(os.getenv("WP_CATEGORY_ID", "777") or 777)
WP_PUBLISH_STATUS  = os.getenv("WP_PUBLISH_STATUS", "draft")  # post as draft
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

def _post_url(post_id: int) -> str:
    return _api(f"posts/{post_id}")

# cache tag name -> id per run
_TAG_CACHE: dict[str, int] = {}

def _sanitize_tag_name(name: str) -> str:
    s = str(name).strip()
    s = s.replace("&", "and")
    for ch in [",", ".", ";", ":", "’", "'", '"', "(", ")", "[", "]", "{", "}", "/", "\\"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())[:190]
    return s or "tag"

def _get_or_create_tag_id(name: str) -> Optional[int]:
    """Resolve a tag name to an ID; create it if missing (handles term_exists)."""
    if not name:
        return None
    key = str(name).strip().lower()
    if key in _TAG_CACHE:
        return _TAG_CACHE[key]

    # 1) try search (exact match)
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

    def _post_tag(tag_name: str) -> Optional[int]:
        r = requests.post(
            _tags_url(),
            auth=_auth(),
            json={"name": tag_name},
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        if r.status_code == 201:
            return int(r.json()["id"])
        # WP commonly returns term_exists
        try:
            data = r.json()
            if isinstance(data, dict) and data.get("code") == "term_exists":
                return int(data.get("data", {}).get("term_id"))
        except Exception:
            pass
        return None

    # 2) create original
    tid = _post_tag(name)
    if tid:
        _TAG_CACHE[key] = tid
        return tid

    # 3) sanitize + retry
    clean = _sanitize_tag_name(name)
    if clean != name:
        tid = _post_tag(clean)
        if tid:
            _TAG_CACHE[key] = tid
            return tid

    logger.error("Tag create failed for '%s'", name)
    return None

def _ensure_categories(payload: Dict) -> None:
    # Always enforce category 777 by default (can still be overridden via env)
    if WP_CATEGORY_ID:
        payload["categories"] = [WP_CATEGORY_ID]

def _normalize_tags(payload: Dict) -> None:
    """
    Convert payload['tags'] (names or IDs) into a proper ID list for WP REST.
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
        if isinstance(raw, int):
            ids = [raw]
        else:
            tid = _get_or_create_tag_id(str(raw))
            if tid:
                ids = [tid]

    if ids:
        payload["tags"] = ids
    else:
        payload.pop("tags", None)

# ----------------- Duplicate protection -----------------

_UID_PREFIX = "PW-AFM-UID:"

def _extract_uid(payload: Dict) -> Optional[str]:
    meta_uid = (payload.get("meta") or {}).get("afm_unique_id")
    return (str(meta_uid).strip() or None) if meta_uid else None

def _embed_uid_marker(payload: Dict, uid: str) -> None:
    """
    Append an invisible unique marker to the content so we can detect duplicates
    via WP search API. We include both an HTML comment and a hidden span.
    """
    marker_comment = f"<!--{_UID_PREFIX}{uid}-->"
    marker_span    = f'<span style="display:none">{_UID_PREFIX}{uid}</span>'
    content = payload.get("content") or ""
    payload["content"] = (content + "\n" + marker_comment + "\n" + marker_span).strip()

def _post_exists_by_uid(uid: str) -> bool:
    """
    Search WP posts for our unique marker. We fetch a few results and verify
    marker presence in the content to avoid false positives.
    """
    try:
        resp = requests.get(
            _posts_url(),
            auth=_auth(),
            params={"search": _UID_PREFIX + uid, "per_page": 5, "status": "any"},
            timeout=30,
        )
        resp.raise_for_status()
        results = resp.json()
        if isinstance(results, list) and results:
            # Optional hard check: fetch the first few posts' content to confirm marker
            for post in results:
                pid = int(post.get("id"))
                detail = requests.get(_post_url(pid), auth=_auth(), timeout=30)
                if detail.status_code == 200:
                    content = (detail.json().get("content", {}) or {}).get("rendered", "") or ""
                    if (_UID_PREFIX + uid) in content:
                        return True
        return False
    except Exception as e:
        logger.warning("UID duplicate check failed for %s: %s", uid, e)
        # Fail-open: if we can't check, we won't block posting
        return False

# ----------------- Core posting -----------------

def _post_to_wordpress(payload: Dict) -> Dict:
    _ensure_categories(payload)
    _normalize_tags(payload)
    payload.setdefault("status", WP_PUBLISH_STATUS)

    # Duplicate protection
    uid = _extract_uid(payload)
    if uid:
        _embed_uid_marker(payload, uid)
        if _post_exists_by_uid(uid):
            logger.info("Duplicate detected; skipping post for uid=%s", uid)
            return {"skipped": True, "reason": "duplicate", "uid": uid}

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
        payload.setdefault("status", WP_PUBLISH_STATUS)  # draft

        try:
            res = _post_to_wordpress(payload)
            if isinstance(res, dict) and res.get("skipped"):
                logger.info("Skipped duplicate (uid=%s).", res.get("uid"))
            else:
                created += 1
                time.sleep(0.3)
        except Exception as e:
            logger.error("Failed to publish item (afm_key=%s): %s", item.get("afm_key") or item.get("unique_id"), e)
    logger.info("Published %d items.", created)
    return created

def publish_to_wordpress(arg: Union[Dict, List[Dict]]) -> int:
    """
    Back-compat: accept either a single already-built article payload (dict) or
    a list of source records (list[dict]).
    """
    if isinstance(arg, dict):
        payload = dict(arg)
        payload.setdefault("status", WP_PUBLISH_STATUS)  # draft
        try:
            res = _post_to_wordpress(payload)
            if isinstance(res, dict) and res.get("skipped"):
                logger.info("Skipped duplicate (uid=%s).", res.get("uid"))
                return 0
            logger.info("Published 1 item (single payload).")
            return 1
        except Exception as e:
            logger.error("Failed to publish single payload: %s", e)
            return 0
    if isinstance(arg, list):
        return publish_items(arg)
    raise TypeError("publish_to_wordpress expects a dict (payload) or list[dict] (records).")
