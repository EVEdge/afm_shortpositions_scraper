# publisher.py

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
    if WP_CATEGORY_ID:
        payload["categories"] = [WP_CATEGORY_ID]


def _dedupe_key(item: Dict) -> Optional[str]:
    return item.get("afm_key") or item.get("unique_id")


def _derive_short_fields(item: Dict) -> Tuple[float, str]:
    """
    Detect short-position items and normalize their % fields into the legacy
    'kapitaalbelang' fields so the old skip logic never fires.
    """
    is_short = (
        item.get("meldingstype") == "shortpositie"
        or "net_short_pct" in item
        or "net_short_pct_num" in item
        or "Netto Shortpositie" in item
    )

    if is_short:
        num_val = item.get("net_short_pct_num")
        if num_val is None:
            s = str(item.get("net_short_pct") or item.get("Netto Shortpositie") or "").replace("%", "").replace(",", ".")
            try:
                num_val = float(s)
            except Exception:
                num_val = 0.0
        pretty = item.get("net_short_pct") or f"{num_val:.2f}%"

        item["kapitaalbelang"] = num_val
        item["kapitaalbelang_str"] = pretty
        item["meldingstype"] = "shortpositie"
        return num_val, pretty

    # Legacy path
    try:
        num_val = float(str(item.get("kapitaalbelang", "0")).replace(",", "."))
    except Exception:
        num_val = 0.0
    pretty = item.get("kapitaalbelang_str") or (f"{num_val:.2f}%" if num_val else "n.n.b.")
    return num_val, pretty


def _should_skip(item: Dict) -> Optional[str]:
    num, pretty = _derive_short_fields(item)

    # Never skip short-positions
    if item.get("meldingstype") == "shortpositie":
        return None

    # Legacy skip rule (meldingen only)
    if not num and (pretty.lower() == "n.n.b." or pretty.strip() in {"", "0", "0%"}):
        return f"No valid kapitaalbelang (would be n.n.b.) for afm_key={_dedupe_key(item)}"
    return None


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
    """Publish up to MAX_POSTS_PER_RUN items."""
    created = 0

    for item in items:
        if created >= MAX_POSTS_PER_RUN:
            break

        skip = _should_skip(item)
        if skip:
            logger.info("[SKIP] %s", skip)
            continue

        payload = build_article(item, category_id=WP_CATEGORY_ID)
        payload["status"] = WP_PUBLISH_STATUS

        _ensure_list_category(payload)
        _resolve_tags_to_ids_if_needed(payload)

        afm_key = _dedupe_key(item)
        if afm_key:
            payload.setdefault("meta", {})
            payload["meta"]["afm_key"] = afm_key

        try:
            _post_to_wordpress(payload)
            created += 1
            time.sleep(0.4)
        except Exception as e:
            logger.error("Failed to publish item (afm_key=%s): %s", afm_key, e)

    return created


# ---------- Backward-compat alias for existing main.py ----------
def publish_to_wordpress(items: List[Dict]) -> int:
    """
    Old entrypoint name expected by some main.py versions.
    Delegates to publish_items().
    """
    return publish_items(items)
