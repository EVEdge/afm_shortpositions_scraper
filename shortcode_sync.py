"""Synchronize validated AFM historical filings with the WordPress shortcode plugin.

Opt-in via AFM_SHORTCODE_SYNC_ENABLED=1 once the WP plugin has been activated.
Does not change the existing AFM article publication or its state database.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

IMPORT_BATCH_SIZE = 100
HTTP_TIMEOUT = (10, 30)


def shortcode_sync_enabled() -> bool:
    return os.getenv("AFM_SHORTCODE_SYNC_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _valid_date(value: object, label: str) -> str:
    raw = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        raise ValueError(f"Invalid {label}: {raw!r}")
    datetime.strptime(raw, "%Y-%m-%d")
    return raw


def _number(value: object, label: str, *, required: bool) -> Optional[float]:
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise ValueError(f"Missing {label}")
        return None
    if isinstance(value, bool):
        raise ValueError(f"Invalid {label}: boolean")
    try:
        number = float(str(value).strip().replace("%", "").replace(",", "."))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {label}: {value!r}") from exc
    if not math.isfinite(number) or not 0 <= number <= 100:
        raise ValueError(f"Invalid {label}: {value!r}")
    return round(number, 4)


def prepare_shortcode_records(records: Iterable[Dict]) -> List[Dict]:
    """Keep only fields used by WP; reject inconsistent duplicate source UIDs."""
    by_uid: Dict[str, Dict] = {}
    for item in records:
        if not isinstance(item, dict):
            raise ValueError("AFM record is not a dictionary")
        uid = str(item.get("unique_id") or item.get("afm_key") or "").strip()
        issuer = str(item.get("issuer") or item.get("emittent") or "").strip()
        holder = str(item.get("short_seller") or item.get("melder") or "").strip()
        isin = str(item.get("issuer_isin") or "").strip().upper()
        if not re.fullmatch(r"[a-zA-Z0-9_-]{8,64}", uid):
            raise ValueError(f"Invalid AFM filing UID: {uid!r}")
        if not issuer or not holder or len(issuer) > 255 or len(holder) > 255:
            raise ValueError(f"Invalid AFM issuer or short seller: uid={uid}")
        if isin and (len(isin) > 20 or not re.fullmatch(r"[A-Z0-9]+", isin)):
            raise ValueError(f"Invalid ISIN: uid={uid} isin={isin!r}")
        position_date = _valid_date(item.get("position_date_iso"), "position_date")
        pct = _number(
            item.get("net_short_pct_num", item.get("kapitaalbelang")),
            "net_short_pct_num", required=True,
        )
        previous = _number(item.get("prev_net_short_pct_num"), "prev_net_short_pct_num", required=False)
        previous_date_raw = item.get("prev_position_date_iso")
        previous_date = _valid_date(previous_date_raw, "prev_position_date") if previous_date_raw else None
        if previous_date and previous_date > position_date:
            raise ValueError(f"Previous position date is after current date: uid={uid}")
        row = {
            "uid": uid,
            "aandeel": issuer,
            "isin": isin,
            "fonds": holder,
            "positie": pct,
            "vorige_positie": previous,
            "positiedatum": position_date,
            "vorige_datum": previous_date,
        }
        if uid in by_uid and by_uid[uid] != row:
            raise ValueError(f"AFM filing UID occurs with conflicting data: {uid}")
        by_uid[uid] = row
    if not by_uid:
        raise ValueError("Refusing to sync empty AFM dataset")
    return sorted(by_uid.values(), key=lambda row: row["uid"])


def shortcode_dataset_hash(rows: List[Dict]) -> str:
    canonical = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=2,
        status=3,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def _json_or_fail(response: requests.Response) -> Dict:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        # Do not log Basic Auth, headers, or application password.
        raise RuntimeError(
            f"AFM shortcode WordPress sync HTTP {response.status_code}: "
            f"{response.text[:350]}"
        ) from exc
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("AFM shortcode WordPress sync returned invalid JSON") from exc
    if not isinstance(data, dict):
        raise RuntimeError("AFM shortcode WordPress sync returned invalid response structure")
    return data


def enrich_shortcode_links(
    rows: List[Dict],
    *,
    stock_resolver: Optional[Callable[[str, Optional[str]], Optional[str]]] = None,
    holder_resolver: Optional[Callable[[str], Optional[str]]] = None,
) -> List[Dict]:
    """Attach only verified DeKoersen internal URLs, once per unique entity.

    URL lookup happens during sync, never during a visitor pageview. Missing
    lookups are omitted from the payload so WordPress can preserve an already
    verified URL if a later lookup temporarily fails.
    """
    if stock_resolver is None or holder_resolver is None:
        from internal_links import resolve_holder_page, resolve_stock_page
        if stock_resolver is None:
            stock_resolver = lambda issuer, isin: resolve_stock_page(issuer, isin)
        if holder_resolver is None:
            holder_resolver = resolve_holder_page

    stock_cache: Dict[tuple[str, str], Optional[str]] = {}
    holder_cache: Dict[str, Optional[str]] = {}
    enriched: List[Dict] = []

    for source in rows:
        row = dict(source)
        stock_key = (str(row.get("aandeel") or ""), str(row.get("isin") or ""))
        holder_key = str(row.get("fonds") or "")

        if stock_key not in stock_cache:
            try:
                stock_cache[stock_key] = stock_resolver(stock_key[0], stock_key[1] or None)
            except Exception as exc:
                logger.warning("Shortcode stock-link lookup failed for %r: %s", stock_key[0], exc)
                stock_cache[stock_key] = None

        if holder_key not in holder_cache:
            try:
                holder_cache[holder_key] = holder_resolver(holder_key)
            except Exception as exc:
                logger.warning("Shortcode holder-link lookup failed for %r: %s", holder_key, exc)
                holder_cache[holder_key] = None

        if stock_cache[stock_key]:
            row["aandeel_url"] = stock_cache[stock_key]
        if holder_cache[holder_key]:
            row["fonds_url"] = holder_cache[holder_key]
        enriched.append(row)

    logger.info(
        "AFM shortcode link enrichment: stock_entities=%d linked=%d, "
        "holder_entities=%d linked=%d.",
        len(stock_cache),
        sum(1 for value in stock_cache.values() if value),
        len(holder_cache),
        sum(1 for value in holder_cache.values() if value),
    )
    return enriched


def sync_records_to_wordpress(
    records: Iterable[Dict],
    *,
    base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    http: Optional[requests.Session] = None,
    stock_resolver: Optional[Callable[[str, Optional[str]], Optional[str]]] = None,
    holder_resolver: Optional[Callable[[str], Optional[str]]] = None,
    force: bool = False,
    endpoint_namespace: str = "dk-afm",
) -> int:
    """Sync complete AFM export; return newly transmitted rows (0 if unchanged).

    WordPress stores history persistently. Partial network failures are safe:
    the next full batch run retries idempotently, and the digest is committed
    only on the last successful batch.
    """
    rows = prepare_shortcode_records(records)
    digest = shortcode_dataset_hash(rows)
    root = (base_url or os.getenv("WP_BASE_URL") or "").rstrip("/")
    user = username or os.getenv("WP_USERNAME") or ""
    secret = password or os.getenv("WP_APP_PASSWORD") or ""
    if not root.startswith(("https://", "http://")) or not user or not secret:
        raise RuntimeError("AFM shortcode sync requires WP_BASE_URL, WP_USERNAME, WP_APP_PASSWORD")
    if root.startswith("http://") and not root.startswith("http://localhost") and not root.startswith("http://127.0.0.1"):
        raise RuntimeError("AFM shortcode sync requires HTTPS for remote WordPress authentication")
    if endpoint_namespace not in {"dk-afm", "pw-afm"}:
        raise ValueError("Unsupported AFM WordPress endpoint namespace.")
    url = root + f"/wp-json/{endpoint_namespace}/v1/import"
    auth = (user, secret)
    client = http or _session()
    try:
        status = _json_or_fail(client.get(url, auth=auth, timeout=HTTP_TIMEOUT))
        if (
            not force
            and status.get("dataset_hash") == digest
            and status.get("link_sync_hash") == digest
            and isinstance(status.get("row_count"), int)
            and status["row_count"] >= len(rows)
        ):
            logger.info("AFM shortcode dataset unchanged (%d rows): no import needed.", len(rows))
            return 0
        rows = enrich_shortcode_links(
            rows,
            stock_resolver=stock_resolver,
            holder_resolver=holder_resolver,
        )
        num_batches = (len(rows) + IMPORT_BATCH_SIZE - 1) // IMPORT_BATCH_SIZE
        written_total = 0
        unchanged_total = 0
        for index in range(num_batches):
            batch = rows[index * IMPORT_BATCH_SIZE:(index + 1) * IMPORT_BATCH_SIZE]
            payload = {
                "dataset_hash": digest,
                "batch_index": index,
                "batch_total": num_batches,
                "link_schema_version": "1",
                "records": batch,
            }
            result = _json_or_fail(client.post(url, json=payload, auth=auth, timeout=HTTP_TIMEOUT))
            if result.get("imported") != len(batch):
                raise RuntimeError(
                    f"AFM shortcode batch {index + 1}/{num_batches} acknowledged "
                    f"{result.get('imported')!r} instead of {len(batch)} records"
                )

            written = result.get("written")
            unchanged = result.get("unchanged")
            if isinstance(written, int) and written >= 0:
                written_total += written
            if isinstance(unchanged, int) and unchanged >= 0:
                unchanged_total += unchanged

        final = _json_or_fail(client.get(url, auth=auth, timeout=HTTP_TIMEOUT))
        if final.get("dataset_hash") != digest:
            raise RuntimeError("AFM shortcode sync digest not confirmed by WordPress")
        if final.get("link_sync_hash") != digest:
            raise RuntimeError("AFM shortcode link sync was not confirmed by WordPress")
        logger.info(
            "AFM shortcode sync completed: %d rows across %d batches; "
            "WordPress DB writes=%d, unchanged=%d.",
            len(rows),
            num_batches,
            written_total,
            unchanged_total,
        )
        return len(rows)
    finally:
        if http is None:
            client.close()
