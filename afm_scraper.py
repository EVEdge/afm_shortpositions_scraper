import csv
import io
import logging
import re
import hashlib
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, DefaultDict
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

AFM_SHORTPOS_URL = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

# ---------- CSV headers currently used by AFM ----------
COL_HOLDER = "Positie houder"            # short seller
COL_ISSUER = "Naam van de emittent"      # issuer
COL_ISIN   = "ISIN"
COL_PCT    = "Netto Shortpositie"        # numeric, no % sign
COL_DATE   = "Positiedatum"              # e.g. 2025-11-07 00:00:00

DATE_RE = re.compile(r"(\d{4})[-/](\d{2})[-/](\d{2})|(\d{2})[-/](\d{2})[-/](\d{4})")


@dataclass
class ShortPosition:
    issuer: str
    issuer_isin: Optional[str]
    short_seller: str
    net_short_pct: str
    net_short_pct_num: float
    position_date: str
    position_date_iso: str
    source_url: str
    unique_id: str

    # previously added fields
    prev_net_short_pct: Optional[str] = None
    prev_net_short_pct_num: Optional[float] = None
    prev_position_date_iso: Optional[str] = None
    direction: Optional[str] = None  # "up" | "down" | None

    # NEW: full previous history (most recent first, up to 10)
    history: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict:
        d = asdict(self)
        # legacy aliases so old code NEVER skips and can tag/dedupe
        d["melder"] = self.short_seller
        d["emittent"] = self.issuer
        d["afm_key"] = self.unique_id
        d["kapitaalbelang"] = self.net_short_pct_num
        d["kapitaalbelang_str"] = self.net_short_pct or f"{self.net_short_pct_num:.2f}%"
        d["meldingstype"] = "shortpositie"

        # expose "previous" info & history
        if self.prev_net_short_pct_num is not None:
            d["prev_net_short_pct_num"] = self.prev_net_short_pct_num
            d["prev_net_short_pct"] = self.prev_net_short_pct
            d["prev_position_date_iso"] = self.prev_position_date_iso
            d["direction"] = self.direction
        if self.history:
            d["history"] = self.history
        return d


# ---------- helpers ----------

def _clean(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())

def _pct_to_float(p: str) -> float:
    s = str(p or "").replace("%", "").replace(",", ".").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0

def _pct_to_str_two(num: float, fallback: str = "") -> str:
    if num is not None:
        try:
            return f"{float(num):.2f}%".replace(".", ",")
        except Exception:
            pass
    s = (fallback or "").strip()
    if not s:
        return ""
    s = s.replace(".", ",")
    return s if s.endswith("%") else s + "%"

def _parse_date(d: str) -> Tuple[str, str]:
    raw = _clean(d)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return raw, datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = DATE_RE.search(raw)
    if m:
        if m.group(1):  # yyyy-mm-dd
            return raw, f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        else:           # dd-mm-yyyy
            return raw, f"{m.group(6)}-{m.group(5)}-{m.group(4)}"
    return raw, raw

def _uid(issuer: str, short_seller: str, iso_date: str, pct: str) -> str:
    base = f"{issuer}|{short_seller}|{iso_date}|{pct}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def _abs_url(href: str) -> str:
    return href if not href.startswith("/") else "https://www.afm.nl" + href

def _decode_best(content: bytes) -> str:
    for enc in ("utf-8", "utf-16", "cp1252", "latin-1"):
        try:
            return content.decode(enc)
        except Exception:
            continue
    return content.decode("utf-8", errors="ignore")

def _sniff_delimiter(text: str) -> str:
    try:
        return csv.Sniffer().sniff(text[:4096], delimiters=";,\t").delimiter
    except Exception:
        return ";"


# ---------- discover CSV link ----------

def _find_csv_url() -> Optional[str]:
    logger.info("Fetching AFM page: %s", AFM_SHORTPOS_URL)
    r = requests.get(
        AFM_SHORTPOS_URL,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)"},
    )
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = _clean(a.get_text()).lower()
        if ".csv" in href.lower() or "csv" in label or "download" in label:
            url = _abs_url(href)
            logger.info("Found CSV link: %s", url)
            return url

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if "export.aspx" in href and "format=csv" in href:
            url = _abs_url(a["href"])
            logger.info("Found export link: %s", url)
            return url

    logger.warning("No CSV link found on AFM page.")
    return None


# ---------- parsing & grouping ----------

def _parse_csv_rows(text: str) -> List[ShortPosition]:
    delim = _sniff_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    rows: List[ShortPosition] = []
    seen = 0
    for row in reader:
        seen += 1
        issuer = _clean(row.get(COL_ISSUER, ""))
        holder = _clean(row.get(COL_HOLDER, ""))
        isin   = _clean(row.get(COL_ISIN, "")) or None
        pct_raw = _clean(str(row.get(COL_PCT, "")))
        date_raw = _clean(str(row.get(COL_DATE, "")))
        if not issuer or not holder or not pct_raw:
            continue

        pct_num = _pct_to_float(pct_raw)
        date_raw, iso = _parse_date(date_raw)
        uid = _uid(issuer, holder, iso, pct_raw)

        rows.append(
            ShortPosition(
                issuer=issuer,
                issuer_isin=isin,
                short_seller=holder,
                net_short_pct=_pct_to_str_two(pct_num, pct_raw),
                net_short_pct_num=pct_num,
                position_date=date_raw,
                position_date_iso=iso,
                source_url=AFM_SHORTPOS_URL,
                unique_id=uid,
            )
        )

    logger.info("Parsed CSV rows: seen=%d, kept=%d", seen, len(rows))
    return rows


def _attach_previous(rows: List[ShortPosition]) -> List[ShortPosition]:
    """
    For each (issuer, short_seller) group, sort by date and keep only the latest item,
    but attach the most recent previous position (if any) to that latest item.
    Also attach history (up to 10 previous filings, most recent first).
    """
    groups: DefaultDict[Tuple[str, str], List[ShortPosition]] = defaultdict(list)
    for sp in rows:
        groups[(sp.issuer, sp.short_seller)].append(sp)

    output: List[ShortPosition] = []
    for key, items in groups.items():
        # sort by date ascending (older -> newer)
        items.sort(key=lambda x: (x.position_date_iso or "", x.net_short_pct_num or 0.0))
        latest = items[-1]
        prev_items = items[:-1]

        if prev_items:
            prev = prev_items[-1]
            latest.prev_net_short_pct_num = prev.net_short_pct_num
            latest.prev_net_short_pct = prev.net_short_pct
            latest.prev_position_date_iso = prev.position_date_iso
            if latest.net_short_pct_num > prev.net_short_pct_num:
                latest.direction = "up"
            elif latest.net_short_pct_num < prev.net_short_pct_num:
                latest.direction = "down"
            else:
                latest.direction = None

            # Build history table: most recent first, limit 10
            prev_items_sorted_desc = sorted(
                prev_items, key=lambda x: (x.position_date_iso or "", x.net_short_pct_num or 0.0), reverse=True
            )
            history = []
            for sp in prev_items_sorted_desc[:10]:
                history.append({
                    "date": sp.position_date_iso,
                    "pct_num": sp.net_short_pct_num,
                    "pct": sp.net_short_pct,  # already two decimals with comma
                })
            latest.history = history

        output.append(latest)

    return output


# ---------- public scrape ----------

def scrape_short_positions() -> List[ShortPosition]:
    csv_url = _find_csv_url()
    if not csv_url:
        logger.warning("AFM short positions: found 0 items (no CSV link).")
        return []

    resp = requests.get(
        csv_url,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)"},
    )
    resp.raise_for_status()
    text = _decode_best(resp.content)

    all_rows = _parse_csv_rows(text)
    latest_with_prev = _attach_previous(all_rows)
    return latest_with_prev


# ---------- compatibility for your main.py ----------

def fetch_items() -> List[Dict]:
    return [sp.to_dict() for sp in scrape_short_positions()]

def fetch_afm_table() -> List[Dict]:
    return fetch_items()
