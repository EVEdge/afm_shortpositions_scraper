# afm_scraper.py

import csv
import io
import logging
import re
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

AFM_SHORTPOS_URL = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

logger = logging.getLogger(__name__)
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)


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

    def to_dict(self) -> Dict:
        return asdict(self)


# ---------- helpers ----------

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")
DATE_RE = re.compile(r"(\d{2})[-/](\d{2})[-/](\d{2,4})|(\d{4})[-/](\d{2})[-/](\d{2})")
PERCENT_RE = re.compile(r"\d[\d\.,]*\s*%")

def _clean(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())

def _pct_to_float(p: str) -> float:
    if p is None:
        return 0.0
    s = str(p).replace("%", "").replace(",", ".").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    try:
        v = float(m.group(1)) if m else 0.0
        # guard rails — short positions are within 0..50 typically
        if v < 0 or v > 100:
            return 0.0
        return v
    except Exception:
        return 0.0

def _parse_date(d: str) -> Tuple[str, str]:
    raw = _clean(d)
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return raw, datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = DATE_RE.search(raw)
    if m:
        if m.group(1):  # dd-mm-yyyy
            return raw, f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        else:           # yyyy-mm-dd
            return raw, f"{m.group(4)}-{m.group(5)}-{m.group(6)}"
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


# ---------- header-agnostic CSV parsing ----------

def _score_name_like(text: str) -> float:
    """
    Heuristic score for 'name-looking' strings (issuer/short-seller):
    - prefers alphabetic & spaces
    - penalizes digits/pure codes
    """
    t = _clean(text)
    if not t:
        return 0.0
    letters = sum(c.isalpha() for c in t)
    spaces  = t.count(" ")
    digits  = sum(c.isdigit() for c in t)
    # short sellers/issuers are mostly letters + spaces, few digits
    return letters + 0.5 * spaces - 1.5 * digits

def _pick_columns_by_values(row: Dict[str, str]) -> Tuple[str, str, str, str, Optional[str]]:
    """
    From a CSV row (dict), extract: issuer, holder, pct_raw, date_raw, isin
    purely by inspecting cell values + weak header hints.
    """
    cells = [(k, _clean(v or "")) for k, v in row.items()]
    if not cells:
        return "", "", "", "", None

    # Detect candidates
    pct_candidates  = []
    date_candidates = []
    isin_candidates = []
    name_candidates = []

    for idx, (k, v) in enumerate(cells):
        if not v:
            continue
        # percentage: explicit '%' or a numeric within 0..50 (commas allowed)
        if PERCENT_RE.search(v):
            pct_candidates.append((idx, v))
        else:
            m = re.search(r"(\d+(?:[.,]\d+)?)", v)
            if m:
                try:
                    val = float(m.group(1).replace(",", "."))
                    if 0.0 < val <= 50.0:
                        pct_candidates.append((idx, v))
                except Exception:
                    pass

        if DATE_RE.search(v):
            date_candidates.append((idx, v))

        if ISIN_RE.match(v):
            isin_candidates.append((idx, v))

        name_candidates.append((idx, v, _score_name_like(v)))

    # choose the first/best matches
    pct_idx, pct_raw = (pct_candidates[0] if pct_candidates else (None, ""))
    date_idx, date_raw = (date_candidates[0] if date_candidates else (None, ""))

    isin = isin_candidates[0][1] if isin_candidates else None

    # Remove used indices from name pool
    used = {i for i in (pct_idx, date_idx) if i is not None}
    if isin_candidates:
        used.add(isin_candidates[0][0])

    names = [(i, v, s) for (i, v, s) in name_candidates if i not in used and v]
    # prefer higher score, but keep original order among the top 2 for stability
    names.sort(key=lambda t: (-t[2], t[0]))
    issuer = names[0][1] if len(names) >= 1 else ""
    holder = names[1][1] if len(names) >= 2 else ""

    return issuer, holder, pct_raw, date_raw, isin

def _parse_csv_rows(text: str) -> List[ShortPosition]:
    delim = _sniff_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    out: List[ShortPosition] = []
    seen = 0

    for row in reader:
        seen += 1

        issuer, holder, pct_raw, date_raw, isin = _pick_columns_by_values(row)

        if not issuer or not holder or not pct_raw:
            continue

        pct_num = _pct_to_float(pct_raw)
        date_raw, iso = _parse_date(date_raw or "")
        uid = _uid(issuer, holder, iso, pct_raw)

        out.append(
            ShortPosition(
                issuer=issuer,
                issuer_isin=isin,
                short_seller=holder,
                net_short_pct=pct_raw,
                net_short_pct_num=pct_num,
                position_date=date_raw,
                position_date_iso=iso,
                source_url=AFM_SHORTPOS_URL,
                unique_id=uid,
            )
        )

    logger.info("Parsed CSV rows: seen=%d, kept=%d", seen, len(out))
    return out


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

    items = _parse_csv_rows(text)
    if not items:
        logger.warning("AFM short positions: found 0 items (empty CSV parse).")
    return items


# ---------- compatibility for your main.py ----------

def fetch_items() -> List[Dict]:
    return [sp.to_dict() for sp in scrape_short_positions()]

def fetch_afm_table() -> List[Dict]:
    return fetch_items()
