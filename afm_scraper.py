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
        d = asdict(self)
        # --- Backward-compat aliases for the old pipeline ---
        d["melder"] = self.short_seller      # old “meldingen” field
        d["emittent"] = self.issuer          # old “meldingen” field
        d["afm_key"] = self.unique_id        # stable key for DB/dedupe
        return d


# ---------- helpers ----------

DATE_RE = re.compile(r"(\d{4})[-/](\d{2})[-/](\d{2})|(\d{2})[-/](\d{2})[-/](\d{4})")

def _clean(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())

def _pct_to_float(p: str) -> float:
    """Accept '0,60', '0.60', '0,60%' etc.; CSV here has no % sign."""
    if p is None:
        return 0.0
    s = str(p).replace("%", "").replace(",", ".").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0

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


# ---------- CSV parsing for current AFM headers ----------

# Exact Dutch headers observed in your file:
COL_HOLDER = "Positie houder"
COL_ISSUER = "Naam van de emittent"
COL_ISIN   = "ISIN"
COL_PCT    = "Netto Shortpositie"      # numeric, no % sign
COL_DATE   = "Positiedatum"            # e.g. 2025-11-07 00:00:00

def _parse_csv_rows(text: str) -> List[ShortPosition]:
    delim = _sniff_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    out: List[ShortPosition] = []
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

        out.append(
            ShortPosition(
                issuer=issuer,
                issuer_isin=isin,
                short_seller=holder,
                net_short_pct=pct_raw if "%" in pct_raw else f"{pct_raw}%",  # cosmetic
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
