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


# ---------- small helpers ----------

def _clean(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())

def _pct_to_float(p: str) -> float:
    if p is None:
        return 0.0
    s = str(p).replace("%", "").replace(",", ".").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0

def _parse_date(d: str) -> Tuple[str, str]:
    raw = _clean(d)
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return raw, datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"(\d{2})[-/](\d{2})[-/](\d{4})", raw)
    if m:
        return raw, f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", raw)
    if m:
        return raw, f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
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


# ---------- discover the CSV link on the page ----------

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


# ---------- CSV parsing (header-agnostic) ----------

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")

def _sniff_delimiter(text: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=";,\t")
        return dialect.delimiter
    except Exception:
        return ";"

def _map_get(row: Dict[str, str], keys: List[str]) -> str:
    if not row:
        return ""
    for k in keys:
        if k in row and row[k]:
            return _clean(row[k])
    lower = {k.lower(): v for k, v in row.items()}
    for k in keys:
        lk = k.lower()
        if lk in lower and lower[lk]:
            return _clean(lower[lk])
    return ""

def _pick_by_heuristics(row: Dict[str, str]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Return indices for pct_idx, date_idx, isin_idx if we can detect them."""
    pct_idx = date_idx = isin_idx = None
    for i, (_k, v) in enumerate(row.items()):
        t = _clean(v or "")
        if pct_idx is None and re.search(r"\d[\d\.,]*\s*%", t):
            pct_idx = i
        if date_idx is None and re.search(r"\d{2}[-/]\d{2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2}", t):
            date_idx = i
        if isin_idx is None and _ISIN_RE.match(t):
            isin_idx = i
    return pct_idx, date_idx, isin_idx

def _parse_csv_rows(text: str) -> List[ShortPosition]:
    delim = _sniff_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    SYM_ISSUER = ["Uitgevende instelling", "Issuer", "Uitgevende", "Issuer name", "Company"]
    SYM_ISIN   = ["ISIN"]
    SYM_HOLDER = ["Partij", "Houder", "Short seller", "Meldingsplichtige", "Shorter", "Shorting party"]
    SYM_PCT    = ["Netto shortpositie", "Net short position", "Netto-shortpositie", "%", "Net short %"]
    SYM_DATE   = ["Datum", "Date", "Datum melding", "Meldingsdatum", "Position date"]

    out: List[ShortPosition] = []
    seen = 0

    for row in reader:
        seen += 1

        # 1) Try normal header mapping
        issuer = _map_get(row, SYM_ISSUER)
        holder = _map_get(row, SYM_HOLDER)
        pct_raw = _map_get(row, SYM_PCT)
        date_raw = _map_get(row, SYM_DATE)
        isin = _map_get(row, SYM_ISIN) or None

        # 2) Heuristics for % / date / ISIN
        if not pct_raw or not date_raw or not isin or not issuer or not holder:
            pct_idx, date_idx, isin_idx = _pick_by_heuristics(row)
            cells = [(_clean(v or ""), i) for i, (_k, v) in enumerate(row.items())]

            if (not pct_raw) and pct_idx is not None:
                pct_raw = cells[pct_idx][0]
            if (not date_raw) and date_idx is not None:
                date_raw = cells[date_idx][0]
            if (not isin) and isin_idx is not None:
                isin = cells[isin_idx][0] or None

            # 3) If issuer/holder still empty, choose the two longest remaining cells (in order)
            if not issuer or not holder:
                blocked = {idx for idx in (pct_idx, date_idx, isin_idx) if idx is not None}
                candidates = [(len(txt), j, txt) for (txt, j) in cells if txt and j not in blocked]
                candidates.sort(reverse=True)  # longest first
                # Keep original order among the top two
                top = sorted([c for c in candidates[:2]], key=lambda x: x[1])
                names = [t for (_len, _j, t) in top]
                if not issuer and names:
                    issuer = names[0]
                if not holder and len(names) > 1:
                    holder = names[1]

        # 4) Minimum required fields
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

    resp = requests.get(csv_url, timeout=30, headers={"User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)"})
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
