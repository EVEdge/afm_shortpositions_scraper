import os
import csv
import io
import hashlib
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

AFM_SHORTPOS_URL = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

# Optional filter (OFF by default). Even if company_filter_pennywatch exists,
# we will NOT drop any rows unless SHORTPOS_USE_FILTER=1.
USE_FILTER = os.getenv("SHORTPOS_USE_FILTER", "0").strip().lower() in {"1", "true", "yes"}

def _always_true(*_args, **_kwargs) -> bool:
    return True

if USE_FILTER:
    try:
        from company_filter_pennywatch import is_approved_company as _row_ok
    except Exception:
        _row_ok = _always_true
else:
    _row_ok = _always_true

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("PW_LOG_LEVEL", "INFO").upper())


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


# ------------------------- helpers -------------------------

def _clean_text(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())


def _pct_to_float(p: str) -> float:
    """Accept '0,60%', '0.60', '0,60' etc."""
    if p is None:
        return 0.0
    s = str(p).replace("%", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        m = re.search(r"(\d+(?:[.,]\d+)?)", s)
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except Exception:
                return 0.0
        return 0.0


def _parse_date(d: str) -> Tuple[str, str]:
    raw = _clean_text(d)
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            iso = datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            return raw, iso
        except ValueError:
            pass
    m = re.search(r"(\d{2})[-/](\d{2})[-/](\d{4})", raw)
    if m:
        return raw, f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", raw)
    if m:
        return raw, f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return raw, raw


def _make_uid(issuer: str, short_seller: str, iso_date: str, pct: str) -> str:
    base = f"{issuer}|{short_seller}|{iso_date}|{pct}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


# ------------------------- HTML path -------------------------

def _all_tables(soup: BeautifulSoup) -> List[BeautifulSoup]:
    return soup.find_all("table")

def _table_headers(tbl: BeautifulSoup) -> List[str]:
    heads = []
    thead = tbl.find("thead")
    if thead:
        heads = thead.find_all("th")
    else:
        fr = tbl.find("tr")
        heads = fr.find_all(["th", "td"]) if fr else []
    return [_clean_text(h.get_text()) for h in heads]

def _iter_rows(tbl: BeautifulSoup) -> Iterable[List[BeautifulSoup]]:
    tbody = tbl.find("tbody") or tbl
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if tds:
            yield tds

def _score_table(headers_lower: List[str]) -> int:
    keys = ["netto", "short", "positie", "shortpositie", "partij", "datum", "issuer", "uitgevende"]
    return sum(1 for h in headers_lower for k in keys if k in h)

def _pick_best_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    best, score = None, -1
    for tbl in _all_tables(soup):
        heads = [h.lower() for h in _table_headers(tbl)]
        s = _score_table(heads)
        if s > score:
            best, score = tbl, s
    if best:
        logger.info("Using table with score %s", score)
    return best

def _header_map(tbl: BeautifulSoup) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    headers = [h.lower() for h in _table_headers(tbl)]
    for i, txt in enumerate(headers):
        if any(k in txt for k in ["uitgevende instelling", "issuer", "uitgevende"]):
            idx["issuer"] = i
        elif "isin" in txt:
            idx["isin"] = i
        elif any(k in txt for k in ["partij", "short", "meldingsplichtige", "houder"]):
            idx["short_seller"] = i
        elif any(k in txt for k in ["netto", "shortpositie", "%"]):
            idx["net_short_pct"] = i
        elif any(k in txt for k in ["datum", "date"]):
            idx["date"] = i

    if {"issuer", "short_seller", "net_short_pct"} <= idx.keys():
        return idx

    # Heuristic fallback on first row
    first = next(_iter_rows(tbl), None)
    if not first:
        return idx
    texts = [_clean_text(td.get_text()) for td in first]

    for i, t in enumerate(texts):
        if re.search(r"\d[\d\.,]*\s*%", t):
            idx.setdefault("net_short_pct", i)
    for i, t in enumerate(texts):
        if re.search(r"\d{2}[-/]\d{2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2}", t):
            idx.setdefault("date", i)
    idx.setdefault("issuer", 0)
    if "short_seller" not in idx:
        candidates = []
        for i, t in enumerate(texts):
            if i in {idx.get("issuer", -1), idx.get("net_short_pct", -1), idx.get("date", -1)}:
                continue
            candidates.append((len(t), i))
        if candidates:
            _, i = max(candidates)
            idx["short_seller"] = i
    logger.info("Header map (with heuristics): %s", idx)
    return idx

def _parse_from_table(tbl: BeautifulSoup) -> List[ShortPosition]:
    hmap = _header_map(tbl)
    out: List[ShortPosition] = []
    seen = 0
    for tds in _iter_rows(tbl):
        seen += 1

        def pick(key: str) -> str:
            i = hmap.get(key, -1)
            return _clean_text(tds[i].get_text()) if 0 <= i < len(tds) else ""

        issuer = pick("issuer")
        issuer_isin = pick("isin") or None
        short_seller = pick("short_seller")
        net_pct_raw = pick("net_short_pct")
        date_raw = pick("date")

        if not net_pct_raw:
            for td in tds:
                txt = _clean_text(td.get_text())
                if re.search(r"\d[\d\.,]*\s*%", txt):
                    net_pct_raw = txt
                    break
        if not date_raw:
            for td in tds:
                txt = _clean_text(td.get_text())
                if re.search(r"\d{2}[-/]\d{2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2}", txt):
                    date_raw = txt
                    break

        if not issuer or not short_seller or not net_pct_raw:
            continue
        if not _row_ok(issuer, issuer_isin):
            continue

        pct_num = _pct_to_float(net_pct_raw)
        date_raw, iso = _parse_date(date_raw or "")
        uid = _make_uid(issuer, short_seller, iso, net_pct_raw)

        out.append(
            ShortPosition(
                issuer=issuer,
                issuer_isin=issuer_isin,
                short_seller=short_seller,
                net_short_pct=net_pct_raw,
                net_short_pct_num=pct_num,
                position_date=date_raw,
                position_date_iso=iso,
                source_url=AFM_SHORTPOS_URL,
                unique_id=uid,
            )
        )
    logger.info("HTML: rows seen=%d; kept=%d", seen, len(out))
    return out


# ------------------------- CSV fallback -------------------------

def _find_download_link(soup: BeautifulSoup) -> Optional[str]:
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = _clean_text(a.get_text()).lower()
        if ".csv" in href.lower() or "csv" in label or "download" in label:
            if href.startswith("/"):
                return "https://www.afm.nl" + href
            return href
    return None

def _decode_best(content: bytes) -> str:
    for enc in ("utf-8", "utf-16", "cp1252", "latin-1"):
        try:
            return content.decode(enc)
        except Exception:
            continue
    return content.decode("utf-8", errors="ignore")

def _parse_csv(text: str) -> List[ShortPosition]:
    out: List[ShortPosition] = []
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=";,\t")
        delim = dialect.delimiter
    except Exception:
        delim = ";"

    reader = csv.DictReader(io.StringIO(text), delimiter=delim)

    def get(row: Dict, keys: List[str]) -> str:
        for k in keys:
            if k in row and row[k]:
                return _clean_text(row[k])
        lower = {k.lower(): v for k, v in row.items()}
        for k in keys:
            if k.lower() in lower and lower[k.lower()]:
                return _clean_text(lower[k.lower()])
        return ""

    SE_ISSUER   = ["Uitgevende instelling", "Issuer", "Uitgevende"]
    SE_ISIN     = ["ISIN"]
    SE_HOLDER   = ["Partij", "Houder", "Short seller", "Meldingsplichtige"]
    SE_PERCENT  = ["Netto shortpositie", "Net short position", "Netto-shortpositie", "%", "Net short %"]
    SE_DATE     = ["Datum", "Date", "Datum melding", "Meldingsdatum"]

    seen = 0
    for row in reader:
        seen += 1
        issuer = get(row, SE_ISSUER)
        issuer_isin = get(row, SE_ISIN) or None
        short_seller = get(row, SE_HOLDER)
        net_pct_raw = get(row, SE_PERCENT)
        date_raw = get(row, SE_DATE)

        if not issuer or not short_seller or not net_pct_raw:
            continue
        if not _row_ok(issuer, issuer_isin):
            continue

        pct_num = _pct_to_float(net_pct_raw)
        date_raw, iso = _parse_date(date_raw or "")
        uid = _make_uid(issuer, short_seller, iso, net_pct_raw)

        out.append(
            ShortPosition(
                issuer=issuer,
                issuer_isin=issuer_isin,
                short_seller=short_seller,
                net_short_pct=net_pct_raw,
                net_short_pct_num=pct_num,
                position_date=date_raw,
                position_date_iso=iso,
                source_url=AFM_SHORTPOS_URL,
                unique_id=uid,
            )
        )
    logger.info("CSV: rows seen=%d; kept=%d", seen, len(out))
    return out


# ------------------------- main scrape -------------------------

def scrape_short_positions() -> List[ShortPosition]:
    logger.info("Fetching AFM page (filter_enabled=%s): %s", USE_FILTER, AFM_SHORTPOS_URL)
    resp = requests.get(
        AFM_SHORTPOS_URL,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)",
            "Accept-Language": "nl,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml",
        },
        timeout=30,
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) Try HTML table
    tbl = _pick_best_table(soup)
    if tbl:
        items = _parse_from_table(tbl)
        if items:
            return items

    # 2) CSV fallback
    csv_url = _find_download_link(soup)
    if csv_url:
        logger.info("Attempting CSV download: %s", csv_url)
        csv_resp = requests.get(
            csv_url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)"},
        )
        csv_resp.raise_for_status()
        text = _decode_best(csv_resp.content)
        items = _parse_csv(text)
        if items:
            return items

    logger.warning("AFM short positions: found 0 items.")
    return []


# ------------------------- public API (compat) -------------------------

def fetch_items() -> List[Dict]:
    return [sp.to_dict() for sp in scrape_short_positions()]

def fetch_afm_table() -> List[Dict]:
    """Backward-compatible alias for existing main.py."""
    return fetch_items()
