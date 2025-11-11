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

from company_filter_pennywatch import is_approved_company

AFM_SHORTPOS_URL = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

logger = logging.getLogger(__name__)
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


# ------------------------- helpers -------------------------

def _clean_text(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())


def _pct_to_float(p: str) -> float:
    if not p:
        return 0.0
    s = p.replace("%", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_date(d: str) -> Tuple[str, str]:
    raw = _clean_text(d)
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            iso = datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            return raw, iso
        except ValueError:
            continue
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

    # Heuristic fallback
    if {"issuer", "short_seller", "net_short_pct"} <= idx.keys():
        return idx
    first = next(_iter_rows(tbl), None)
    if not first:
        return idx
    texts = [_clean_text(td.get_text()) for td in first]

    # % column
    for i, t in enumerate(texts):
        if re.search(r"\d[\d\.,]*\s*%", t):
            idx.setdefault("net_short_pct", i)
    # date column
    for i, t in enumerate(texts):
        if re.search(r"\d{2}[-/]\d{2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2}"
