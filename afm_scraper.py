import hashlib
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup

from company_filter_pennywatch import is_approved_company

AFM_SHORTPOS_URL = (
    "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class ShortPosition:
    issuer: str                      # Uitgevende instelling
    issuer_isin: Optional[str]       # ISIN if present
    short_seller: str                # Shorting party
    net_short_pct: str               # Raw string, e.g. "0,60%"
    net_short_pct_num: float         # Numeric, e.g. 0.60
    position_date: str               # Raw date string
    position_date_iso: str           # YYYY-MM-DD
    source_url: str                  # Page scraped
    unique_id: str                   # Stable UID (issuer+shorter+date+pct)

    def to_dict(self) -> Dict:
        return asdict(self)


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


def _parse_date(d: str) -> (str, str):
    raw = _clean_text(d)
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            iso = datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            return raw, iso
        except ValueError:
            continue
    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", raw)
    if m:
        iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return raw, iso
    return raw, raw


def _make_uid(issuer: str, short_seller: str, iso_date: str, pct: str) -> str:
    base = f"{issuer}|{short_seller}|{iso_date}|{pct}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def _find_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    tables = soup.find_all("table")
    if not tables:
        return None

    def headers_of(tbl: BeautifulSoup) -> List[str]:
        heads = []
        thead = tbl.find("thead")
        if thead:
            for th in thead.find_all("th"):
                heads.append(_clean_text(th.get_text()))
        else:
            first_row = tbl.find("tr")
            if first_row:
                for th in first_row.find_all(["th", "td"]):
                    heads.append(_clean_text(th.get_text()))
        return [h.lower() for h in heads]

    for t in tables:
        lower_heads = headers_of(t)
        if any("netto" in h and "short" in h for h in lower_heads) or any(
            "shortpositie" in h for h in lower_heads
        ):
            return t
    return tables[0]


def _header_map(table: BeautifulSoup) -> Dict[str, int]:
    map_idx: Dict[str, int] = {}
    thead = table.find("thead")
    headers = []
    if thead:
        headers = thead.find_all("th")
    else:
        first_tr = table.find("tr")
        headers = first_tr.find_all(["th", "td"]) if first_tr else []

    for i, th in enumerate(headers):
        txt = _clean_text(th.get_text()).lower()
        if any(k in txt for k in ["uitgevende instelling", "issuer", "uitgevende"]):
            map_idx["issuer"] = i
        if "isin" in txt:
            map_idx["isin"] = i
        if any(k in txt for k in ["partij", "short", "meldingsplichtige", "houder"]):
            map_idx["short_seller"] = i
        if any(k in txt for k in ["netto short", "netto-short", "shortpositie", "%"]):
            map_idx["net_short_pct"] = i
        if any(k in txt for k in ["datum", "date"]):
            map_idx["date"] = i

    return map_idx


def _iter_rows(table: BeautifulSoup) -> Iterable[List[BeautifulSoup]]:
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        yield tds


def scrape_short_positions() -> List[ShortPosition]:
    logger.info("Fetching AFM short positions: %s", AFM_SHORTPOS_URL)
    resp = requests.get(
        AFM_SHORTPOS_URL,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)",
            "Accept-Language": "nl,en;q=0.9",
        },
        timeout=30,
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = _find_table(soup)
    if not table:
        logger.warning("No table found on AFM short positions page.")
        return []

    hmap = _header_map(table)
    results: List[ShortPosition] = []

    for tds in _iter_rows(table):
        def pick(idx_key: str) -> str:
            if idx_key not in hmap:
                return ""
            i = hmap[idx_key]
            if i < len(tds):
                return _clean_text(tds[i].get_text())
            return ""

        issuer = pick("issuer")
        issuer_isin = pick("isin") or None
        short_seller = pick("short_seller")
        net_pct_raw = pick("net_short_pct")
        date_raw = pick("date")

        if not issuer or not short_seller or not net_pct_raw:
            continue

        if not is_approved_company(issuer, issuer_isin):
            continue

        pct_num = _pct_to_float(net_pct_raw)
        date_raw, iso = _parse_date(date_raw or "")
        uid = _make_uid(issuer, short_seller, iso, net_pct_raw)

        results.append(
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

    logger.info("Parsed %d short positions (after filtering).", len(results))
    return results


# ---- Public API used by the rest of the pipeline --------------------------------

def fetch_items() -> List[Dict]:
    """Return list of dicts (compat with existing pipeline)."""
    return [sp.to_dict() for sp in scrape_short_positions()]

# Backward-compat alias for older main.py that still imports fetch_afm_table
def fetch_afm_table() -> List[Dict]:
    """Deprecated: use fetch_items(). Kept for backward compatibility."""
    return fetch_items()
