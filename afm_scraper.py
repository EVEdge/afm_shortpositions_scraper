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

from company_filter_pennywatch import is_approved_company

AFM_SHORTPOS_URL = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

# Set PENNYWATCH_BYPASS_FILTER=1 to ignore the company filter during debugging
BYPASS_FILTER = os.getenv("PENNYWATCH_BYPASS_FILTER", "0") in {"1", "true", "True"}

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
    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", raw)
    if m:
        iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return raw, iso
    return raw, raw


def _make_uid(issuer: str, short_seller: str, iso_date: str, pct: str) -> str:
    base = f"{issuer}|{short_seller}|{iso_date}|{pct}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


# ------------------------- HTML flow -------------------------

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
            logger.info("Found AFM table with headers: %s", lower_heads)
            return t

    logger.info("Falling back to first table; headers unknown.")
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

    logger.info("Header map: %s", map_idx)
    return map_idx


def _iter_rows(table: BeautifulSoup) -> Iterable[List[BeautifulSoup]]:
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        yield tds


def _parse_rows_from_table(table: BeautifulSoup) -> List[ShortPosition]:
    hmap = _header_map(table)
    results: List[ShortPosition] = []
    raw_count = 0

    for tds in _iter_rows(table):
        raw_count += 1

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

        if not (BYPASS_FILTER or is_approved_company(issuer, issuer_isin)):
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

    logger.info("HTML table rows seen: %d; kept after filtering: %d", raw_count, len(results))
    return results


# ------------------------- CSV fallback -------------------------

def _try_csv_from_page(soup: BeautifulSoup) -> Optional[str]:
    """
    Look for a CSV download link on the page.
    We consider <a> tags whose href contains 'csv' or endswith '.csv'.
    """
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = _clean_text(a.get_text()).lower()
        if ".csv" in href.lower() or "csv" in label:
            # Make absolute
            if href.startswith("/"):
                return "https://www.afm.nl" + href
            return href
    return None


def _parse_csv(text: str) -> List[ShortPosition]:
    results: List[ShortPosition] = []

    # Try to sniff delimiter (AFM often uses semicolon)
    sample = text[:1024]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";"

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    raw_count = 0

    # Normalize common Dutch/English headers
    def get(row: Dict, keys: List[str]) -> str:
        for k in keys:
            if k in row and row[k]:
                return _clean_text(row[k])
        # try case-insensitive
        lower = {k.lower(): v for k, v in row.items()}
        for k in keys:
            if k.lower() in lower and lower[k.lower()]:
                return _clean_text(lower[k.lower()])
        return ""

    for row in reader:
        raw_count += 1
        issuer = get(row, ["Uitgevende instelling", "Issuer", "Uitgevende"])
        issuer_isin = get(row, ["ISIN"])
        short_seller = get(row, ["Partij", "Houder", "Short seller", "Meldingsplichtige"])
        net_pct_raw = get(row, ["Netto shortpositie", "Net short position", "Netto-shortpositie", "%"])
        date_raw = get(row, ["Datum", "Date"])

        if not issuer or not short_seller or not net_pct_raw:
            continue

        if not (BYPASS_FILTER or is_approved_company(issuer, issuer_isin or None)):
            continue

        pct_num = _pct_to_float(net_pct_raw)
        date_raw, iso = _parse_date(date_raw or "")
        uid = _make_uid(issuer, short_seller, iso, net_pct_raw)

        results.append(
            ShortPosition(
                issuer=issuer,
                issuer_isin=issuer_isin or None,
                short_seller=short_seller,
                net_short_pct=net_pct_raw,
                net_short_pct_num=pct_num,
                position_date=date_raw,
                position_date_iso=iso,
                source_url=AFM_SHORTPOS_URL,
                unique_id=uid,
            )
        )

    logger.info("CSV rows seen: %d; kept after filtering: %d", raw_count, len(results))
    return results


# ------------------------- main scrape -------------------------

def scrape_short_positions() -> List[ShortPosition]:
    logger.info("Fetching AFM page: %s", AFM_SHORTPOS_URL)
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

    # 1) Try HTML table first
    table = _find_table(soup)
    if table:
        items = _parse_rows_from_table(table)
        if items:
            return items

    logger.info("No items from HTML table, trying CSV fallback…")
    csv_url = _try_csv_from_page(soup)
    if csv_url:
        try:
            csv_resp = requests.get(csv_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            csv_resp.raise_for_status()
            items = _parse_csv(csv_resp.text)
            if items:
                return items
        except Exception as e:
            logger.warning("CSV fallback failed: %s", e)

    logger.warning("AFM short positions: found 0 items.")
    return []


# ------------------------- public API (compat) -------------------------

def fetch_items() -> List[Dict]:
    return [sp.to_dict() for sp in scrape_short_positions()]

def fetch_afm_table() -> List[Dict]:
    """Deprecated alias kept for backward compatibility with main.py."""
    return fetch_items()
