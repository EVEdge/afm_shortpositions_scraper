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

# ---- Debug toggles ----
BYPASS_FILTER = os.getenv("PENNYWATCH_BYPASS_FILTER", "0").lower() in {"1", "true", "yes"}
LOG_LEVEL = os.getenv("PW_LOG_LEVEL", "INFO").upper()

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL or "INFO")
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL or "INFO")
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(ch)


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
    # try to normalize dd-mm-yyyy inside text
    m = re.search(r"(\d{2})[-/](\d{2})[-/](\d{4})", raw)
    if m:
        iso = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return raw, iso
    # or yyyy-mm-dd
    m = re.search(r"(\d{4})[-/](\d{2})[-/](\d{2})", raw)
    if m:
        iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return raw, iso
    return raw, raw


def _make_uid(issuer: str, short_seller: str, iso_date: str, pct: str) -> str:
    base = f"{issuer}|{short_seller}|{iso_date}|{pct}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


# ------------------------- HTML flow -------------------------

def _all_tables(soup: BeautifulSoup) -> List[BeautifulSoup]:
    tables = soup.find_all("table")
    logger.debug("Found %d <table> elements on page.", len(tables))
    return tables


def _table_headers(tbl: BeautifulSoup) -> List[str]:
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
    return heads


def _iter_rows(tbl: BeautifulSoup) -> Iterable[List[BeautifulSoup]]:
    tbody = tbl.find("tbody") or tbl
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if tds:
            yield tds


def _score_table(headers_lower: List[str]) -> int:
    """
    Rank tables by how likely they are the short-positions table.
    """
    keys = ["netto", "short", "positie", "shortpositie", "partij", "datum", "issuer", "uitgevende"]
    score = sum(1 for h in headers_lower for k in keys if k in h)
    return score


def _pick_best_table(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    best_tbl = None
    best_score = -1
    for tbl in _all_tables(soup):
        heads = [h.lower() for h in _table_headers(tbl)]
        score = _score_table(heads)
        logger.debug("Table headers: %s | score=%d", heads, score)
        if score > best_score:
            best_tbl, best_score = tbl, score
    if best_tbl:
        logger.info("Using table with score %d", best_score)
    return best_tbl


def _header_map(tbl: BeautifulSoup) -> Dict[str, int]:
    """
    Try to map columns by header text; if that fails, fall back to heuristics on the first row.
    """
    idx: Dict[str, int] = {}
    headers = _table_headers(tbl)
    headers_lower = [h.lower() for h in headers]
    logger.debug("Detected headers: %s", headers_lower)

    for i, txt in enumerate(headers_lower):
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

    # Heuristic fallback using first data row
    first = next(_iter_rows(tbl), None)
    if not first:
        return idx

    texts = [_clean_text(td.get_text()) for td in first]
    logger.debug("First-row texts for heuristic mapping: %s", texts)

    # percent column: contains a %
    for i, t in enumerate(texts):
        if re.search(r"\d[\d\.,]*\s*%", t):
            idx.setdefault("net_short_pct", i)

    # date column: contains a typical date pattern
    for i, t in enumerate(texts):
        if re.search(r"\d{2}[-/]\d{2}[-/]\d{2,4}|\d{4}[-/]\d{2}[-/]\d{2}", t):
            idx.setdefault("date", i)

    # issuer: often first column
    if "issuer" not in idx and texts:
        idx["issuer"] = 0

    # short_seller: pick the longest non-pct/date text that isn't issuer
    if "short_seller" not in idx:
        candidates = []
        for i, t in enumerate(texts):
            if i == idx.get("issuer"):
                continue
            if i == idx.get("net_short_pct"):
                continue
            if i == idx.get("date"):
                continue
            candidates.append((len(t), i))
        if candidates:
            _, i = max(candidates)
            idx["short_seller"] = i

    logger.info("Header map (with heuristics): %s", idx)
    return idx


def _parse_from_table(tbl: BeautifulSoup) -> List[ShortPosition]:
    hmap = _header_map(tbl)
    results: List[ShortPosition] = []
    seen = 0

    for tds in _iter_rows(tbl):
        seen += 1

        def pick(key: str) -> str:
            i = hmap.get(key, -1)
            if i >= 0 and i < len(tds):
                return _clean_text(tds[i].get_text())
            return ""

        issuer = pick("issuer")
        issuer_isin = pick("isin") or None
        short_seller = pick("short_seller")
        net_pct_raw = pick("net_short_pct")
        date_raw = pick("date")

        # Fallbacks if heuristics missed:
        if not net_pct_raw:
            # find a td with % in it
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

    logger.info("HTML: rows seen=%d; kept=%d", seen, len(results))
    return results


# ------------------------- CSV fallback -------------------------

def _find_download_link(soup: BeautifulSoup) -> Optional[str]:
    """
    Find a CSV/Download link on the page.
    """
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = _clean_text(a.get_text()).lower()
        if ".csv" in href.lower() or "csv" in label or "download" in label:
            if href.startswith("/"):
                return "https://www.afm.nl" + href
            return href
    return None


def _parse_csv(text: str) -> List[ShortPosition]:
    results: List[ShortPosition] = []
    try:
        dialect = csv.Sniffer().sniff(text[:2048], delimiters=";,\t")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ";"

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    def get(row: Dict, keys: List[str]) -> str:
        for k in keys:
            if k in row and row[k]:
                return _clean_text(row[k])
        lower = {k.lower(): v for k, v in row.items()}
        for k in keys:
            if k.lower() in lower and lower[k.lower()]:
                return _clean_text(lower[k.lower()])
        return ""

    seen = 0
    for row in reader:
        seen += 1
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

    logger.info("CSV: rows seen=%d; kept=%d", seen, len(results))
    return results


# ------------------------- main scrape -------------------------

def scrape_short_positions() -> List[ShortPosition]:
    logger.info("Fetching AFM page (bypass_filter=%s): %s", BYPASS_FILTER, AFM_SHORTPOS_URL)
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

    # 1) HTML table
    tbl = _pick_best_table(soup)
    if tbl:
        items = _parse_from_table(tbl)
        if items:
            return items

    # 2) CSV fallback
    logger.info("HTML path produced 0 items; trying CSV fallback…")
    csv_url = _find_download_link(soup)
    if csv_url:
        logger.info("Attempting CSV download: %s", csv_url)
        try:
            csv_resp = requests.get(
                csv_url,
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0 (compatible; PennywatchScraper/1.0)"},
            )
            csv_resp.raise_for_status()
            # decode with best guess
            text = csv_resp.content.decode("utf-8", errors="ignore")
            items = _parse_csv(text)
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
