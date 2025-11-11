from company_filter_pennywatch import is_approved_company
import re
import hashlib
import time
import logging
from typing import Tuple, Optional, List, Dict
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup, Tag

from config import AFM_URL

BASE = "https://www.afm.nl"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
}
TIMEOUT = 25
RETRIES = 2

# ---------- helpers ----------

def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _norm_pct(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%?", str(s))
    if not m:
        return None
    try:
        return f"{float(m.group(1).replace(',', '.')):.2f}%"
    except Exception:
        return None

def _request(url: str) -> requests.Response:
    last_exc = None
    for i in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 403:
                time.sleep(0.7)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(0.7)
    raise last_exc

# ---------- detail parsing ----------

LABEL_PAT_KAP = re.compile(r"\bkapitaalbelang\b|\bkapitaal\b|\bcapital\b", re.I)
LABEL_PAT_STEM = re.compile(r"\bstemrecht\b|\bstemrechten\b|\bvoting\b", re.I)

HEADER_PAT_TOTAL = re.compile(r"\btotale? deelneming\b|\btotal holding\b", re.I)
HEADER_PAT_PREV  = re.compile(r"\bvoorheen\b|\bvorige\b|\bprevious\b", re.I)

def _row_texts(tr: Tag) -> List[str]:
    return [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]

def _extract_from_detail_html(html: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    kap = stem = prev_kap = prev_stem = None

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue

        header_cells = None
        for tr in trs:
            ths = tr.find_all("th")
            if ths:
                header_cells = [th.get_text(" ", strip=True) for th in ths]
                header_norms = [_norm_text(h) for h in header_cells]
                break
        if not header_cells:
            continue

        total_idx = None
        prev_idx = None
        for idx, hn in enumerate(header_norms):
            if HEADER_PAT_TOTAL.search(hn):
                total_idx = idx
            if HEADER_PAT_PREV.search(hn):
                prev_idx = idx

        if total_idx is None:
            continue

        for tr in trs:
            tds = tr.find_all("td")
            if not tds:
                continue
            cells = [td.get_text(" ", strip=True) for td in tds]
            if not cells:
                continue
            label = _norm_text(cells[0])

            if LABEL_PAT_KAP.search(label):
                if total_idx < len(cells):
                    kap = _norm_pct(cells[total_idx]) or kap
                if prev_idx is not None and prev_idx < len(cells):
                    prev_kap = _norm_pct(cells[prev_idx]) or prev_kap

            if LABEL_PAT_STEM.search(label):
                if total_idx < len(cells):
                    stem = _norm_pct(cells[total_idx]) or stem
                if prev_idx is not None and prev_idx < len(cells):
                    prev_stem = _norm_pct(cells[prev_idx]) or prev_stem

        if kap or stem:
            break

    if not kap and not stem:
        for table in soup.find_all("table"):
            trs = table.find_all("tr")
            if not trs:
                continue

            headers = []
            for tr in trs:
                ths = tr.find_all("th")
                if ths:
                    headers = [_norm_text(th.get_text(" ", strip=True)) for th in ths]
                    break

            for tr in trs:
                cells = _row_texts(tr)
                if len(cells) < 2:
                    continue
                label = _norm_text(cells[0])

                if LABEL_PAT_KAP.search(label):
                    val = None
                    if headers:
                        for idx, h in enumerate(headers):
                            if HEADER_PAT_TOTAL.search(h) and idx < len(cells):
                                val = cells[idx]
                                break
                    if not val:
                        for c in cells[1:]:
                            if _norm_pct(c):
                                val = c
                                break
                    kap = _norm_pct(val) or kap

                if LABEL_PAT_STEM.search(label):
                    val = None
                    if headers:
                        for idx, h in enumerate(headers):
                            if HEADER_PAT_TOTAL.search(h) and idx < len(cells):
                                val = cells[idx]
                                break
                    if not val:
                        for c in cells[1:]:
                            if _norm_pct(c):
                                val = c
                                break
                    stem = _norm_pct(val) or stem

            if kap or stem:
                break

    if not kap or not stem:
        full = soup.get_text(" ", strip=True).lower()
        if not kap:
            m = re.search(r"(kapitaal\w*|capital\w*|total holding|totale deelneming|belang).{0,24}(\d+(?:[.,]\d+)?)\s*%", full)
            kap = _norm_pct(m.group(2)) if m else kap
        if not stem:
            m = re.search(r"(stemrecht\w*|voting\w*).{0,24}(\d+(?:[.,]\d+)?)\s*%", full)
            stem = _norm_pct(m.group(2)) if m else stem

    return kap, stem, prev_kap, prev_stem

# ---------- list page ----------

def _collect_detail_links_and_df(list_html: str) -> Tuple[List[Optional[str]], List[Dict[str, str]]]:
    soup = BeautifulSoup(list_html, "lxml")
    table = soup.find("table")
    if not table:
        return [], []

    detail_hrefs = []
    for tr in table.find_all("tr"):
        td = tr.find("td")
        a = td.find("a", href=True) if td else None
        detail_hrefs.append(a["href"] if a else None)

    headers = []
    for tr in table.find_all("tr"):
        ths = tr.find_all("th")
        if ths:
            headers = [th.get_text(" ", strip=True) for th in ths]
            break

    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        cells = [td.get_text(" ", strip=True) for td in tds]
        row = {}
        for idx, val in enumerate(cells):
            key = headers[idx] if idx < len(headers) else f"col_{idx}"
            row[key] = val

        emittent = row.get("Uitgevende instelling") or row.get("Issuer") or ""
        melder   = row.get("Meldingsplichtige") or row.get("Shareholder") or ""
        datum    = row.get("Datum meldingsplicht") or row.get("Datum melding") or row.get("Date") or ""
        rows.append({"emittent": emittent.strip(), "melder": melder.strip(), "datum": datum.strip()})

    if len(detail_hrefs) == len(rows) + 1:
        detail_hrefs = detail_hrefs[1:]

    return detail_hrefs, rows

# ---------- main scraper ----------

def fetch_afm_table():
    r = _request(AFM_URL)
    if r.status_code == 403:
        print("[AFM] 403 Forbidden op lijstpagina.")
        return []

    detail_hrefs, rows = _collect_detail_links_and_df(r.text)
    if not rows:
        print("[AFM] Geen rijen gevonden op de lijstpagina.")
        return []

    results = []
    for i, row in enumerate(rows):
        emittent = row.get("emittent", "")
        melder   = row.get("melder", "")
        datum    = row.get("datum", "")

        # ✅ Filter by approved companies
        if not is_approved_company(emittent):
            logging.info(f"⏭️ Skipped: {emittent} not in approved list.")
            continue

        href = detail_hrefs[i] if i < len(detail_hrefs) else None
        detail_url = urljoin(BASE, href) if href else None

        kap = stem = prev_kap = prev_stem = None
        if detail_url:
            try:
                rd = _request(detail_url)
                kap, stem, prev_kap, prev_stem = _extract_from_detail_html(rd.text)
            except Exception as e:
                print(f"[AFM] detail fetch/parse failed: {detail_url} ({e})")

        key_src = f"{emittent}|{melder}|{datum}|{kap or ''}|{stem or ''}"
        afm_key = hashlib.sha1(key_src.encode("utf-8")).hexdigest()

        results.append({
            "afm_key": afm_key,
            "emittent": emittent,
            "melder": melder,
            "meldingsdatum": datum,
            "kapitaal_pct": kap,
            "stem_pct": stem,
            "prev_kapitaal_pct": prev_kap,
            "prev_stem_pct": prev_stem,
            "detail_url": detail_url,
        })

    return results
