"""Exact Pennywatch smallcap allowlist, effective 2026-11-16.

A security enters the Pennywatch feed only when its AFM issuer matches one
of these explicitly approved names. Market cap is deliberately irrelevant.
Never match by substring or assume a ticker from the issuer's ISIN.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, Iterable, List, Optional

# Keep all 37 requested securities registered, but activate only the 35 .AS listings.
# Fagron and Vastned are deferred until FSMA short filings are integrated.
APPROVED = {
    "BAMNB.AS": ("Koninklijke BAM Groep", "BAM Groep"),
    "VLK.AS": ("Van Lanschot Kempen",),
    "APAM.AS": ("Aperam",),
    "HEIJM.AS": ("Koninklijke Heijmans", "Heijmans"),
    "THEON.AS": ("Theon International",),
    "LKFT.AS": ("Lakefront Biotherapeutics",),
    "BFIT.AS": ("Basic-Fit", "Basic Fit"),
    "TWEKA.AS": ("TKH Group",),
    "LIGHT.AS": ("Signify",),
    "HAVAS.AS": ("Havas",),
    "FAGR.BR": ("Fagron",),
    "ECMPA.AS": ("Eurocommercial Properties",),
    "CRBN.AS": ("Corbion",),
    "FAST.AS": ("Fastned",),
    "FLOW.AS": ("Flow Traders",),
    "FUR.AS": ("Fugro",),
    "OCI.AS": ("OCI",),
    "WHA.AS": ("Wereldhave",),
    "PHARM.AS": ("Pharming Group",),
    "AMG.AS": ("AMG Critical Materials",),
    "FERGR.AS": ("Ferrari Group",),
    "ACOMO.AS": ("Acomo",),
    "NEDAP.AS": ("Nedap",),
    "TRIO.AS": ("Triodos Bank",),
    "FFARM.AS": ("ForFarmers",),
    "VASTB.BR": ("Vastned", "Vastned Retail"),
    "TOM2.AS": ("TomTom",),
    "PNL.AS": ("PostNL",),
    "SLIGR.AS": ("Sligro Food Group",),
    "BRNL.AS": ("Brunel International",),
    "NSI.AS": ("NSI",),
    "KENDR.AS": ("Kendrion",),
    "ALFEN.AS": ("Alfen",),
    "CMCOM.AS": ("CM.com", "CM Com"),
    "AXS.AS": ("Accsys Technologies",),
    "AVTX.AS": ("Avantium",),
    "SIFG.AS": ("Sif Holding",),
}
assert len(APPROVED) == 37
ACTIVE_TICKERS = frozenset(ticker for ticker in APPROVED if ticker.endswith('.AS'))
assert len(ACTIVE_TICKERS) == 35


def _normalize(name: str) -> str:
    ascii_name = unicodedata.normalize("NFKD", str(name or ""))
    ascii_name = ascii_name.encode("ascii", "ignore").decode("ascii").lower()
    ascii_name = ascii_name.replace("&", " and ")
    result = re.sub(r"[^a-z0-9]+", " ", ascii_name)
    result = re.sub(r"\b(n v|nv|b v|bv|s a|sa|plc|ltd)\s*$", "", result)
    return " ".join(result.split())


ALIASES: Dict[str, str] = {}
for _ticker, _names in APPROVED.items():
    for _name in _names:
        _key = _normalize(_name)
        if _key in ALIASES and ALIASES[_key] != _ticker:
            raise RuntimeError("Ambiguous Pennywatch smallcap name: " + _key)
        ALIASES[_key] = _ticker


def ticker_for_record(record: dict) -> Optional[str]:
    """Return the allowlisted exchange ticker or None (never fuzzy-match)."""
    name = record.get("issuer") or record.get("emittent") or ""
    return ALIASES.get(_normalize(name))


def filter_pennywatch_records(records: Iterable[dict]) -> List[dict]:
    """Preserve original fields/UIDs; record-level filter is fail-closed."""
    return [dict(row) for row in records if ticker_for_record(row) in ACTIVE_TICKERS]
