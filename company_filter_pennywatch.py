"""
Filtering for Pennywatch content.

For SHORT POSITIONS we want to publish broadly. Therefore, the default here is:
- Allow everything (return True).
- Optional deny/allow lists can be applied via environment variables.

Env:
  SHORTPOS_USE_FILTER   -> "1" to enable basic filtering (default: "0" = disabled)
  SHORTPOS_ALLOW_ISINS  -> comma-separated ISINs to allow (case-insensitive)
  SHORTPOS_ALLOW_ISSUERS-> comma-separated issuer name fragments to allow (lowercased match)
  SHORTPOS_DENY_ISSUERS -> comma-separated issuer name fragments to block (lowercased match)
"""

import os

USE_FILTER = os.getenv("SHORTPOS_USE_FILTER", "0").strip().lower() in {"1", "true", "yes"}

def _csv_set(env_key: str):
    raw = os.getenv(env_key, "")
    vals = [v.strip() for v in raw.split(",") if v.strip()]
    return set(vals)

ALLOW_ISINS      = {v.upper() for v in _csv_set("SHORTPOS_ALLOW_ISINS")}
ALLOW_ISSUERS    = {v.lower() for v in _csv_set("SHORTPOS_ALLOW_ISSUERS")}
DENY_ISSUERS     = {v.lower() for v in _csv_set("SHORTPOS_DENY_ISSUERS")}

def _match_fragment(hay: str, needles: set[str]) -> bool:
    h = (hay or "").lower()
    return any(n in h for n in needles)

def is_approved_company(issuer_name: str | None, issuer_isin: str | None = None) -> bool:
    """
    Default: allow all (so short positions aren’t accidentally suppressed).
    If SHORTPOS_USE_FILTER=1, apply simple allow/deny lists.
    """
    if not USE_FILTER:
        return True

    name_ok = True
    if DENY_ISSUERS and _match_fragment(issuer_name or "", DENY_ISSUERS):
        name_ok = False
    if ALLOW_ISSUERS:
        name_ok = name_ok and _match_fragment(issuer_name or "", ALLOW_ISSUERS)

    isin_ok = True
    if ALLOW_ISINS:
        isin_ok = (issuer_isin or "").upper() in ALLOW_ISINS

    return bool(name_ok and isin_ok)
