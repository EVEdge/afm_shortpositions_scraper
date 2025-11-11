import os

USE_FILTER = os.getenv("SHORTPOS_USE_FILTER", "0").strip().lower() in {"1", "true", "yes"}

def _csv(envkey: str) -> set[str]:
    raw = os.getenv(envkey, "")
    return {x.strip() for x in raw.split(",") if x.strip()}

ALLOW_ISINS   = {v.upper() for v in _csv("SHORTPOS_ALLOW_ISINS")}
ALLOW_ISSUERS = {v.lower() for v in _csv("SHORTPOS_ALLOW_ISSUERS")}
DENY_ISSUERS  = {v.lower() for v in _csv("SHORTPOS_DENY_ISSUERS")}

def _has(hay: str, needles: set[str]) -> bool:
    h = (hay or "").lower()
    return any(n in h for n in needles)

def is_approved_company(issuer_name: str | None, issuer_isin: str | None = None) -> bool:
    if not USE_FILTER:
        return True
    if issuer_name and _has(issuer_name, DENY_ISSUERS):
        return False
    ok_name = True if not ALLOW_ISSUERS else _has(issuer_name or "", ALLOW_ISSUERS)
    ok_isin = True if not ALLOW_ISINS else (issuer_isin or "").upper() in ALLOW_ISINS
    return ok_name and ok_isin
