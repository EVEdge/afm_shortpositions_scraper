# article_builder.py

from typing import Dict, Optional

AFM_SOURCE_LABEL = "klik hier"

def _pct_nl(num: Optional[float], fallback: str = "") -> str:
    """
    Format percentage with Dutch comma separator.
    - If num is provided: '1,20%'
    - Else use fallback string as-is (and ensure it ends with %)
    """
    if num is not None:
        try:
            return f"{float(num):.2f}%".replace(".", ",")
        except Exception:
            pass
    s = (fallback or "").strip()
    if not s:
        return ""
    # normalize decimal separator in fallback to comma
    s = s.replace(".", ",")
    return s if s.endswith("%") else s + "%"

def _nl_title(issuer: str, short_seller: str, pct_str: str) -> str:
    # Example: "Voleon Capital Management LP meldt 1,2% shortpositie in Alfen N.V."
    # Use one decimal in title for readability (e.g. 1,2%)
    try:
        # try to derive one-decimal string from pct_str
        raw = pct_str.replace("%", "").replace(",", ".")
        one = f"{float(raw):.1f}%".replace(".", ",")
    except Exception:
        one = pct_str or ""
    return f"{short_seller} meldt {one} shortpositie in {issuer}."

def _excerpt_nl(issuer: str, short_seller: str, pct_str: str, date_iso: str) -> str:
    return (
        f"{short_seller} heeft een netto shortpositie van {pct_str} in {issuer}. "
        f"Gebaseerd op het actuele AFM-register (datum: {date_iso})."
    )

def _content_nl(item: Dict) -> str:
    issuer        = item.get("issuer") or item.get("emittent") or ""
    isin          = item.get("issuer_isin") or ""
    short_seller  = item.get("short_seller") or item.get("melder") or ""
    pct_num       = item.get("net_short_pct_num")
    pct_raw       = item.get("net_short_pct") or ""
    pct_str       = _pct_nl(pct_num, pct_raw)
    date_iso      = item.get("position_date_iso") or item.get("position_date") or item.get("meldingsdatum") or ""
    source_url    = item.get("source_url") or "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

    parts: list[str] = []
    parts.append("<h3>Overzicht van shortpositie</h3>")
    parts.append("<ul>")
    parts.append(f"<li><strong>Aandeel:</strong> {issuer}</li>")
    parts.append(f"<li><strong>Short seller:</strong> {short_seller}</li>")
    # Example shows 'Meldingsdatum' twice; we include both labels using the same date for parity
    if date_iso:
        parts.append(f"<li><strong>Meldingsdatum:</strong> {date_iso}</li>")
    parts.append(f"<li><strong>Positie:</strong> {pct_str}</li>")
    if isin:
        parts.append(f"<li><strong>ISIN:</strong> {isin}</li>")
    if date_iso:
        parts.append(f"<li><strong>Meldingsdatum:</strong> {date_iso}</li>")
    parts.append("</ul>")

    parts.append("<h3>Disclaimer</h3>")
    parts.append(
        "<p>Deze publicatie is informatief en vormt geen beleggingsadvies. "
        "De informatie op deze pagina is gebaseerd op het AFM-register voor nettoshortposities. "
        "De publicaties van het register zijn openbaar en bereikbaar via de AFM-website: "
        f'<a href="{source_url}" target="_blank" rel="nofollow noopener">{AFM_SOURCE_LABEL}</a>. '
        "Pennywatch.nl is niet gelieerd aan de Autoriteit Financiële Markten (AFM). "
        "Pennywatch.nl geeft geen garanties over de juistheid of volledigheid van de informatie.</p>"
    )

    return "\n".join(parts)

def build_article(item: Dict, *, category_id: int | None = None) -> Dict:
    issuer        = item.get("issuer") or item.get("emittent") or ""
    short_seller  = item.get("short_seller") or item.get("melder") or ""
    pct_num       = item.get("net_short_pct_num")
    pct_raw       = item.get("net_short_pct") or ""
    pct_str       = _pct_nl(pct_num, pct_raw)
    date_iso      = item.get("position_date_iso") or item.get("position_date") or item.get("meldingsdatum") or ""

    title   = _nl_title(issuer, short_seller, pct_str)
    excerpt = _excerpt_nl(issuer, short_seller, pct_str, date_iso)
    content = _content_nl(item)

    payload: Dict = {
        "title": title,
        "status": "publish",
        "excerpt": excerpt,
        "content": content,
        # Let publisher resolve tag names to IDs
        "tags": list(filter(None, {issuer, short_seller})),
        "meta": {
            "afm_unique_id": item.get("unique_id") or item.get("afm_key"),
            "afm_date": date_iso,
            "type": "shortpositie",
        },
    }
    if category_id is not None:
        payload["categories"] = [int(category_id)]
    return payload

# Backward-compat alias if something calls build_post()
def build_post(item: Dict, *, category_id: int | None = None) -> Dict:
    return build_article(item, category_id=category_id)
