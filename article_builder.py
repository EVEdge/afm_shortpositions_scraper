from typing import Dict, Optional, List

AFM_SOURCE_LABEL = "klik hier"
AFM_SOURCE_URL_FALLBACK = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/netto-shortposities-actueel"

def _pct_nl(num: Optional[float], fallback: str = "") -> str:
    """
    Format percentage with Dutch comma separator.
    Always two decimals.
    """
    if num is not None:
        try:
            return f"{float(num):.2f}%".replace(".", ",")
        except Exception:
            pass
    s = (fallback or "").strip()
    if not s:
        return ""
    # normalize to comma and ensure it ends with %
    s = s.replace(".", ",")
    return s if s.endswith("%") else s + "%"

def _nl_title(issuer: str, short_seller: str, pct_str_two: str, direction: Optional[str]) -> str:
    # pct_str_two is already two decimals with comma
    if direction == "up":
        return f"{short_seller} vergroot shortpositie ({pct_str_two}) in {issuer}."
    if direction == "down":
        return f"{short_seller} verkleint shortpositie ({pct_str_two}) in {issuer}."
    return f"{short_seller} meldt {pct_str_two} shortpositie in {issuer}."

def _excerpt_nl(issuer: str, short_seller: str, pct_str: str, date_iso: str, prev_str: Optional[str], direction: Optional[str]) -> str:
    if prev_str:
        if direction == "up":
            trend = " Deze positie is verhoogd ten opzichte van de vorige melding."
        elif direction == "down":
            trend = " Deze positie is verlaagd ten opzichte van de vorige melding."
        else:
            trend = ""
        return (
            f"{short_seller} heeft een netto shortpositie van {pct_str} in {issuer}. "
            f"Vorige positie: {prev_str}. {('Datum: ' + date_iso) if date_iso else ''}{trend}"
        )
    return (
        f"{short_seller} heeft een netto shortpositie van {pct_str} in {issuer}. "
        f"Gebaseerd op het actuele AFM-register{(' (datum: ' + date_iso + ')') if date_iso else ''}."
    )

def _history_table(history: List[Dict]) -> str:
    """
    Build a simple HTML table with columns: Datum, Positie (max 10 rows).
    History is expected most recent first.
    """
    if not history:
        return ""
    rows = []
    rows.append('<table><thead><tr><th>Datum</th><th>Positie</th></tr></thead><tbody>')
    for h in history[:10]:
        date = h.get("date", "")
        pct  = h.get("pct") or _pct_nl(h.get("pct_num"))
        rows.append(f"<tr><td>{date}</td><td>{pct}</td></tr>")
    rows.append("</tbody></table>")
    return "\n".join(rows)

def _content_nl(item: Dict) -> str:
    issuer        = item.get("issuer") or item.get("emittent") or ""
    isin          = item.get("issuer_isin") or ""
    short_seller  = item.get("short_seller") or item.get("melder") or ""
    pct_num       = item.get("net_short_pct_num")
    pct_raw       = item.get("net_short_pct") or ""
    pct_str       = _pct_nl(pct_num, pct_raw)
    date_iso      = item.get("position_date_iso") or item.get("position_date") or item.get("meldingsdatum") or ""
    source_url    = item.get("source_url") or AFM_SOURCE_URL_FALLBACK

    prev_num      = item.get("prev_net_short_pct_num")
    prev_raw      = item.get("prev_net_short_pct") or ""
    prev_str      = _pct_nl(prev_num, prev_raw) if prev_num is not None else None
    history       = item.get("history") or []

    parts: list[str] = []
    parts.append("<h3>Overzicht van shortpositie</h3>")
    parts.append("<ul>")
    parts.append(f"<li><strong>Aandeel:</strong> {issuer}</li>")
    parts.append(f"<li><strong>Short seller:</strong> {short_seller}</li>")
    # Removed the first 'Meldingsdatum' as requested
    # Positie + vorige (if available)
    if prev_str:
        parts.append(f"<li><strong>Positie:</strong> {pct_str} (vorige: {prev_str})</li>")
    else:
        parts.append(f"<li><strong>Positie:</strong> {pct_str}</li>")
    if isin:
        parts.append(f"<li><strong>ISIN:</strong> {isin}</li>")
    if date_iso:
        parts.append(f"<li><strong>Meldingsdatum:</strong> {date_iso}</li>")
    parts.append("</ul>")

    # New section: Eerdere meldingen (with table)
    parts.append("<h3>Eerdere meldingen</h3>")
    table_html = _history_table(history)
    if table_html:
        parts.append(table_html)
    else:
        parts.append("<p>Geen eerdere meldingen gevonden.</p>")

    parts.append("<h3>Disclaimer</h3>")
    parts.append(
        "<p><em>Deze publicatie is informatief en vormt geen beleggingsadvies. "
        "De informatie op deze pagina is gebaseerd op het AFM-register voor nettoshortposities. "
        "De publicaties van het register zijn openbaar en bereikbaar via de AFM-website: "
        f'<a href="{source_url}" target="_blank" rel="nofollow noopener">{AFM_SOURCE_LABEL}</a>. '
        "Pennywatch.nl is niet gelieerd aan de Autoriteit Financiële Markten (AFM). "
        "Pennywatch.nl geeft geen garanties over de juistheid of volledigheid van de informatie.</em></p>"
    )

    return "\n".join(parts)

def build_article(item: Dict, *, category_id: int | None = None) -> Dict:
    issuer        = item.get("issuer") or item.get("emittent") or ""
    short_seller  = item.get("short_seller") or item.get("melder") or ""
    pct_num       = item.get("net_short_pct_num")
    pct_raw       = item.get("net_short_pct") or ""
    pct_str       = _pct_nl(pct_num, pct_raw)  # two decimals
    date_iso      = item.get("position_date_iso") or item.get("position_date") or item.get("meldingsdatum") or ""

    prev_num      = item.get("prev_net_short_pct_num")
    prev_raw      = item.get("prev_net_short_pct") or ""
    prev_str      = _pct_nl(prev_num, prev_raw) if prev_num is not None else None
    direction     = item.get("direction")

    title   = _nl_title(issuer, short_seller, pct_str, direction)
    excerpt = _excerpt_nl(issuer, short_seller, pct_str, date_iso, prev_str, direction)
    content = _content_nl(item)

    payload: Dict = {
        "title": title,
        # Publisher will set status/category (draft / 777)
        "excerpt": excerpt,
        "content": content,
        # Tags as names — publisher resolves/creates IDs
        "tags": list(filter(None, {issuer, short_seller})),
        "meta": {
            "afm_unique_id": item.get("unique_id") or item.get("afm_key"),
            "afm_date": date_iso,
            "type": "shortpositie",
            "prev_pct": prev_str,
            "direction": direction,
        },
    }
    if category_id is not None:
        payload["categories"] = [int(category_id)]
    return payload

def build_post(item: Dict, *, category_id: int | None = None) -> Dict:
    return build_article(item, category_id=category_id)
