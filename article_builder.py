from html import escape
from typing import Optional

# AFM-register hoofdpagina (brontekst + link in disclaimer)
AFM_REGISTER_PAGE = "https://www.afm.nl/nl-nl/sector/registers/meldingenregisters/substantiele-deelnemingen"

def _fmt_pct_display(s: Optional[str]) -> str:
    """Toon EU-stijl (komma) of 'n.n.b.' als niet beschikbaar."""
    if not s:
        return "n.n.b."
    return s.replace(".", ",") if "." in s else s

def _slugify(text: str) -> str:
    """Very simple slugify: lower, replace spaces with '-', strip non-url-safe chars."""
    import re
    t = text.lower().strip()
    t = re.sub(r"\s+", "-", t)
    t = re.sub(r"[^a-z0-9\-]+", "", t)
    return t.strip("-")

def _short_id(afm_id: str | None) -> str:
    """Deterministic 6-char id from the unique afm_key (sha1 hex)."""
    if not afm_id:
        return ""
    return afm_id.strip()[:6].lower()

def build_article(melding: dict, prev_from_db: dict | None = None) -> dict:
    emittent = (melding.get("emittent") or "onbekende uitgevende instelling").strip()
    melder   = (melding.get("melder") or "onbekende melder").strip()
    datum    = (melding.get("meldingsdatum") or "onbekende datum").strip()
    kap      = melding.get("kapitaal_pct")     # bv. '5.26%'
    stem     = melding.get("stem_pct")         # bv. '6.15%'
    detail   = melding.get("detail_url")
    afm_id   = (melding.get("afm_key") or "").strip()  # unieke id (sha1)

    # Title (inclusief kapitaalbelang en datum)
    kap_for_title = _fmt_pct_display(kap)
    title = f"{melder} meldt {kap_for_title} belang in {emittent} ({datum})"

    # Overzicht (één alinea met <br>)
    overzicht_rows = [
        f"<strong>Meldingsplichtige:</strong> {escape(melder)}",
        f"<strong>Aandeel:</strong> {escape(emittent)}",
        f"<strong>Meldingsdatum:</strong> {escape(datum)}",
        f"<strong>Kapitaalbelang:</strong> {_fmt_pct_display(kap)}",
        f"<strong>Stemrechten:</strong> {_fmt_pct_display(stem)}",
    ]
    overzicht_html = "<p>" + "<br>".join(overzicht_rows) + "</p>"

    # Toelichting
    toelichting_html = """
<ul>
  <li><strong>Drempels & rapportage:</strong> Meldingen volgen bij het overschrijden van wettelijke drempels voor kapitaal of stemrechten. Drempelpercentages: 3%, 5%, 10%, 15%, 20%, 25%, 30%, 50%, 75%.</li>
  <li><strong>Signaalfunctie:</strong> Een stijging kan toegenomen vertrouwen of strategische opbouw duiden; een daling kan winstneming of herallocatie zijn. Plaats dit in de context van nieuws en cijfers.</li>
  <li><strong>Liquiditeit & koersimpact:</strong> Grote veranderingen in aandelenposities beïnvloeden vaak tijdelijk de orderflow. Check handelsvolumes rond de meldingsdatum en eventuele bloktransacties.</li>
  <li><strong>Vergelijking in de tijd:</strong> Volg dezelfde melder door de tijd om intentie te duiden (passief vs. actief, tactisch vs. strategisch).</li>
  <li><strong>Geen advies:</strong> Combineer meldingen met eigen onderzoek en analyse. Een melding is op zichzelf geen koop- of verkoopaanbeveling.</li>
</ul>
""".strip()

    # Disclaimer
    disclaimer_html = f"""
<p><em>Deze publicatie is informatief en vormt geen beleggingsadvies. De informatie op deze pagina is gebaseerd op het AFM-register voor substantiële deelnemingen. De publicaties van het register zijn openbaar en bereikbaar via de AFM-website: <a href="{escape(AFM_REGISTER_PAGE)}" target="_blank" rel="nofollow noopener">klik hier</a>. Pennywatch.nl is niet gelieerd aan de Autoriteit Financiële Markten (AFM). Pennywatch.nl geeft geen garanties over de juistheid of volledigheid van de informatie.</em></p>
""".strip()

    # Content (met verborgen id als HTML comment)
    content = f"""
<h3>Overzicht van de melding</h3>
{overzicht_html}

<h3>Waarom dit relevant is voor beleggers</h3>
{toelichting_html}

<h3>Disclaimer</h3>
{disclaimer_html}

<!-- afm_id:{escape(afm_id)} -->
""".strip()

    # Slug met 6-char id AAN HET EINDE (deterministisch)
    base = f"{_slugify(melder)}-belang-in-{_slugify(emittent)}-{_slugify(datum)}"
    sid = _short_id(afm_id)
    slug = f"{base}-{sid}" if sid else base

    return {"title": title, "content": content, "slug": slug}
