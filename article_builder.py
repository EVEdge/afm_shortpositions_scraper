# article_builder.py

from typing import Dict

def _fmt_pct(n: float | str) -> str:
    try:
        return f"{float(n):.2f}%"
    except Exception:
        return str(n)

def _title(issuer: str, short_seller: str, pct_str: str) -> str:
    # Example: "Current Net Short Position: Adyen — Marshall Wace at 0.60%"
    return f"Current Net Short Position: {issuer} — {short_seller} at {pct_str}"

def _excerpt(issuer: str, short_seller: str, pct_str: str, date_iso: str) -> str:
    d = date_iso or ""
    return (
        f"{short_seller} currently holds a net short position in {issuer} of {pct_str}. "
        f"Based on AFM’s current register (date: {d})."
    )

def _content_html(item: Dict) -> str:
    issuer = item.get("issuer") or ""
    isin = item.get("issuer_isin") or ""
    short_seller = item.get("short_seller") or ""
    pct_raw = item.get("net_short_pct") or ""
    pct_num = item.get("net_short_pct_num") or 0.0
    pct_fmt = _fmt_pct(pct_num) if pct_num else pct_raw
    date_iso = item.get("position_date_iso") or item.get("position_date") or ""
    source_url = item.get("source_url") or ""

    parts: list[str] = []
    parts.append("<h3>Net Short Position Snapshot</h3>")
    parts.append("<ul>")
    parts.append(f"<li><strong>Issuer:</strong> {issuer}</li>")
    if isin:
        parts.append(f"<li><strong>ISIN:</strong> {isin}</li>")
    parts.append(f"<li><strong>Short seller:</strong> {short_seller}</li>")
    parts.append(f"<li><strong>Position:</strong> {pct_fmt}</li>")
    if date_iso:
        parts.append(f"<li><strong>Date (AFM):</strong> {date_iso}</li>")
    parts.append("</ul>")

    parts.append(
        "<p>This post is generated from the AFM register of current net short positions. "
        "Percentages refer to the disclosed net short interest in the issuer’s outstanding share capital.</p>"
    )

    if source_url:
        parts.append(
            f'<p><em>Source:</em> <a href="{source_url}" rel="nofollow noopener" target="_blank">'
            "AFM – Current Net Short Positions</a></p>"
        )

    parts.append("<h4>Notes</h4>")
    parts.append("<ul>")
    parts.append("<li>Positions can change frequently; consult the AFM register for the latest status.</li>")
    parts.append("<li>Thresholds start at 0.5% and must be updated at each 0.1% change thereafter.</li>")
    parts.append("</ul>")
    parts.append("<p><em>Disclaimer: This is not investment advice. Do your own research.</em></p>")

    return "\n".join(parts)

def build_article(item: Dict, *, category_id: int | None = None) -> Dict:
    """
    Build a WordPress-ready payload from a ShortPosition dict.
    """
    issuer = item.get("issuer") or ""
    short_seller = item.get("short_seller") or ""
    pct_raw = item.get("net_short_pct") or ""
    pct_num = item.get("net_short_pct_num") or 0.0
    pct_fmt = _fmt_pct(pct_num) if pct_num else pct_raw
    date_iso = item.get("position_date_iso") or item.get("position_date") or ""

    title = _title(issuer, short_seller, pct_fmt)
    excerpt = _excerpt(issuer, short_seller, pct_fmt, date_iso)
    content = _content_html(item)

    tags = list(filter(None, {issuer, short_seller}))

    payload: Dict = {
        "title": title,
        "status": "publish",           # caller can override
        "excerpt": excerpt,
        "content": content,
        "meta": {
            "afm_unique_id": item.get("unique_id"),
            "afm_date": date_iso,
        },
        # If your publisher resolves tag names to IDs, keep these as names.
        "tags": tags,
    }
    if category_id is not None:
        payload["categories"] = [int(category_id)]
    return payload

# Backward-compatibility aliases
def build_post(item: Dict, *, category_id: int | None = None) -> Dict:
    return build_article(item, category_id=category_id)

def build_afm_article(item: Dict, *, category_id: int | None = None) -> Dict:
    return build_article(item, category_id=category_id)

__all__ = ["build_article", "build_post", "build_afm_article"]
