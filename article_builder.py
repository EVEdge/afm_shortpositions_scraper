from datetime import datetime
from typing import Dict

def _fmt_pct(n: float) -> str:
    # Always show with 2 decimals and % sign, independent of comma/point input
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

    # Keep markup clean (no <hr>), use compact headings and lists.
    # Pennywatch tone: concise, factual, English.
    lines = []

    lines.append(f"<h3>Net Short Position Snapshot</h3>")
    lines.append("<ul>")
    lines.append(f"<li><strong>Issuer:</strong> {issuer}</li>")
    if isin:
        lines.append(f"<li><strong>ISIN:</strong> {isin}</li>")
    lines.append(f"<li><strong>Short seller:</strong> {short_seller}</li>")
    lines.append(f"<li><strong>Position:</strong> {pct_fmt}</li>")
    if date_iso:
        lines.append(f"<li><strong>Date (AFM):</strong> {date_iso}</li>")
    lines.append("</ul>")

    lines.append("<p>This post is generated from the AFM register of current net short positions. "
                 "Percentages refer to the disclosed net short interest in the issuer’s outstanding share capital.</p>")

    if source_url:
        lines.append(f'<p><em>Source:</em> <a href="{source_url}" rel="nofollow noopener" target="_blank">AFM – Current Net Short Positions</a></p>')

    lines.append("<h4>Notes</h4>")
    lines.append("<ul>")
    lines.append("<li>Positions can change frequently; consult the AFM register for the latest status.</li>")
    lines.append("<li>Thresholds start at 0.5% and must be updated at each 0.1% change thereafter.</li>")
    lines.append("</ul>")

    # Optional lightweight disclaimer
    lines.append("<p><em>Disclaimer: This is not investment advice. Do your own research.</em></p>")

    return "\n".join(lines)

def build_article(item: Dict, *, category_id: int | None = None) -> Dict:
    """
    Build a WordPress-ready payload from a ShortPosition record (dict).
    Returns a dict you can send to the WP REST API /posts endpoint.
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

    # Build tags: issuer, short seller; categories handled by caller
    tags = list(filter(None, {issuer, short_seller}))

    payload = {
        "title": title,
        "status": "publish",  # caller may override
        "excerpt": excerpt,
        "content": content,
        "meta": {
            # keep a stable unique key so publisher/DB can dedupe
            "afm_unique_id": item.get("unique_id"),
            "afm_date": date_iso,
        },
        # The caller/publisher should map these to WP term IDs.
        # If your publisher expects term IDs already, resolve them before POSTing.
        "tags": tags,  # names (if your publisher auto-resolves); otherwise swap for IDs
    }

    if category_id is not None:
        # Most WP APIs expect IDs, and as a list.
        payload["categories"] = [int(category_id)]

    return payload


# Backwards-compatible helper if your main() expects this name:
def build_post(item: Dict, *, category_id: int | None = None) -> Dict:
    return build_article(item, category_id=category_id)
