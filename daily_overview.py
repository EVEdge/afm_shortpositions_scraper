"""Daily AFM short-position overview for DeKoersen.nl.

The overview is data-driven: on every scraper run we look at the newest valid
`position_date_iso` present in the AFM data. For that AFM position date we keep
one WordPress article. If additional filings with the same position date appear
later, the same article is updated. If nothing changed, no WordPress write is
performed.

This avoids empty weekend articles and avoids assuming that the AFM has already
published all filings at a fixed clock time.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from html import escape
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from internal_links import (
    linked_holder_name,
    linked_stock_name,
)
from ticker_mapping import resolve_ticker


AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")
DAILY_SLUG_PREFIX = "shortmeldingen"


def _as_amsterdam(
    now: Optional[datetime] = None,
) -> datetime:

    now = (
        now
        or datetime.now(
            AMSTERDAM_TZ
        )
    )

    if now.tzinfo is None:
        return now.replace(
            tzinfo=AMSTERDAM_TZ
        )

    return now.astimezone(
        AMSTERDAM_TZ
    )


def _parse_iso_date(
    value: object,
) -> Optional[date]:

    text = str(
        value or ""
    ).strip()

    if not text:
        return None

    try:
        return datetime.strptime(
            text[:10],
            "%Y-%m-%d",
        ).date()

    except ValueError:
        return None


def _item_date(
    item: Dict,
) -> Optional[date]:

    return _parse_iso_date(
        item.get("position_date_iso")
        or item.get("position_date")
        or item.get("meldingsdatum")
    )


def latest_report_date(
    items: Iterable[Dict],
    now: Optional[datetime] = None,
) -> Optional[str]:
    """
    Return the newest valid AFM position date
    that is not in the future.
    """

    today = _as_amsterdam(
        now
    ).date()

    valid_dates = []

    for item in items:
        parsed = _item_date(
            item
        )

        if (
            parsed is not None
            and parsed <= today
        ):
            valid_dates.append(
                parsed
            )

    if not valid_dates:
        return None

    return max(
        valid_dates
    ).isoformat()


def select_daily_items(
    items: Iterable[Dict],
    report_date: str,
) -> List[Dict]:
    """
    Return unique filings whose AFM
    position date equals report_date.
    """

    target = _parse_iso_date(
        report_date
    )

    if target is None:
        return []

    selected: Dict[
        str,
        Dict
    ] = {}

    for item in items:

        if _item_date(item) != target:
            continue

        uid = str(
            item.get("unique_id")
            or item.get("afm_key")
            or ""
        ).strip()

        if not uid:
            uid = "|".join(
                [
                    str(
                        item.get("issuer")
                        or item.get("emittent")
                        or ""
                    ).strip(),

                    str(
                        item.get("short_seller")
                        or item.get("melder")
                        or ""
                    ).strip(),

                    report_date,

                    str(
                        item.get("net_short_pct_num")
                        or item.get("kapitaalbelang")
                        or ""
                    ).strip(),
                ]
            )

        selected[uid] = dict(
            item
        )

    return sorted(
        selected.values(),
        key=lambda item: (
            str(
                item.get("issuer")
                or item.get("emittent")
                or ""
            ).lower(),

            str(
                item.get("short_seller")
                or item.get("melder")
                or ""
            ).lower(),
        ),
    )


def daily_fingerprint(
    items: Iterable[Dict],
    report_date: str,
) -> str:
    """
    Stable hash used to avoid rewriting
    an unchanged daily overview.
    """

    normalized = []

    for item in select_daily_items(
        items,
        report_date,
    ):
        normalized.append(
            {
                "uid": str(
                    item.get("unique_id")
                    or item.get("afm_key")
                    or ""
                ),

                "issuer": str(
                    item.get("issuer")
                    or item.get("emittent")
                    or ""
                ),

                "isin": str(
                    item.get("issuer_isin")
                    or ""
                ),

                "holder": str(
                    item.get("short_seller")
                    or item.get("melder")
                    or ""
                ),

                "pct": item.get(
                    "net_short_pct_num",
                    item.get(
                        "kapitaalbelang"
                    ),
                ),

                "prev": item.get(
                    "prev_net_short_pct_num"
                ),

                "direction": item.get(
                    "direction"
                ),
            }
        )

    raw = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def daily_slug(
    report_date: str,
) -> str:

    return (
        f"{DAILY_SLUG_PREFIX}-"
        f"{report_date}"
    )


def _date_nl(
    report_date: str,
) -> str:

    parsed = _parse_iso_date(
        report_date
    )

    if parsed is None:
        return report_date

    months = (
        "januari",
        "februari",
        "maart",
        "april",
        "mei",
        "juni",
        "juli",
        "augustus",
        "september",
        "oktober",
        "november",
        "december",
    )

    return (
        f"{parsed.day} "
        f"{months[parsed.month - 1]} "
        f"{parsed.year}"
    )


def _pct_nl(
    value: object,
) -> str:

    try:
        return (
            f"{float(value):.2f}%"
            .replace(".", ",")
        )

    except (
        TypeError,
        ValueError,
    ):
        return "—"


def _delta_text(
    item: Dict,
) -> str:

    current = item.get(
        "net_short_pct_num",
        item.get(
            "kapitaalbelang"
        ),
    )

    previous = item.get(
        "prev_net_short_pct_num"
    )

    if previous is None:
        return "Nieuwe melding"

    try:
        delta = (
            float(current)
            - float(previous)
        )

    except (
        TypeError,
        ValueError,
    ):
        return "—"

    if abs(delta) < 0.005:
        return "Ongewijzigd"

    sign = (
        "+"
        if delta > 0
        else ""
    )

    return (
        f"{sign}{delta:.2f}%-punt"
        .replace(".", ",")
    )


def _direction_counts(
    items: List[Dict],
) -> Dict[str, int]:

    counts = {
        "up": 0,
        "down": 0,
        "other": 0,
    }

    for item in items:

        direction = item.get(
            "direction"
        )

        if direction == "up":
            counts["up"] += 1

        elif direction == "down":
            counts["down"] += 1

        else:
            counts["other"] += 1

    return counts


def _table(
    items: List[Dict],
) -> str:

    rows = [
        "<table>",
        "<thead><tr>",
        "<th>Aandeel</th>"
        "<th>Shortpartij</th>"
        "<th>Nieuwe positie</th>",
        "<th>Vorige positie</th>"
        "<th>Mutatie</th>",
        "</tr></thead><tbody>",
    ]

    for item in items:

        issuer = str(
            item.get("issuer")
            or item.get("emittent")
            or ""
        ).strip()

        isin = (
            str(
                item.get("issuer_isin")
                or ""
            ).strip()
            or None
        )

        holder = str(
            item.get("short_seller")
            or item.get("melder")
            or ""
        ).strip()

        ticker = resolve_ticker(
            issuer,
            isin,
        )

        issuer_html = linked_stock_name(
            issuer,
            isin,
            ticker,
        )

        holder_html = linked_holder_name(
            holder
        )

        current = item.get(
            "net_short_pct_num",
            item.get(
                "kapitaalbelang"
            ),
        )

        previous = item.get(
            "prev_net_short_pct_num"
        )

        rows.append(
            "<tr>"
            f"<td>{issuer_html}</td>"
            f"<td>{holder_html}</td>"
            f"<td>{_pct_nl(current)}</td>"
            f"<td>{_pct_nl(previous)}</td>"
            f"<td>{escape(_delta_text(item))}</td>"
            "</tr>"
        )

    rows.append(
        "</tbody></table>"
    )

    return "\n".join(
        rows
    )


def build_daily_article(
    items: List[Dict],
    report_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict:
    """
    Build the WordPress payload for
    one AFM position date.
    """

    daily_items = select_daily_items(
        items,
        report_date,
    )

    if not daily_items:
        raise ValueError(
            "Cannot build daily short "
            "overview without filings."
        )

    now = _as_amsterdam(
        now
    )

    date_label = _date_nl(
        report_date
    )

    counts = _direction_counts(
        daily_items
    )

    total = len(
        daily_items
    )

    title = (
        "Nieuwe shortmeldingen bij de AFM "
        f"({date_label})"
    )

    intro = (
        "<p>"
        "Dit zijn de openbaar gemaakte wijzigingen "
        "in netto shortposities met positiedatum "
        f"{date_label}. "
        f"In totaal gaat het om {total} "
        f"{'melding' if total == 1 else 'meldingen'}."
        "</p>"
    )

    summary = (
        "<p>"
        f"Daarvan betreft het "
        f"{counts['up']} "
        f"{'verhoging' if counts['up'] == 1 else 'verhogingen'}, "
        f"{counts['down']} "
        f"{'verlaging' if counts['down'] == 1 else 'verlagingen'} en "
        f"{counts['other']} overige "
        f"{'melding' if counts['other'] == 1 else 'meldingen'}."
        "</p>"
    )

    disclaimer = (
        "<p><em>"
        "De gegevens zijn gebaseerd op het openbare "
        "AFM-register voor netto shortposities. "
        "Het overzicht toont de meldingen die in de "
        "gebruikte AFM-data aan deze positiedatum zijn "
        "gekoppeld. Deze publicatie is informatief en "
        "vormt geen beleggingsadvies."
        "</em></p>"
    )

    content = "\n".join(
        [
            intro,
            summary,

            "<h3>"
            "Overzicht shortmeldingen"
            "</h3>",

            _table(
                daily_items
            ),

            "<h3>"
            "Bron en disclaimer"
            "</h3>",

            disclaimer,
        ]
    )

    tickers = []

    for item in daily_items:

        issuer = str(
            item.get("issuer")
            or item.get("emittent")
            or ""
        ).strip()

        isin = (
            str(
                item.get("issuer_isin")
                or ""
            ).strip()
            or None
        )

        ticker = resolve_ticker(
            issuer,
            isin,
        )

        if ticker:
            tickers.append(
                ticker
            )

    return {
        "title": title,

        "slug": daily_slug(
            report_date
        ),

        "excerpt": (
            "Bekijk de nieuwe openbaar gemaakte "
            "shortmeldingen bij de AFM met "
            f"positiedatum {date_label}."
        ),

        "content": content,

        "tags": list(
            dict.fromkeys(
                [
                    "shortmeldingen",
                    "shortposities",
                    *tickers,
                ]
            )
        ),

        "date": now.strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),

        "status": "publish",
    }
