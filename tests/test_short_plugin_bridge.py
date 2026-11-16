"""Test the opt-in Pennywatch import bridge without WordPress or AFM network."""
from __future__ import annotations
import unittest
from unittest.mock import patch
from afm_scraper import ShortPosition, fetch_full_history_for_plugin
import afm_scraper
from pennywatch_universe import APPROVED, ACTIVE_TICKERS, filter_pennywatch_records
from shortcode_sync import prepare_shortcode_records, shortcode_dataset_hash


def row(uid: str, date: str, pct: float) -> ShortPosition:
    return ShortPosition(
        issuer="Fugro N.V.",
        issuer_isin="NL00150003E1",
        short_seller="Test Capital LLP",
        net_short_pct=f"{pct:.2f}%",
        net_short_pct_num=pct,
        position_date=date,
        position_date_iso=date,
        source_url="https://www.afm.nl/",
        unique_id=uid,
    )


class BridgeTests(unittest.TestCase):
    def test_active_universe(self):
        self.assertEqual(len(APPROVED), 37)
        self.assertEqual(len(ACTIVE_TICKERS), 35)
        self.assertFalse(any(x.endswith(".BR") for x in ACTIVE_TICKERS))
        source = [
            {"issuer": "Fugro N.V."},
            {"issuer": "ASML Holding N.V."},
            {"issuer": "Fagron NV"},
            {"issuer": "Vastned N.V."},
        ]
        self.assertEqual(len(filter_pennywatch_records(source)), 1)

    def test_full_history_contains_previous_filing(self):
        history = [row("FUG-0001", "2026-11-12", 0.6), row("FUG-0002", "2026-11-13", 0.8)]
        with patch.object(afm_scraper, "_FULL_SHORT_HISTORY", history):
            expanded = fetch_full_history_for_plugin()
        self.assertEqual(len(expanded), 2)
        self.assertEqual(expanded[1]["prev_net_short_pct_num"], 0.6)
        self.assertEqual(expanded[1]["prev_position_date_iso"], "2026-11-12")
        prepared = prepare_shortcode_records(expanded)
        self.assertEqual(len(prepared), 2)
        self.assertTrue(shortcode_dataset_hash(prepared))

    def test_empty_history_is_noop(self):
        with patch.object(afm_scraper, "_FULL_SHORT_HISTORY", []):
            self.assertEqual(fetch_full_history_for_plugin(), [])


if __name__ == "__main__":
    unittest.main()
