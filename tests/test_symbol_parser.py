# ============================================================================
# tests/test_symbol_parser.py
# ============================================================================
# Unit tests for symbol parser

import unittest
from datetime import datetime, date

from utilities.symbol_parser import SymbolParser


class TestSymbolParser(unittest.TestCase):
    """Test SymbolParser functionality."""
    
    def test_parse_nifty_ce_symbol(self):
        """Test parsing NIFTY CE symbol."""
        symbol = "NIFTY04NOV2523000CE"
        result = SymbolParser.parse_symbol(symbol)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['index_name'], 'NIFTY')
        self.assertEqual(result['strike'], 23000)
        self.assertEqual(result['option_type'], 'CE')
        self.assertEqual(result['expiry_date'], date(2025, 11, 4))
    
    def test_parse_sensex_pe_symbol(self):
        """Test parsing SENSEX PE symbol."""
        symbol = "SENSEX04NOV2581000PE"
        result = SymbolParser.parse_symbol(symbol)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['index_name'], 'SENSEX')
        self.assertEqual(result['strike'], 81000)
        self.assertEqual(result['option_type'], 'PE')
        self.assertEqual(result['expiry_date'], date(2025, 11, 4))
    
    def test_build_symbol(self):
        """Test building symbol from components."""
        expiry = date(2025, 11, 4)
        symbol = SymbolParser.build_symbol('NIFTY', expiry, 23000, 'CE')
        
        self.assertEqual(symbol, 'NIFTY04NOV2523000CE')
    
    def test_round_trip_symbol(self):
        """Test parse and build round trip."""
        original = "NIFTY04NOV2523000CE"
        parsed = SymbolParser.parse_symbol(original)
        rebuilt = SymbolParser.build_symbol(
            parsed['index_name'],
            parsed['expiry_date'],
            parsed['strike'],
            parsed['option_type']
        )
        
        self.assertEqual(original, rebuilt)
    
    def test_invalid_symbol(self):
        """Test parsing invalid symbol."""
        result = SymbolParser.parse_symbol("INVALID")
        self.assertIsNone(result)
    
    def test_validate_symbol(self):
        """Test symbol validation."""
        self.assertTrue(SymbolParser.validate_symbol("NIFTY04NOV2523000CE"))
        self.assertTrue(SymbolParser.validate_symbol("SENSEX04NOV2581000PE"))
        self.assertFalse(SymbolParser.validate_symbol("INVALID"))


class TestSymbolParserDateParsing(unittest.TestCase):
    """Test date parsing in symbols."""
    
    def test_parse_date_november_2025(self):
        """Test parsing November 2025 date."""
        # NIFTY 04-NOV-2025 -> day=04, month=NOV, year=25
        parsed = SymbolParser.parse_symbol("NIFTY04NOV2523000CE")
        self.assertEqual(parsed['expiry_date'], date(2025, 11, 4))
    
    def test_parse_date_december_2026(self):
        """Test parsing December 2026 date."""
        # Build and parse back
        expiry = date(2026, 12, 24)
        symbol = SymbolParser.build_symbol('NIFTY', expiry, 25000, 'PE')
        parsed = SymbolParser.parse_symbol(symbol)
        self.assertEqual(parsed['expiry_date'], expiry)
    
    def test_parse_date_january_2026(self):
        """Test parsing January 2026 date."""
        symbol = SymbolParser.build_symbol('NIFTY', date(2026, 1, 29), 24000, 'CE')
        parsed = SymbolParser.parse_symbol(symbol)
        self.assertEqual(parsed['expiry_date'], date(2026, 1, 29))


if __name__ == '__main__':
    unittest.main()
