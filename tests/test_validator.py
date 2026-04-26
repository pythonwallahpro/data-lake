# ============================================================================
# tests/test_validator.py
# ============================================================================
# Unit tests for data validator

import unittest
import pandas as pd
from datetime import datetime, date

from modules.validator import DataValidator


class TestOHLCValidator(unittest.TestCase):
    """Test OHLC validation logic."""
    
    def test_valid_ohlc(self):
        """Test validation of valid OHLC data."""
        df = pd.DataFrame({
            'open': [100, 105, 110],
            'high': [105, 110, 115],
            'low': [98, 103, 108],
            'close': [102, 108, 112],
            'volume': [1000, 1200, 800],
        })
        
        is_valid, errors = DataValidator.validate_ohlc_logic(df)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
    
    def test_invalid_high_value(self):
        """Test detection of invalid high value."""
        df = pd.DataFrame({
            'open': [100],
            'high': [99],  # High < Open
            'low': [98],
            'close': [102],
            'volume': [1000],
        })
        
        is_valid, errors = DataValidator.validate_ohlc_logic(df)
        self.assertFalse(is_valid)
        self.assertTrue(any('high' in e.lower() for e in errors))
    
    def test_negative_volume(self):
        """Test detection of negative volume."""
        df = pd.DataFrame({
            'open': [100],
            'high': [105],
            'low': [98],
            'close': [102],
            'volume': [-1000],  # Negative volume
        })
        
        is_valid, errors = DataValidator.validate_volumes(df)
        self.assertFalse(is_valid)
        self.assertTrue(any('negative' in e.lower() for e in errors))
    
    def test_duplicate_timestamps(self):
        """Test detection of duplicate timestamps."""
        df = pd.DataFrame({
            'datetime': [
                pd.Timestamp('2025-01-01 09:15:00'),
                pd.Timestamp('2025-01-01 09:15:00'),  # Duplicate
                pd.Timestamp('2025-01-01 09:16:00'),
            ],
            'open': [100, 100, 105],
            'high': [105, 105, 110],
            'low': [98, 98, 103],
            'close': [102, 102, 108],
            'volume': [1000, 1000, 1200],
        })
        
        is_valid, errors = DataValidator.validate_timestamps(df)
        self.assertFalse(is_valid)
        self.assertTrue(any('duplicate' in e.lower() for e in errors))


class TestCandleCountValidator(unittest.TestCase):
    """Test candle count validation."""
    
    def test_full_day_data(self):
        """Test validation of full trading day."""
        # Generate 375 minutes of data (9:15 to 15:30)
        datetimes = pd.date_range('2025-01-20 09:15', '2025-01-20 15:30', freq='1T')
        df = pd.DataFrame({
            'datetime': datetimes,
            'open': [100] * len(datetimes),
            'high': [105] * len(datetimes),
            'low': [98] * len(datetimes),
            'close': [102] * len(datetimes),
            'volume': [1000] * len(datetimes),
        })
        
        is_valid, details = DataValidator.validate_candle_count(
            df,
            date(2025, 1, 20),
            interval='ONE_MINUTE'
        )
        
        self.assertTrue(is_valid or 'within tolerance' in str(details))
    
    def test_incomplete_day_data(self):
        """Test detection of incomplete trading day."""
        # Generate only 100 candles (should be 375 for ONE_MINUTE)
        datetimes = pd.date_range('2025-01-20 09:15', periods=100, freq='1T')
        df = pd.DataFrame({
            'datetime': datetimes,
            'open': [100] * 100,
            'high': [105] * 100,
            'low': [98] * 100,
            'close': [102] * 100,
            'volume': [1000] * 100,
        })
        
        is_valid, details = DataValidator.validate_candle_count(
            df,
            date(2025, 1, 20),
            interval='ONE_MINUTE'
        )
        
        self.assertEqual(details['missing_candles'], 275)


class TestDataCompletenessValidator(unittest.TestCase):
    """Test data completeness validation."""
    
    def test_full_week_coverage(self):
        """Test validation of full trading week."""
        # Generate data for Mon-Fri
        datetimes = pd.date_range('2025-01-20', '2025-01-24', freq='D')
        df = pd.DataFrame({
            'datetime': datetimes,
            'open': [100] * len(datetimes),
            'high': [105] * len(datetimes),
            'low': [98] * len(datetimes),
            'close': [102] * len(datetimes),
            'volume': [1000] * len(datetimes),
        })
        
        results = DataValidator.validate_data_completeness(
            df,
            date(2025, 1, 20),
            date(2025, 1, 24)
        )
        
        self.assertGreater(results['coverage_percent'], 90)
        self.assertTrue(results['is_valid'])


if __name__ == '__main__':
    unittest.main()
