# ============================================================================
# modules/validator.py
# ============================================================================
# Comprehensive data validation for OHLC candles

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate OHLC data quality and integrity."""
    
    @staticmethod
    def validate_ohlc_logic(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate OHLC logical relationships.
        
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        
        if df.empty:
            return True, []
        
        try:
            # High >= Open, Close, Low
            invalid_high = df[
                (df['high'] < df['open']) | 
                (df['high'] < df['close']) | 
                (df['high'] < df['low'])
            ]
            if not invalid_high.empty:
                errors.append(f"Found {len(invalid_high)} rows with high < other values")
            
            # Low <= Open, Close, High
            invalid_low = df[
                (df['low'] > df['open']) | 
                (df['low'] > df['close']) | 
                (df['low'] > df['high'])
            ]
            if not invalid_low.empty:
                errors.append(f"Found {len(invalid_low)} rows with low > other values")
            
            # Open, High, Low, Close > 0
            negative_values = df[
                (df['open'] <= 0) | 
                (df['high'] <= 0) | 
                (df['low'] <= 0) | 
                (df['close'] <= 0)
            ]
            if not negative_values.empty:
                errors.append(f"Found {len(negative_values)} rows with non-positive OHLC values")
            
        except KeyError as e:
            errors.append(f"Missing required column: {e}")
        except Exception as e:
            errors.append(f"Error validating OHLC: {e}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_volumes(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate volume data."""
        errors = []
        
        if df.empty or 'volume' not in df.columns:
            return True, []
        
        try:
            # Check for negative volumes
            negative_vol = df[df['volume'] < 0]
            if not negative_vol.empty:
                errors.append(f"Found {len(negative_vol)} rows with negative volume")
            
            # Check for excessively large volumes (potential data errors)
            # Skip this for now as we don't know expected ranges
            
        except Exception as e:
            errors.append(f"Error validating volumes: {e}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_timestamps(df: pd.DataFrame, expected_interval: Optional[str] = None) -> Tuple[bool, List[str]]:
        """
        Validate timestamp consistency.
        
        Args:
            df: DataFrame with 'datetime' column
            expected_interval: Expected interval like 'ONE_MINUTE' (optional)
            
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        
        if df.empty or 'datetime' not in df.columns:
            return True, []
        
        try:
            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Sort and check for duplicates
            sorted_df = df.sort_values('datetime')
            duplicates = sorted_df[sorted_df.duplicated(subset=['datetime'], keep=False)]
            if not duplicates.empty:
                errors.append(f"Found {len(duplicates)} duplicate timestamps")
            
            # Check for timestamps in future (relative to now, with some buffer)
            future_timestamps = sorted_df[sorted_df['datetime'] > datetime.now() + timedelta(days=1)]
            if not future_timestamps.empty:
                errors.append(f"Found {len(future_timestamps)} future timestamps")
            
            # Check timestamp ordering
            if not sorted_df['datetime'].is_monotonic_increasing:
                errors.append("Timestamps are not monotonically increasing")
            
        except Exception as e:
            errors.append(f"Error validating timestamps: {e}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_candle_count(
        df: pd.DataFrame,
        expected_date: date,
        interval: str = "ONE_MINUTE",
        market_open_time: str = "09:15",
        market_close_time: str = "15:30"
    ) -> Tuple[bool, Dict[str, any]]:
        """
        Validate candle count for a trading day.
        
        Args:
            df: DataFrame with 'datetime' column
            expected_date: Date to validate
            interval: Trading interval
            market_open_time: Market open time HH:MM
            market_close_time: Market close time HH:MM
            
        Returns:
            (is_valid, details_dict)
        """
        details = {
            'date': str(expected_date),
            'interval': interval,
            'total_records': len(df),
            'expected_candles': 0,
            'actual_candles': 0,
            'missing_candles': 0,
            'is_valid': False,
            'notes': []
        }
        
        try:
            if df.empty or 'datetime' not in df.columns:
                details['notes'].append("Empty data or missing datetime column")
                return False, details
            
            # Filter for the expected date
            if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            df_date = df[df['datetime'].dt.date == expected_date]
            
            if df_date.empty:
                details['notes'].append(f"No data for date {expected_date}")
                return False, details
            
            # Calculate expected candle count
            interval_map = {
                'ONE_MINUTE': 1,
                'THREE_MINUTE': 3,
                'FIVE_MINUTE': 5,
                'TEN_MINUTE': 10,
                'FIFTEEN_MINUTE': 15,
                'THIRTY_MINUTE': 30,
                'ONE_HOUR': 60,
            }
            
            if interval not in interval_map:
                details['notes'].append(f"Unknown interval: {interval}")
                return False, details
            
            mins_per_interval = interval_map[interval]
            
            # Market hours: 09:15 to 15:30 = 6 hours 15 minutes = 375 minutes
            market_minutes = 6 * 60 + 15  # 375 minutes
            expected_candles = market_minutes // mins_per_interval
            
            actual_candles = len(df_date)
            missing = max(0, expected_candles - actual_candles)
            
            details['expected_candles'] = expected_candles
            details['actual_candles'] = actual_candles
            details['missing_candles'] = missing
            
            # Allow some tolerance (market may have halts, etc)
            tolerance = max(1, expected_candles * 0.05)  # 5% tolerance
            
            if missing <= tolerance:
                details['is_valid'] = True
                details['notes'].append(f"Candle count within tolerance ({actual_candles}/{expected_candles})")
            else:
                details['notes'].append(f"Missing {missing} candles ({actual_candles}/{expected_candles})")
            
        except Exception as e:
            details['notes'].append(f"Error: {str(e)}")
        
        return details['is_valid'], details
    
    @staticmethod
    def validate_data_completeness(
        df: pd.DataFrame,
        start_date: date,
        end_date: date,
        interval: str = "ONE_MINUTE"
    ) -> Dict[str, any]:
        """
        Validate data completeness across date range.
        
        Returns:
            Dict with validation results
        """
        results = {
            'start_date': str(start_date),
            'end_date': str(end_date),
            'interval': interval,
            'total_records': len(df),
            'dates_with_data': 0,
            'dates_without_data': [],
            'is_valid': False,
            'coverage_percent': 0.0,
            'missing_segments': []
        }
        
        try:
            if df.empty or 'datetime' not in df.columns:
                return results
            
            if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Get dates with data
            dates_in_data = set(df['datetime'].dt.date.unique())
            
            # Generate expected trading dates (weekdays only, excluding holidays)
            current_date = start_date
            expected_dates = []
            
            while current_date <= end_date:
                # Exclude weekends (5=Saturday, 6=Sunday)
                if current_date.weekday() < 5:
                    expected_dates.append(current_date)
                current_date += timedelta(days=1)
            
            # Find missing dates
            missing_dates = set(expected_dates) - dates_in_data
            
            results['dates_with_data'] = len(dates_in_data)
            results['dates_without_data'] = sorted([str(d) for d in missing_dates])
            results['coverage_percent'] = (len(dates_in_data) / len(expected_dates)) * 100 if expected_dates else 0
            
            # Consider valid if > 95% coverage
            results['is_valid'] = results['coverage_percent'] >= 95
            
        except Exception as e:
            logger.error(f"Error validating data completeness: {e}")
        
        return results
    
    @staticmethod
    def validate_parquet_file(file_path: Path) -> Tuple[bool, Dict[str, any]]:
        """
        Comprehensive validation of a Parquet file.
        
        Returns:
            (is_valid, validation_report)
        """
        report = {
            'file_path': str(file_path),
            'file_exists': file_path.exists(),
            'is_valid': False,
            'checks': {},
            'errors': [],
            'warnings': []
        }
        
        try:
            if not file_path.exists():
                report['errors'].append(f"File not found: {file_path}")
                return False, report
            
            # Try to read file
            try:
                df = pd.read_parquet(file_path)
                report['checks']['file_readable'] = True
                report['checks']['record_count'] = len(df)
            except Exception as e:
                report['checks']['file_readable'] = False
                report['errors'].append(f"Cannot read file: {e}")
                return False, report
            
            # Validate OHLC
            ohlc_valid, ohlc_errors = DataValidator.validate_ohlc_logic(df)
            report['checks']['ohlc_logic'] = ohlc_valid
            if not ohlc_valid:
                report['errors'].extend(ohlc_errors)
            
            # Validate volumes
            vol_valid, vol_errors = DataValidator.validate_volumes(df)
            report['checks']['volumes'] = vol_valid
            if not vol_valid:
                report['errors'].extend(vol_errors)
            
            # Validate timestamps
            ts_valid, ts_errors = DataValidator.validate_timestamps(df)
            report['checks']['timestamps'] = ts_valid
            if not ts_valid:
                report['errors'].extend(ts_errors)
            
            # Overall validity
            report['is_valid'] = all(report['checks'].values())
            
        except Exception as e:
            report['errors'].append(f"Unexpected error: {e}")
        
        return report['is_valid'], report
