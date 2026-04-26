# ============================================================================
# modules/query_engine.py
# ============================================================================
# Query engine for backtesting and data analysis

import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import logging

from modules.parquet_handler import ParquetHandler
from modules.data_lake_manager import DataLakeManager

logger = logging.getLogger(__name__)


class QueryEngine:
    """
    Query engine for backtesting-ready data access.
    Supports various query patterns for options data.
    """
    
    def __init__(self, data_lake_path: str = "./data_lake"):
        """Initialize query engine."""
        self.data_lake_manager = DataLakeManager(data_lake_path)
        self.parquet_handler = ParquetHandler()
    
    def load_option_data(
        self,
        index_name: str,
        expiry: date,
        option_type: str,
        strike: float,
        interval: str,
        layer: str = 'cleaned'
    ) -> Optional[pd.DataFrame]:
        """
        Load data for a specific option contract.
        
        Args:
            index_name: e.g., 'NIFTY'
            expiry: Expiry date
            option_type: 'CE' or 'PE'
            strike: Strike price
            interval: e.g., 'ONE_MINUTE'
            layer: 'raw' or 'cleaned'
            
        Returns:
            DataFrame or None
        """
        try:
            # Find matching parquet file
            if layer == 'cleaned':
                base = self.data_lake_manager.cleaned_path
            else:
                base = self.data_lake_manager.raw_path
            
            # Build path
            search_path = (
                base
                / index_name
                / "options"
                / f"expiry={expiry.strftime('%Y-%m-%d')}"
                / f"interval={interval}"
                / f"option_type={option_type}"
                / f"strike={int(strike)}"
            )
            
            # Find parquet file
            if search_path.exists():
                parquet_files = list(search_path.glob("*.parquet"))
                if parquet_files:
                    return self.parquet_handler.read_parquet(parquet_files[0])
            
            logger.warning(f"No data found for {index_name} {expiry} {strike}{option_type} {interval}")
            return None
            
        except Exception as e:
            logger.error(f"Error loading option data: {e}")
            return None
    
    def load_expiry_chain(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        option_type: Optional[str] = None,
        layer: str = 'cleaned'
    ) -> Optional[pd.DataFrame]:
        """
        Load all options for a given expiry.
        
        Returns:
            DataFrame with all contracts for expiry
        """
        try:
            if layer == 'cleaned':
                base = self.data_lake_manager.cleaned_path
            else:
                base = self.data_lake_manager.raw_path
            
            expiry_path = (
                base
                / index_name
                / "options"
                / f"expiry={expiry.strftime('%Y-%m-%d')}"
                / f"interval={interval}"
            )
            
            if not expiry_path.exists():
                logger.warning(f"Expiry path not found: {expiry_path}")
                return None
            
            # Collect all parquet files
            parquet_files = list(expiry_path.rglob("*.parquet"))
            
            if not parquet_files:
                logger.warning(f"No parquet files found for {index_name} {expiry}")
                return None
            
            # Load and concatenate
            dfs = []
            for pfile in parquet_files:
                df = self.parquet_handler.read_parquet(pfile)
                if df is not None:
                    dfs.append(df)
            
            if dfs:
                result_df = pd.concat(dfs, ignore_index=True)
                result_df = result_df.sort_values(['datetime', 'strike', 'option_type'])
                logger.info(f"Loaded {len(result_df)} records for {index_name} {expiry}")
                return result_df
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading expiry chain: {e}")
            return None
    
    def load_atm_chain(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        atm_strikes: int = 1,
        otm_strikes: int = 2,
        spot_price: Optional[float] = None,
        layer: str = 'cleaned'
    ) -> Optional[pd.DataFrame]:
        """
        Load ATM ± OTM options for an expiry.
        
        Args:
            atm_strikes: Number of ATM strikes (1-2)
            otm_strikes: Number of OTM strikes on each side
            spot_price: Current spot price (used to determine ATM)
            
        Returns:
            DataFrame with ATM and OTM options
        """
        try:
            # Load all strikes for this expiry
            chain = self.load_expiry_chain(index_name, expiry, interval, layer=layer)
            
            if chain is None or chain.empty:
                return None
            
            if 'strike' not in chain.columns:
                logger.error("Chain data missing strike column")
                return None
            
            # Get unique strikes
            strikes = sorted(chain['strike'].unique())
            
            # Determine ATM if spot price provided
            if spot_price is not None:
                # Find closest strike to spot
                atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot_price))
            else:
                # Use middle strike
                atm_idx = len(strikes) // 2
            
            # Select strikes
            selected_strikes = []
            
            # Add OTM below ATM
            for i in range(max(0, atm_idx - otm_strikes), atm_idx):
                selected_strikes.append(strikes[i])
            
            # Add ATM
            for i in range(atm_idx, min(len(strikes), atm_idx + atm_strikes)):
                selected_strikes.append(strikes[i])
            
            # Add OTM above ATM
            for i in range(atm_idx + atm_strikes, min(len(strikes), atm_idx + atm_strikes + otm_strikes)):
                selected_strikes.append(strikes[i])
            
            # Filter chain
            result = chain[chain['strike'].isin(selected_strikes)]
            
            logger.info(f"Loaded ATM chain with {len(selected_strikes)} strikes")
            return result
            
        except Exception as e:
            logger.error(f"Error loading ATM chain: {e}")
            return None
    
    def get_available_expiries(self, index_name: Optional[str] = None, layer: str = 'cleaned') -> Dict[str, List[str]]:
        """Get all available expiries in data lake."""
        try:
            return self.data_lake_manager.list_available_expiries(layer, index_name)
        except Exception as e:
            logger.error(f"Error getting available expiries: {e}")
            return {}
    
    def get_available_strikes(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        layer: str = 'cleaned'
    ) -> List[float]:
        """Get all available strikes for an index/expiry."""
        try:
            chain = self.load_expiry_chain(index_name, expiry, interval, layer=layer)
            
            if chain is None or 'strike' not in chain.columns:
                return []
            
            return sorted(chain['strike'].unique().tolist())
            
        except Exception as e:
            logger.error(f"Error getting strikes: {e}")
            return []
    
    def resample_interval(
        self,
        df: pd.DataFrame,
        source_interval: str,
        target_interval: str
    ) -> pd.DataFrame:
        """
        Resample data from one interval to another.
        
        Example: Convert ONE_MINUTE to FIVE_MINUTE candles
        """
        try:
            return self.parquet_handler.resample_data(df, source_interval, target_interval)
        except Exception as e:
            logger.error(f"Error resampling: {e}")
            return df
    
    def get_data_completeness_report(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        layer: str = 'cleaned'
    ) -> Dict[str, Any]:
        """Generate data completeness report for an expiry."""
        try:
            chain = self.load_expiry_chain(index_name, expiry, interval, layer=layer)
            
            if chain is None or chain.empty:
                return {
                    'index_name': index_name,
                    'expiry': str(expiry),
                    'interval': interval,
                    'total_records': 0,
                    'unique_tokens': 0,
                    'unique_dates': 0,
                    'date_range': None
                }
            
            if 'datetime' in chain.columns:
                chain['datetime'] = pd.to_datetime(chain['datetime'])
                chain['date'] = chain['datetime'].dt.date
            
            report = {
                'index_name': index_name,
                'expiry': str(expiry),
                'interval': interval,
                'total_records': len(chain),
                'unique_tokens': chain['token'].nunique() if 'token' in chain.columns else 0,
                'unique_dates': chain['date'].nunique() if 'date' in chain.columns else 0,
            }
            
            if 'datetime' in chain.columns:
                report['first_datetime'] = str(chain['datetime'].min())
                report['last_datetime'] = str(chain['datetime'].max())
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating completeness report: {e}")
            return {}
    
    def find_missing_data_segments(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        layer: str = 'cleaned'
    ) -> List[Dict[str, Any]]:
        """
        Find time segments with missing data.
        
        Returns:
            List of missing segments with timestamps
        """
        try:
            chain = self.load_expiry_chain(index_name, expiry, interval, layer=layer)
            
            if chain is None or chain.empty:
                return []
            
            if 'datetime' not in chain.columns:
                return []
            
            chain = chain.copy()
            chain['datetime'] = pd.to_datetime(chain['datetime'])
            chain = chain.sort_values('datetime')
            
            # Group by token and find gaps
            missing_segments = []
            
            for token in chain['token'].unique() if 'token' in chain.columns else [None]:
                if token is not None:
                    token_data = chain[chain['token'] == token].sort_values('datetime')
                else:
                    token_data = chain.sort_values('datetime')
                
                if len(token_data) < 2:
                    continue
                
                # Calculate expected intervals
                interval_map = {
                    'ONE_MINUTE': 1,
                    'THREE_MINUTE': 3,
                    'FIVE_MINUTE': 5,
                    'TEN_MINUTE': 10,
                }
                
                expected_delta = timedelta(minutes=interval_map.get(interval, 1))
                
                # Find gaps
                datetimes = token_data['datetime'].values
                for i in range(len(datetimes) - 1):
                    actual_delta = pd.Timestamp(datetimes[i+1]) - pd.Timestamp(datetimes[i])
                    
                    if actual_delta > expected_delta * 1.5:  # Allow 50% tolerance
                        missing_segments.append({
                            'token': token,
                            'missing_from': str(datetimes[i]),
                            'missing_to': str(datetimes[i+1]),
                            'duration_minutes': (actual_delta.total_seconds() / 60),
                        })
            
            return missing_segments
            
        except Exception as e:
            logger.error(f"Error finding missing segments: {e}")
            return []
    
    def get_data_lake_summary(self) -> Dict[str, Any]:
        """Get overall summary of data lake content."""
        try:
            stats = self.data_lake_manager.get_data_lake_stats()
            
            summary = {
                'total_raw_parquets': stats['raw_parquet_count'],
                'total_cleaned_parquets': stats['cleaned_parquet_count'],
                'indices_in_raw': stats['raw_indices'],
                'indices_in_cleaned': stats['cleaned_indices'],
                'raw_expiries': stats['raw_expiries'],
                'cleaned_expiries': stats['cleaned_expiries'],
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting data lake summary: {e}")
            return {}
