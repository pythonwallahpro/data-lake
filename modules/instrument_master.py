# ============================================================================
# modules/instrument_master.py
# ============================================================================
# Load, cache, and filter instrument master from Angel Broking API

import os
import json
import urllib.request
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class InstrumentMaster:
    """
    Load and manage Angel Broking instrument master.
    Caches locally to avoid repeated downloads.
    """
    
    SCRIPT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    
    def __init__(self, data_dir: str = "./data", cache_days: int = 7):
        """
        Initialize InstrumentMaster.
        
        Args:
            data_dir: Directory to cache instrument master
            cache_days: Number of days to keep cache before refreshing
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.csv_path = self.data_dir / "instrument_master.csv"
        self.cache_days = cache_days
        self.instruments_df = None
        
    def load(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Load instrument master from cache or download fresh.
        
        Args:
            force_refresh: Force download even if cache is fresh
            
        Returns:
            DataFrame with all instruments
        """
        # Check if cache exists and is fresh
        if not force_refresh and self.csv_path.exists():
            file_mtime = datetime.fromtimestamp(self.csv_path.stat().st_mtime)
            if datetime.now() - file_mtime < timedelta(days=self.cache_days):
                logger.info(f"Loading instrument master from cache (updated: {file_mtime:%Y-%m-%d %H:%M:%S})")
                self.instruments_df = pd.read_csv(self.csv_path, dtype=str)
                return self.instruments_df
            else:
                logger.info("Instrument master cache expired. Downloading fresh...")
        else:
            logger.info("Downloading instrument master...")
        
        # Download fresh
        try:
            with urllib.request.urlopen(self.SCRIPT_MASTER_URL, timeout=30) as resp:
                instruments = json.loads(resp.read().decode('utf-8'))
            
            # Convert to DataFrame and save
            self.instruments_df = pd.DataFrame(instruments).astype(str)
            self.instruments_df.to_csv(self.csv_path, index=False)
            
            logger.info(f"Downloaded {len(self.instruments_df)} instruments. Saved to {self.csv_path}")
            return self.instruments_df
            
        except Exception as e:
            logger.error(f"Failed to download instrument master: {e}")
            if self.csv_path.exists():
                logger.warning("Falling back to cached version")
                self.instruments_df = pd.read_csv(self.csv_path, dtype=str)
                return self.instruments_df
            raise
    
    def filter_options(
        self,
        index_name: str,
        exchange_segment: str,
        instrument_type: str = "OPTIDX",
        expiry: Optional[str] = None,
        option_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Filter instruments for options matching criteria.
        
        Args:
            index_name: e.g., 'NIFTY', 'SENSEX'
            exchange_segment: e.g., 'NFO', 'BFO'
            instrument_type: e.g., 'OPTIDX'
            expiry: Specific expiry date (YYYY-MM-DD) or None for all
            option_type: 'CE' or 'PE' or None for all
            
        Returns:
            Filtered DataFrame
        """
        if self.instruments_df is None:
            self.load()
        
        df = self.instruments_df.copy()
        
        # Filter by name (contains index name)
        df = df[df['name'].str.contains(index_name, case=False, na=False)]
        
        # Filter by exchange segment
        df = df[df['exch_seg'].str.upper() == exchange_segment.upper()]
        
        # Filter by instrument type
        df = df[df['instrumenttype'].str.upper() == instrument_type.upper()]
        
        # Filter by expiry if specified
        if expiry:
            df = df[df['expiry'].str.upper() == expiry.upper()]
        
        # Filter out non-option entries (strike should not be 0 for options)
        df = df[df['strike'].astype(float) != 0]
        
        # Filter by option type (CE/PE in symbol) if specified
        if option_type:
            df = df[df['symbol'].str.contains(option_type.upper(), case=False, na=False)]
        
        logger.debug(f"Filtered {len(df)} instruments for {index_name} {exchange_segment}")
        return df
    
    def get_unique_expiries(
        self,
        index_name: str,
        exchange_segment: str
    ) -> List[str]:
        """
        Get all unique expiries for an index.
        
        Returns:
            List of expiry dates sorted in ascending order
        """
        if self.instruments_df is None:
            self.load()
        
        df = self.instruments_df.copy()
        df = df[df['name'].str.contains(index_name, case=False, na=False)]
        df = df[df['exch_seg'].str.upper() == exchange_segment.upper()]
        df = df[df['strike'].astype(float) != 0]
        
        expiries = df['expiry'].dropna().unique()
        expiries = sorted([e for e in expiries if e and str(e).strip() != ''])
        
        logger.debug(f"Found {len(expiries)} unique expiries for {index_name}")
        return list(expiries)
    
    def get_unique_strikes(
        self,
        index_name: str,
        exchange_segment: str,
        expiry: str
    ) -> List[float]:
        """
        Get all unique strikes for an index and expiry.
        
        Returns:
            List of strike prices sorted in ascending order
        """
        df = self.filter_options(index_name, exchange_segment, expiry=expiry)
        strikes = sorted(df['strike'].astype(float).unique())
        
        logger.debug(f"Found {len(strikes)} unique strikes for {index_name} {expiry}")
        return strikes
    
    def get_instrument_by_symbol(self, symbol: str) -> Optional[Dict]:
        """
        Get instrument details by exact symbol match.
        
        Returns:
            Dict with instrument details or None
        """
        if self.instruments_df is None:
            self.load()
        
        matches = self.instruments_df[self.instruments_df['symbol'].str.upper() == symbol.upper()]
        
        if len(matches) == 0:
            return None
        
        row = matches.iloc[0]
        return {
            'token': row['token'],
            'symbol': row['symbol'],
            'expiry': row['expiry'],
            'strike': float(row['strike']),
            'lotsize': int(row['lotsize']),
            'instrumenttype': row['instrumenttype'],
            'exch_seg': row['exch_seg'],
            'tick_size': float(row.get('tick_size', 0.05)),
        }
    
    def get_instruments_by_expiry_and_strikes(
        self,
        index_name: str,
        exchange_segment: str,
        expiry: str,
        strikes: List[float],
        option_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get all instruments matching expiry and specific strikes.
        
        Returns:
            DataFrame with filtered instruments
        """
        df = self.filter_options(index_name, exchange_segment, expiry=expiry, option_type=option_type)
        df = df[df['strike'].astype(float).isin(strikes)]
        return df
