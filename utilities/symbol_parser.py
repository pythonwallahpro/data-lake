# ============================================================================
# utilities/symbol_parser.py
# ============================================================================
# Parse NIFTY/SENSEX option symbols and extract components

import re
from typing import Dict, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SymbolParser:
    """Parse Indian options symbols and extract components."""
    
    # Symbol patterns for different indices
    # NIFTY04NOV2523000CE -> NIFTY | 04NOV25 | 23000 | CE
    # SENSEX04NOV2581000PE -> SENSEX | 04NOV25 | 81000 | PE
    NIFTY_PATTERN = r'^NIFTY(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$'
    SENSEX_PATTERN = r'^SENSEX(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$'
    
    # Generic pattern for any index
    GENERIC_PATTERN = r'^([A-Z]+?)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$'
    
    @staticmethod
    def parse_symbol(symbol: str) -> Optional[Dict[str, any]]:
        """
        Parse an option symbol and extract components.
        
        Args:
            symbol: Option symbol (e.g., "NIFTY04NOV2523000CE")
            
        Returns:
            Dict with keys: index_name, expiry_date, strike, option_type, or None
        """
        symbol = symbol.strip().upper()
        
        # Try specific patterns first
        for pattern in [SymbolParser.NIFTY_PATTERN, SymbolParser.SENSEX_PATTERN]:
            match = re.match(pattern, symbol)
            if match:
                groups = match.groups()
                day, month, year, strike, option_type = groups
                
                expiry = SymbolParser._parse_date(day, month, year)
                index_name = SymbolParser._extract_index_name(symbol)
                
                return {
                    'index_name': index_name,
                    'symbol': symbol,
                    'expiry_date': expiry,
                    'strike': int(strike),
                    'option_type': option_type,
                    'day': day,
                    'month': month,
                    'year': year,
                }
        
        logger.warning(f"Could not parse symbol: {symbol}")
        return None
    
    @staticmethod
    def _parse_date(day: str, month: str, year: str) -> datetime.date:
        """
        Parse date components from symbol.
        
        Format: day=04, month=NOV, year=25 -> datetime.date(2025, 11, 4)
        """
        months = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        month_num = months.get(month.upper())
        if not month_num:
            raise ValueError(f"Invalid month: {month}")
        
        year_int = int(year)
        # Assume 20xx format
        if year_int < 50:
            year_int += 2000
        else:
            year_int += 1900
        
        return datetime(year_int, month_num, int(day)).date()
    
    @staticmethod
    def _extract_index_name(symbol: str) -> str:
        """Extract index name from symbol."""
        if symbol.startswith('NIFTY'):
            return 'NIFTY'
        elif symbol.startswith('SENSEX'):
            return 'SENSEX'
        elif symbol.startswith('BANKNIFTY'):
            return 'BANKNIFTY'
        else:
            # Generic extraction
            match = re.match(r'^([A-Z]+?)\d', symbol)
            if match:
                return match.group(1)
        return 'UNKNOWN'
    
    @staticmethod
    def build_symbol(
        index_name: str,
        expiry_date: datetime.date,
        strike: int,
        option_type: str
    ) -> str:
        """
        Build a symbol from components.
        
        Args:
            index_name: e.g., 'NIFTY'
            expiry_date: datetime.date object
            strike: Strike price
            option_type: 'CE' or 'PE'
            
        Returns:
            Symbol string, e.g., 'NIFTY04NOV2523000CE'
        """
        day = f"{expiry_date.day:02d}"
        month = expiry_date.strftime("%b").upper()
        year = f"{expiry_date.year % 100:02d}"
        strike_str = f"{strike:05d}"
        
        return f"{index_name}{day}{month}{year}{strike_str}{option_type}"
    
    @staticmethod
    def validate_symbol(symbol: str) -> bool:
        """Check if symbol is valid."""
        return SymbolParser.parse_symbol(symbol) is not None
