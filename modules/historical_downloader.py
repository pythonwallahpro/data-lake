# ============================================================================
# modules/historical_downloader.py
# ============================================================================
# Download historical OHLC data from Angel Broking API with rate limiting and retry

import pandas as pd
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter for API requests."""
    
    def __init__(self, requests_per_minute: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum requests per minute
        """
        self.requests_per_minute = requests_per_minute
        self.requests = []
    
    def wait_if_needed(self):
        """Wait if rate limit would be exceeded."""
        now = time.time()
        
        # Remove old requests (> 1 minute old)
        self.requests = [req_time for req_time in self.requests if now - req_time < 60]
        
        # If at limit, wait
        if len(self.requests) >= self.requests_per_minute:
            sleep_time = 60 - (now - self.requests[0])
            if sleep_time > 0:
                logger.warning(f"Rate limit reached. Sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
        
        # Record this request
        self.requests.append(time.time())


class HistoricalDataDownloader:
    """
    Download historical OHLC data from Angel Broking SmartAPI.
    Supports retry logic, rate limiting, and resumable downloads.
    """
    
    def __init__(
        self,
        smart_api,
        requests_per_minute: int = 60,
        max_retries: int = 3,
        retry_wait_seconds: int = 5,
        request_timeout: int = 30,
        chunk_size_days: int = 7
    ):
        """
        Initialize downloader.
        
        Args:
            smart_api: Angel Broking SmartConnect API object
            requests_per_minute: Rate limit
            max_retries: Max retry attempts
            retry_wait_seconds: Wait time between retries
            request_timeout: API request timeout
            chunk_size_days: Days per API request chunk
        """
        self.api = smart_api
        self.rate_limiter = RateLimiter(requests_per_minute)
        self.max_retries = max_retries
        self.retry_wait_seconds = retry_wait_seconds
        self.request_timeout = request_timeout
        self.chunk_size_days = chunk_size_days
    
    def download_historic_data(
        self,
        exchange: str,
        token: str,
        start_date: date,
        end_date: date,
        interval: str = "ONE_MINUTE"
    ) -> Optional[pd.DataFrame]:
        """
        Download historical data for a token across date range.
        Downloads in chunks to respect API limits.
        
        Args:
            exchange: Exchange code (NSE, MCX, NCDEX, NFO, BFO, etc)
            token: Instrument token
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            interval: Time interval
            
        Returns:
            DataFrame with OHLC data or None if failed
        """
        all_data = []
        current_date = start_date
        
        while current_date < end_date:
            # Calculate chunk end date
            chunk_end = min(
                current_date + timedelta(days=self.chunk_size_days),
                end_date
            )
            
            logger.info(
                f"Downloading {interval} data for token {token} "
                f"from {current_date} to {chunk_end}"
            )
            
            # Download chunk with retry
            chunk_data = self._download_chunk_with_retry(
                exchange, token, current_date, chunk_end, interval
            )
            
            if chunk_data is not None and len(chunk_data) > 0:
                all_data.append(chunk_data)
                logger.info(f"Downloaded {len(chunk_data)} records for {current_date}")
            else:
                logger.warning(f"No data returned for {current_date} to {chunk_end}")
            
            current_date = chunk_end + timedelta(days=1)
        
        # Combine all data
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            
            # Deduplicate by datetime
            result_df = result_df.drop_duplicates(subset=['datetime'], keep='last')
            
            # Sort by datetime
            result_df = result_df.sort_values('datetime')
            
            logger.info(f"Total downloaded: {len(result_df)} records for token {token}")
            return result_df
        else:
            logger.error(f"No data downloaded for token {token}")
            return None
    
    def _download_chunk_with_retry(
        self,
        exchange: str,
        token: str,
        start_date: date,
        end_date: date,
        interval: str
    ) -> Optional[pd.DataFrame]:
        """Download a chunk of data with retry logic."""
        for attempt in range(self.max_retries + 1):
            try:
                # Apply rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Download
                data = self._download_chunk(
                    exchange, token, start_date, end_date, interval
                )
                
                if data is not None:
                    return data
                else:
                    logger.warning(f"No data for {token} {start_date}-{end_date}")
                    return None
                    
            except Exception as e:
                logger.error(
                    f"Download attempt {attempt + 1}/{self.max_retries + 1} failed "
                    f"for token {token}: {e}"
                )
                
                if attempt < self.max_retries:
                    wait_time = self.retry_wait_seconds * (attempt + 1)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All retry attempts exhausted for token {token}")
                    return None
        
        return None
    
    def _download_chunk(
        self,
        exchange: str,
        token: str,
        start_date: date,
        end_date: date,
        interval: str
    ) -> Optional[pd.DataFrame]:
        """Download a single chunk of data from API."""
        try:
            # Format dates for API
            from_date_str = start_date.strftime("%Y-%m-%d 09:00")
            to_date_str = end_date.strftime("%Y-%m-%d 15:30")
            
            # Prepare API parameters
            params = {
                "exchange": exchange,
                "symboltoken": str(token),
                "interval": interval,
                "fromdate": from_date_str,
                "todate": to_date_str
            }
            
            logger.debug(f"API request params: {params}")
            
            # Call API
            response = self.api.getCandleData(params)
            
            # Check response
            if not response or not response.get("status"):
                logger.warning(f"API returned status: {response}")
                return None
            
            if "data" not in response or not response["data"]:
                logger.warning(f"No data in API response for token {token}")
                return None
            
            # Convert to DataFrame
            candles = response["data"]
            
            if not candles:
                logger.warning(f"Empty candles for {token}")
                return None
            
            df = pd.DataFrame(
                candles,
                columns=["datetime", "open", "high", "low", "close", "volume"]
            )
            
            # Convert dtypes
            df['datetime'] = pd.to_datetime(df['datetime'])
            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col])
            df['volume'] = pd.to_numeric(df['volume']).astype(int)
            
            logger.debug(f"Downloaded {len(df)} candles from API")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading chunk: {e}")
            raise
    
    def get_latest_available_date(
        self,
        exchange: str,
        token: str,
        interval: str = "ONE_MINUTE"
    ) -> Optional[date]:
        """
        Get the latest date with available data for a token.
        
        Returns:
            Latest date or None
        """
        try:
            today = date.today()
            
            # Try to fetch last 30 days
            start_date = today - timedelta(days=30)
            end_date = today
            
            df = self._download_chunk(
                exchange, token, start_date, end_date, interval
            )
            
            if df is not None and len(df) > 0:
                df['datetime'] = pd.to_datetime(df['datetime'])
                latest_datetime = df['datetime'].max()
                return latest_datetime.date()
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest available date: {e}")
            return None
    
    def get_earliest_available_date(
        self,
        exchange: str,
        token: str,
        interval: str = "ONE_MINUTE",
        lookback_years: int = 2
    ) -> Optional[date]:
        """
        Find the earliest date with available data for a token.
        Uses binary search for efficiency.
        
        Args:
            lookback_years: How far back to search
            
        Returns:
            Earliest available date or None
        """
        try:
            today = date.today()
            far_past = today - timedelta(days=365 * lookback_years)
            
            # Binary search
            left = far_past
            right = today
            earliest = None
            
            while left <= right:
                mid = left + (right - left) // 2
                
                # Try to fetch data at mid point
                test_start = mid
                test_end = mid + timedelta(days=1)
                
                df = self._download_chunk(
                    exchange, token, test_start, test_end, interval
                )
                
                if df is not None and len(df) > 0:
                    # Data exists at this date, try going back
                    earliest = mid
                    right = mid - timedelta(days=1)
                else:
                    # No data, try going forward
                    left = mid + timedelta(days=1)
            
            return earliest
            
        except Exception as e:
            logger.error(f"Error finding earliest available date: {e}")
            return None
