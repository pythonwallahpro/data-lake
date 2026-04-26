# ============================================================================
# modules/parquet_handler.py
# ============================================================================
# Read and write Parquet files for raw and cleaned data layers

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class ParquetHandler:
    """Handle reading and writing Parquet files for data lake."""
    
    @staticmethod
    def write_raw_data(
        df: pd.DataFrame,
        file_path: Path,
        compression: str = "snappy",
        **kwargs
    ) -> bool:
        """
        Write raw OHLC data to Parquet file.
        
        Args:
            df: DataFrame with columns: datetime, open, high, low, close, volume, etc
            file_path: Destination Parquet file path
            compression: 'snappy', 'gzip', 'brotli', etc
            
        Returns:
            True if successful
        """
        try:
            # Ensure datetime column
            if 'datetime' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            # Sort by datetime
            if 'datetime' in df.columns:
                df = df.sort_values('datetime')
            
            # Create directory if needed
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Parquet
            df.to_parquet(
                file_path,
                compression=compression,
                index=False,
                engine='pyarrow'
            )
            
            logger.debug(f"Wrote {len(df)} records to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing parquet file {file_path}: {e}")
            return False
    
    @staticmethod
    def write_cleaned_data(
        df: pd.DataFrame,
        file_path: Path,
        compression: str = "snappy",
        **kwargs
    ) -> bool:
        """
        Write cleaned and validated data to Parquet file.
        
        Args:
            df: DataFrame with cleaned data
            file_path: Destination Parquet file path
            compression: Compression algorithm
            
        Returns:
            True if successful
        """
        try:
            # Ensure datetime column and timezone
            if 'datetime' in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
                    df['datetime'] = pd.to_datetime(df['datetime'])
                
                # Set timezone to Asia/Kolkata if not already set
                if df['datetime'].dt.tz is None:
                    df['datetime'] = df['datetime'].dt.tz_localize('Asia/Kolkata')
                elif str(df['datetime'].dt.tz) != 'Asia/Kolkata':
                    df['datetime'] = df['datetime'].dt.tz_convert('Asia/Kolkata')
            
            # Sort by datetime
            if 'datetime' in df.columns:
                df = df.sort_values('datetime')
            
            # Create directory
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Parquet
            df.to_parquet(
                file_path,
                compression=compression,
                index=False,
                engine='pyarrow'
            )
            
            logger.debug(f"Wrote {len(df)} cleaned records to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing cleaned parquet file {file_path}: {e}")
            return False
    
    @staticmethod
    def read_parquet(file_path: Path) -> Optional[pd.DataFrame]:
        """
        Read Parquet file.
        
        Args:
            file_path: Path to Parquet file
            
        Returns:
            DataFrame or None if error
        """
        try:
            if not file_path.exists():
                logger.warning(f"Parquet file not found: {file_path}")
                return None
            
            df = pd.read_parquet(file_path, engine='pyarrow')
            logger.debug(f"Read {len(df)} records from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading parquet file {file_path}: {e}")
            return None
    
    @staticmethod
    def append_data(
        file_path: Path,
        new_data: pd.DataFrame,
        compression: str = "snappy",
        remove_duplicates: bool = True
    ) -> bool:
        """
        Append new data to existing Parquet file (deduplication-aware).
        
        Args:
            file_path: Path to existing Parquet file
            new_data: DataFrame with new records to append
            compression: Compression algorithm
            remove_duplicates: Remove duplicate timestamps
            
        Returns:
            True if successful
        """
        try:
            # Read existing data
            if file_path.exists():
                existing_df = pd.read_parquet(file_path)
                
                # Combine
                combined_df = pd.concat([existing_df, new_data], ignore_index=True)
                
                # Deduplicate by datetime if requested
                if remove_duplicates and 'datetime' in combined_df.columns:
                    combined_df = combined_df.drop_duplicates(subset=['datetime'], keep='last')
                
                # Sort by datetime
                if 'datetime' in combined_df.columns:
                    combined_df = combined_df.sort_values('datetime')
            else:
                combined_df = new_data.copy()
            
            # Write back
            return ParquetHandler.write_raw_data(
                combined_df,
                file_path,
                compression=compression
            )
            
        except Exception as e:
            logger.error(f"Error appending to parquet file {file_path}: {e}")
            return False
    
    @staticmethod
    def get_file_stats(file_path: Path) -> Optional[Dict[str, Any]]:
        """Get metadata about a Parquet file."""
        try:
            if not file_path.exists():
                return None
            
            df = pd.read_parquet(file_path)
            
            stats = {
                'file_path': str(file_path),
                'file_size_bytes': file_path.stat().st_size,
                'record_count': len(df),
                'columns': list(df.columns),
                'data_types': dict(df.dtypes.astype(str)),
            }
            
            # Add datetime range if available
            if 'datetime' in df.columns:
                stats['first_datetime'] = str(df['datetime'].min())
                stats['last_datetime'] = str(df['datetime'].max())
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting file stats for {file_path}: {e}")
            return None
    
    @staticmethod
    def resample_data(
        df: pd.DataFrame,
        source_interval: str,
        target_interval: str
    ) -> pd.DataFrame:
        """
        Resample OHLC data from one interval to another.
        
        Args:
            df: DataFrame with datetime and OHLC columns
            source_interval: e.g., 'ONE_MINUTE'
            target_interval: e.g., 'FIVE_MINUTE'
            
        Returns:
            Resampled DataFrame
        """
        try:
            # Map interval to pandas frequency
            interval_map = {
                'ONE_MINUTE': '1T',
                'THREE_MINUTE': '3T',
                'FIVE_MINUTE': '5T',
                'TEN_MINUTE': '10T',
                'FIFTEEN_MINUTE': '15T',
                'THIRTY_MINUTE': '30T',
                'ONE_HOUR': '1H',
                'ONE_DAY': '1D',
            }
            
            if target_interval not in interval_map:
                raise ValueError(f"Unsupported interval: {target_interval}")
            
            df = df.copy()
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime')
            
            # Resample OHLC
            resampled = df['open'].resample(interval_map[target_interval]).ohlc()
            resampled['volume'] = df['volume'].resample(interval_map[target_interval]).sum()
            
            # Add other columns if present
            for col in ['open_interest']:
                if col in df.columns:
                    resampled[col] = df[col].resample(interval_map[target_interval]).last()
            
            # Reset index
            resampled = resampled.reset_index()
            
            logger.debug(f"Resampled {len(df)} records from {source_interval} to {target_interval}")
            return resampled
            
        except Exception as e:
            logger.error(f"Error resampling data: {e}")
            return df
