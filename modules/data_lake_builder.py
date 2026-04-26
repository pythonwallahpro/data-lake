# ============================================================================
# modules/data_lake_builder.py
# ============================================================================
# Main orchestrator for building the options data lake

import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Tuple
import logging
from pathlib import Path

from modules.instrument_master import InstrumentMaster
from modules.historical_downloader import HistoricalDataDownloader
from modules.data_lake_manager import DataLakeManager
from modules.progress_tracker import ProgressTracker, ProgressRecord
from modules.parquet_handler import ParquetHandler
from modules.validator import DataValidator
from modules.missing_data_tracker import MissingDataTracker, MissingSegment
from utilities.symbol_parser import SymbolParser

logger = logging.getLogger(__name__)


class DataLakeBuilder:
    """
    Main orchestrator for building the options data lake.
    Handles downloading, storage, validation, and resume logic.
    """
    
    def __init__(
        self,
        smart_api,
        config: Dict,
        data_lake_path: str = "./data_lake"
    ):
        """
        Initialize data lake builder.
        
        Args:
            smart_api: Angel Broking SmartConnect API object
            config: Configuration dictionary
            data_lake_path: Base path for data lake
        """
        self.api = smart_api
        self.config = config
        self.data_lake_path = data_lake_path
        
        # Initialize components
        self.instrument_master = InstrumentMaster(Path(data_lake_path) / "cache")
        self.downloader = HistoricalDataDownloader(
            smart_api,
            requests_per_minute=config.get('download_config', {}).get('requests_per_minute', 60),
            max_retries=config.get('download_config', {}).get('max_retries', 3),
            retry_wait_seconds=config.get('download_config', {}).get('retry_wait_seconds', 5),
            chunk_size_days=config.get('download_config', {}).get('chunk_size_days', 7)
        )
        self.data_lake_manager = DataLakeManager(data_lake_path)
        self.progress_tracker = ProgressTracker(
            backend=config.get('progress_tracking', {}).get('backend', 'sqlite'),
            db_path=str(Path(data_lake_path) / "metadata" / "progress")
        )
        self.parquet_handler = ParquetHandler()
        self.missing_data_tracker = MissingDataTracker(data_lake_path)
        
        logger.info("Data lake builder initialized")
    
    def prepare_token_universe(
        self,
        index_name: str,
        expiry_mode: str,
        specific_expiry: Optional[date] = None
    ) -> List[Dict]:
        """
        Prepare list of tokens to download based on config.
        
        Returns:
            List of token dictionaries
        """
        try:
            # Get index config
            index_config = next((idx for idx in self.config['indices'] if idx['name'] == index_name), None)
            if not index_config:
                logger.error(f"Index {index_name} not found in config")
                return []
            
            # Load instrument master
            self.instrument_master.load()
            
            # Get expiries
            expiries = self._get_expiries(index_name, expiry_mode, specific_expiry)
            if not expiries:
                logger.warning(f"No expiries found for {index_name}")
                return []
            
            # Build token list
            tokens = []
            
            for expiry in expiries:
                for option_type in self.config.get('option_types', ['CE', 'PE']):
                    df = self.instrument_master.filter_options(
                        index_name,
                        index_config['exchange_segment'],
                        expiry=expiry,
                        option_type=option_type
                    )
                    
                    for _, row in df.iterrows():
                        token_info = {
                            'index_name': index_name,
                            'token': row['token'],
                            'symbol': row['symbol'],
                            'expiry': row['expiry'],
                            'strike': float(row['strike']),
                            'option_type': option_type,
                            'exchange_segment': index_config['exchange_segment'],
                            'lot_size': int(row['lotsize']),
                        }
                        tokens.append(token_info)
            
            logger.info(f"Prepared {len(tokens)} tokens for {index_name}")
            return tokens
            
        except Exception as e:
            logger.error(f"Error preparing token universe: {e}")
            return []
    
    def _get_expiries(
        self,
        index_name: str,
        expiry_mode: str,
        specific_expiry: Optional[date] = None
    ) -> List[str]:
        """Get list of expiries based on mode."""
        try:
            index_config = next((idx for idx in self.config['indices'] if idx['name'] == index_name), None)
            if not index_config:
                return []
            
            available_expiries = self.instrument_master.get_unique_expiries(
                index_name,
                index_config['exchange_segment']
            )
            
            if expiry_mode == 'current':
                # Get nearest future expiry
                today = date.today()
                future_expiries = [e for e in available_expiries if e >= today.strftime('%Y-%m-%d')]
                return [future_expiries[0]] if future_expiries else []
            
            elif expiry_mode == 'specific':
                if specific_expiry is None:
                    logger.error("specific_expiry required for 'specific' mode")
                    return []
                return [specific_expiry.strftime('%Y-%m-%d')]
            
            elif expiry_mode == 'all':
                return available_expiries
            
            else:
                logger.error(f"Unknown expiry mode: {expiry_mode}")
                return []
            
        except Exception as e:
            logger.error(f"Error getting expiries: {e}")
            return []
    
    def download_for_tokens(
        self,
        tokens: List[Dict],
        intervals: List[str],
        lookback_days: int = 365,
        resume: bool = True
    ) -> Dict[str, int]:
        """
        Download historical data for list of tokens.
        
        Returns:
            Statistics dict
        """
        stats = {
            'total_tokens': len(tokens),
            'successful': 0,
            'partial': 0,
            'failed': 0,
            'skipped': 0,
            'total_records_downloaded': 0,
        }
        
        try:
            for idx, token_info in enumerate(tokens):
                logger.info(f"Processing {idx + 1}/{len(tokens)}: {token_info['symbol']}")
                
                for interval in intervals:
                    # Check if already completed
                    progress = self.progress_tracker.get(token_info['token'], interval)
                    
                    if progress and progress.status == 'completed' and resume:
                        logger.info(f"Token {token_info['token']} {interval} already completed, skipping")
                        stats['skipped'] += 1
                        continue
                    
                    # Download
                    success = self._download_single_token(token_info, interval, lookback_days)
                    
                    if success:
                        stats['successful'] += 1
                    else:
                        stats['failed'] += 1
            
            logger.info(f"Download complete: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error downloading tokens: {e}")
            return stats
    
    def _download_single_token(
        self,
        token_info: Dict,
        interval: str,
        lookback_days: int
    ) -> bool:
        """Download data for a single token."""
        try:
            token = token_info['token']
            expiry = datetime.strptime(token_info['expiry'], '%Y-%m-%d').date()
            
            # Determine download date range
            end_date = expiry
            start_date = expiry - timedelta(days=lookback_days)
            
            # Create progress record
            progress = ProgressRecord(
                index_name=token_info['index_name'],
                token=token,
                symbol=token_info['symbol'],
                expiry=token_info['expiry'],
                strike=int(token_info['strike']),
                option_type=token_info['option_type'],
                interval=interval,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                status='downloading'
            )
            self.progress_tracker.create_or_update(progress)
            
            # Determine exchange
            exchange_map = {'NFO': 'NFO', 'BFO': 'BFO'}
            exchange = exchange_map.get(token_info['exchange_segment'], 'NFO')
            
            # Download data
            df = self.downloader.download_historic_data(
                exchange,
                token,
                start_date,
                end_date,
                interval
            )
            
            if df is None or df.empty:
                logger.warning(f"No data downloaded for {token}")
                progress.status = 'failed'
                self.progress_tracker.create_or_update(progress)
                return False
            
            # Save to raw layer
            raw_path = self.data_lake_manager.get_raw_parquet_path(
                token_info['index_name'],
                expiry,
                interval,
                token_info['option_type'],
                token_info['strike'],
                token
            )
            
            # Add metadata
            df['token'] = token
            df['symbol'] = token_info['symbol']
            df['index_name'] = token_info['index_name']
            df['expiry'] = token_info['expiry']
            df['strike'] = token_info['strike']
            df['option_type'] = token_info['option_type']
            df['interval'] = interval
            
            self.parquet_handler.write_raw_data(df, raw_path)
            
            # Validate
            is_valid, validation_report = DataValidator.validate_parquet_file(raw_path)
            
            # Update progress
            progress.total_records = len(df)
            progress.last_successful_date = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')
            progress.status = 'completed' if is_valid else 'partial'
            self.progress_tracker.create_or_update(progress)
            
            logger.info(f"Downloaded {len(df)} records for {token}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading token {token_info['token']}: {e}")
            
            # Update progress
            progress = ProgressRecord(
                index_name=token_info['index_name'],
                token=token_info['token'],
                symbol=token_info['symbol'],
                expiry=token_info['expiry'],
                strike=int(token_info['strike']),
                option_type=token_info['option_type'],
                interval=interval,
                status='failed',
                error_message=str(e)
            )
            self.progress_tracker.create_or_update(progress)
            
            return False
    
    def validate_all(self) -> Dict[str, any]:
        """Validate all data in the data lake."""
        try:
            parquet_files = self.data_lake_manager.list_parquet_files('raw')
            
            validation_results = {
                'total_files': len(parquet_files),
                'valid': 0,
                'invalid': 0,
                'errors': []
            }
            
            for file_path in parquet_files:
                is_valid, report = DataValidator.validate_parquet_file(file_path)
                
                if is_valid:
                    validation_results['valid'] += 1
                else:
                    validation_results['invalid'] += 1
                    validation_results['errors'].append(report)
            
            logger.info(f"Validation complete: {validation_results['valid']} valid, {validation_results['invalid']} invalid")
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating: {e}")
            return {'error': str(e)}
    
    def get_progress_summary(self) -> Dict[str, any]:
        """Get progress summary."""
        return self.progress_tracker.get_summary()
