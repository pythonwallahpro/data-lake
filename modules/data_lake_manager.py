# ============================================================================
# modules/data_lake_manager.py
# ============================================================================
# Manage data lake folder structure and file organization

from pathlib import Path
from typing import Optional
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)


class DataLakeManager:
    """
    Manage data lake folder structure following a partitioned design.
    
    Structure:
    data_lake/
      raw/
        {INDEX}/
          options/
            expiry={EXPIRY}/
              interval={INTERVAL}/
                option_type={TYPE}/
                  strike={STRIKE}/
                    {TOKEN}.parquet
      
      cleaned/
        {INDEX}/
          options/
            expiry={EXPIRY}/
              interval={INTERVAL}/
                option_type={TYPE}/
                  strike={STRIKE}/
                    {TOKEN}.parquet
      
      metadata/
        instruments/
        progress/
        validation/
        missing_data/
    """
    
    def __init__(self, base_path: str = "./data_lake"):
        """Initialize data lake manager."""
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        self.raw_path = self.base_path / "raw"
        self.cleaned_path = self.base_path / "cleaned"
        self.metadata_path = self.base_path / "metadata"
        
        # Create main directories
        for path in [self.raw_path, self.cleaned_path, self.metadata_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Data lake initialized at {self.base_path}")
    
    def get_raw_parquet_path(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        option_type: str,
        strike: float,
        token: str
    ) -> Path:
        """
        Get path for raw data parquet file.
        
        Args:
            index_name: e.g., 'NIFTY'
            expiry: datetime.date object
            interval: e.g., 'ONE_MINUTE'
            option_type: 'CE' or 'PE'
            strike: Strike price
            token: Instrument token
            
        Returns:
            Full path to parquet file
        """
        return self._get_parquet_path(
            self.raw_path, index_name, expiry, interval, option_type, strike, token
        )
    
    def get_cleaned_parquet_path(
        self,
        index_name: str,
        expiry: date,
        interval: str,
        option_type: str,
        strike: float,
        token: str
    ) -> Path:
        """Get path for cleaned data parquet file."""
        return self._get_parquet_path(
            self.cleaned_path, index_name, expiry, interval, option_type, strike, token
        )
    
    def _get_parquet_path(
        self,
        layer_path: Path,
        index_name: str,
        expiry: date,
        interval: str,
        option_type: str,
        strike: float,
        token: str
    ) -> Path:
        """Internal method to generate parquet path."""
        # Ensure parquet directory structure exists
        full_path = (
            layer_path
            / index_name
            / "options"
            / f"expiry={expiry.strftime('%Y-%m-%d')}"
            / f"interval={interval}"
            / f"option_type={option_type}"
            / f"strike={int(strike)}"
        )
        
        full_path.mkdir(parents=True, exist_ok=True)
        
        return full_path / f"{token}.parquet"
    
    def get_metadata_instruments_path(self) -> Path:
        """Get path to instruments metadata directory."""
        path = self.metadata_path / "instruments"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def get_progress_db_path(self) -> Path:
        """Get path to progress tracking database."""
        path = self.metadata_path / "progress"
        path.mkdir(parents=True, exist_ok=True)
        return path / "progress.db"
    
    def get_progress_json_path(self) -> Path:
        """Get path to progress tracking JSON file."""
        path = self.metadata_path / "progress"
        path.mkdir(parents=True, exist_ok=True)
        return path / "progress.json"
    
    def get_missing_data_path(self) -> Path:
        """Get path to missing data tracking."""
        path = self.metadata_path / "missing_data"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def get_validation_path(self) -> Path:
        """Get path to validation reports."""
        path = self.metadata_path / "validation"
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def get_index_expiry_path(
        self,
        layer: str,  # 'raw' or 'cleaned'
        index_name: str,
        expiry: date
    ) -> Path:
        """Get base path for an index and expiry."""
        if layer == 'raw':
            base = self.raw_path
        elif layer == 'cleaned':
            base = self.cleaned_path
        else:
            raise ValueError(f"Invalid layer: {layer}")
        
        path = (
            base
            / index_name
            / "options"
            / f"expiry={expiry.strftime('%Y-%m-%d')}"
        )
        
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def list_available_indices(self, layer: str = 'raw') -> list:
        """List all indices that have data in the data lake."""
        if layer == 'raw':
            base = self.raw_path
        elif layer == 'cleaned':
            base = self.cleaned_path
        else:
            raise ValueError(f"Invalid layer: {layer}")
        
        indices = []
        options_path = base / ""
        
        for item in options_path.glob("*/options"):
            index_name = item.parent.name
            indices.append(index_name)
        
        return sorted(list(set(indices)))
    
    def list_available_expiries(
        self,
        layer: str = 'raw',
        index_name: Optional[str] = None
    ) -> dict:
        """
        List all available expiries in data lake.
        
        Returns:
            Dict mapping index_name to list of expiries
        """
        if layer == 'raw':
            base = self.raw_path
        elif layer == 'cleaned':
            base = self.cleaned_path
        else:
            raise ValueError(f"Invalid layer: {layer}")
        
        expiries_by_index = {}
        
        if index_name:
            indices = [index_name]
        else:
            indices = self.list_available_indices(layer)
        
        for idx in indices:
            expiry_path = base / idx / "options"
            if not expiry_path.exists():
                continue
            
            expiries = []
            for item in expiry_path.iterdir():
                if item.is_dir() and item.name.startswith("expiry="):
                    expiry_str = item.name.replace("expiry=", "")
                    expiries.append(expiry_str)
            
            if expiries:
                expiries_by_index[idx] = sorted(expiries)
        
        return expiries_by_index
    
    def list_parquet_files(
        self,
        layer: str = 'raw',
        index_name: Optional[str] = None,
        expiry: Optional[date] = None
    ) -> list:
        """
        List all parquet files matching criteria.
        
        Returns:
            List of Path objects
        """
        if layer == 'raw':
            base = self.raw_path
        elif layer == 'cleaned':
            base = self.cleaned_path
        else:
            raise ValueError(f"Invalid layer: {layer}")
        
        pattern = "*.parquet"
        
        if index_name and expiry:
            search_path = base / index_name / "options" / f"expiry={expiry.strftime('%Y-%m-%d')}"
        elif index_name:
            search_path = base / index_name / "options"
        else:
            search_path = base
        
        if search_path.exists():
            return list(search_path.rglob(pattern))
        
        return []
    
    def get_data_lake_stats(self) -> dict:
        """Get statistics about data lake content."""
        stats = {
            'raw_parquet_count': len(self.list_parquet_files('raw')),
            'cleaned_parquet_count': len(self.list_parquet_files('cleaned')),
            'raw_indices': self.list_available_indices('raw'),
            'cleaned_indices': self.list_available_indices('cleaned'),
            'raw_expiries': self.list_available_expiries('raw'),
            'cleaned_expiries': self.list_available_expiries('cleaned'),
        }
        return stats
