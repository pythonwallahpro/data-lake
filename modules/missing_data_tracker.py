# ============================================================================
# modules/missing_data_tracker.py
# ============================================================================
# Track missing data segments for retry

import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class MissingSegment:
    """Represents a missing data segment to be retried."""
    index_name: str
    token: str
    symbol: str
    date: str  # YYYY-MM-DD
    interval: str  # ONE_MINUTE, etc
    missing_from: str  # ISO datetime
    missing_to: str  # ISO datetime
    status: str = "pending"  # pending, retrying, completed, failed
    retry_count: int = 0
    last_retry_timestamp: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)
    
    def update_retry_info(self, success: bool, error_msg: Optional[str] = None):
        """Update retry information."""
        self.retry_count += 1
        self.last_retry_timestamp = datetime.now().isoformat()
        
        if success:
            self.status = "completed"
        else:
            self.status = "retrying"
            self.error_message = error_msg


class MissingDataTracker:
    """Track and manage missing data segments."""
    
    def __init__(self, data_lake_path: str = "./data_lake"):
        """Initialize missing data tracker."""
        self.base_path = Path(data_lake_path) / "metadata" / "missing_data"
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        self.json_file = self.base_path / "missing_segments.json"
        self._init_json()
        
        logger.info(f"Missing data tracker initialized at {self.base_path}")
    
    def _init_json(self):
        """Initialize JSON file if needed."""
        if not self.json_file.exists():
            with open(self.json_file, 'w') as f:
                json.dump([], f, indent=2)
    
    def add_missing_segment(self, segment: MissingSegment) -> bool:
        """
        Add or update a missing data segment.
        
        Returns:
            True if successful
        """
        try:
            # Load existing
            with open(self.json_file, 'r') as f:
                segments = json.load(f)
            
            # Check if already exists (same token, date, interval)
            found = False
            for i, seg in enumerate(segments):
                if (seg['token'] == segment.token and 
                    seg['date'] == segment.date and 
                    seg['interval'] == segment.interval):
                    segments[i] = segment.to_dict()
                    found = True
                    break
            
            if not found:
                segments.append(segment.to_dict())
            
            # Save
            with open(self.json_file, 'w') as f:
                json.dump(segments, f, indent=2)
            
            logger.debug(f"Added missing segment for {segment.token}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding missing segment: {e}")
            return False
    
    def get_pending_segments(self, max_retry_count: Optional[int] = None) -> List[MissingSegment]:
        """
        Get pending missing segments.
        
        Args:
            max_retry_count: Only return segments with retry_count <= this value
            
        Returns:
            List of pending MissingSegment objects
        """
        try:
            with open(self.json_file, 'r') as f:
                segments_dicts = json.load(f)
            
            segments = [MissingSegment(**s) for s in segments_dicts]
            
            # Filter for pending
            pending = [s for s in segments if s.status in ['pending', 'retrying']]
            
            # Filter by max retry count if specified
            if max_retry_count is not None:
                pending = [s for s in pending if s.retry_count <= max_retry_count]
            
            return sorted(pending, key=lambda s: (s.retry_count, s.date))
            
        except Exception as e:
            logger.error(f"Error retrieving pending segments: {e}")
            return []
    
    def mark_segment_completed(self, token: str, date_str: str, interval: str) -> bool:
        """Mark a missing segment as completed."""
        try:
            with open(self.json_file, 'r') as f:
                segments_dicts = json.load(f)
            
            for seg in segments_dicts:
                if (seg['token'] == token and 
                    seg['date'] == date_str and 
                    seg['interval'] == interval):
                    seg['status'] = 'completed'
                    seg['last_retry_timestamp'] = datetime.now().isoformat()
            
            with open(self.json_file, 'w') as f:
                json.dump(segments_dicts, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error marking segment completed: {e}")
            return False
    
    def get_summary(self) -> Dict[str, int]:
        """Get summary of missing segments."""
        try:
            with open(self.json_file, 'r') as f:
                segments_dicts = json.load(f)
            
            segments = [MissingSegment(**s) for s in segments_dicts]
            
            summary = {
                'total_segments': len(segments),
                'pending': len([s for s in segments if s.status == 'pending']),
                'retrying': len([s for s in segments if s.status == 'retrying']),
                'completed': len([s for s in segments if s.status == 'completed']),
                'failed': len([s for s in segments if s.status == 'failed']),
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting summary: {e}")
            return {}
    
    def clear_completed(self) -> int:
        """Remove all completed segments from tracking."""
        try:
            with open(self.json_file, 'r') as f:
                segments_dicts = json.load(f)
            
            original_count = len(segments_dicts)
            segments_dicts = [s for s in segments_dicts if s['status'] != 'completed']
            
            with open(self.json_file, 'w') as f:
                json.dump(segments_dicts, f, indent=2)
            
            removed = original_count - len(segments_dicts)
            logger.info(f"Removed {removed} completed segments")
            return removed
            
        except Exception as e:
            logger.error(f"Error clearing completed: {e}")
            return 0
    
    def export_to_csv(self, csv_path: Optional[str] = None) -> Optional[str]:
        """Export missing segments to CSV for analysis."""
        try:
            import pandas as pd
            
            if csv_path is None:
                csv_path = str(self.base_path / "missing_segments.csv")
            
            with open(self.json_file, 'r') as f:
                segments_dicts = json.load(f)
            
            if not segments_dicts:
                logger.info("No missing segments to export")
                return None
            
            df = pd.DataFrame(segments_dicts)
            df.to_csv(csv_path, index=False)
            
            logger.info(f"Exported missing segments to {csv_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            return None
