# ============================================================================
# modules/progress_tracker.py
# ============================================================================
# Track download progress for all tokens, supporting JSON and SQLite backends

import json
import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class ProgressRecord:
    """Represents a single token's download progress."""
    index_name: str
    token: str
    symbol: str
    expiry: str  # YYYY-MM-DD
    strike: int
    option_type: str  # CE or PE
    interval: str  # ONE_MINUTE, etc
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None  # YYYY-MM-DD
    last_successful_date: Optional[str] = None  # YYYY-MM-DD
    total_records: int = 0
    missing_segments: int = 0
    retry_count: int = 0
    status: str = "pending"  # pending, downloading, completed, partial, failed
    last_updated: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    def update_timestamp(self):
        """Update last_updated to current time."""
        self.last_updated = datetime.now().isoformat()


class ProgressTracker:
    """Track download progress across all tokens."""
    
    def __init__(self, backend: str = "sqlite", db_path: Optional[str] = None):
        """
        Initialize progress tracker.
        
        Args:
            backend: 'sqlite' or 'json'
            db_path: Path to sqlite database or json file
        """
        self.backend = backend
        
        if db_path is None:
            db_path = "./data_lake/metadata/progress"
        
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        if backend == "sqlite":
            self.db_path = Path(db_path) / "progress.db" if Path(db_path).is_dir() else Path(db_path)
            self._init_sqlite()
        elif backend == "json":
            self.json_path = Path(db_path) / "progress.json" if Path(db_path).is_dir() else Path(db_path)
            self._init_json()
        else:
            raise ValueError(f"Unsupported backend: {backend}")
        
        logger.info(f"Progress tracker initialized with {backend} backend")
    
    def _init_sqlite(self):
        """Initialize SQLite database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_name TEXT NOT NULL,
                token TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiry TEXT NOT NULL,
                strike INTEGER NOT NULL,
                option_type TEXT NOT NULL,
                interval TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                last_successful_date TEXT,
                total_records INTEGER DEFAULT 0,
                missing_segments INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                last_updated TEXT,
                error_message TEXT,
                UNIQUE(token, interval)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_status ON progress(status)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_token ON progress(token)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_expiry ON progress(expiry)
        ''')
        
        conn.commit()
        conn.close()
        logger.debug(f"SQLite database initialized at {self.db_path}")
    
    def _init_json(self):
        """Initialize JSON file."""
        if not self.json_path.exists():
            with open(self.json_path, 'w') as f:
                json.dump([], f, indent=2)
            logger.debug(f"JSON progress file initialized at {self.json_path}")
    
    def create_or_update(self, record: ProgressRecord) -> bool:
        """
        Create or update a progress record.
        
        Returns:
            True if successful
        """
        record.update_timestamp()
        
        if self.backend == "sqlite":
            return self._sqlite_create_or_update(record)
        elif self.backend == "json":
            return self._json_create_or_update(record)
    
    def _sqlite_create_or_update(self, record: ProgressRecord) -> bool:
        """Create or update in SQLite."""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            # Check if exists
            cursor.execute(
                'SELECT id FROM progress WHERE token = ? AND interval = ?',
                (record.token, record.interval)
            )
            exists = cursor.fetchone() is not None
            
            if exists:
                # Update
                cursor.execute('''
                    UPDATE progress SET
                        symbol = ?, expiry = ?, strike = ?, option_type = ?,
                        start_date = ?, end_date = ?, last_successful_date = ?,
                        total_records = ?, missing_segments = ?, retry_count = ?,
                        status = ?, last_updated = ?, error_message = ?
                    WHERE token = ? AND interval = ?
                ''', (
                    record.symbol, record.expiry, record.strike, record.option_type,
                    record.start_date, record.end_date, record.last_successful_date,
                    record.total_records, record.missing_segments, record.retry_count,
                    record.status, record.last_updated, record.error_message,
                    record.token, record.interval
                ))
            else:
                # Insert
                cursor.execute('''
                    INSERT INTO progress (
                        index_name, token, symbol, expiry, strike, option_type, interval,
                        start_date, end_date, last_successful_date,
                        total_records, missing_segments, retry_count,
                        status, last_updated, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.index_name, record.token, record.symbol, record.expiry,
                    record.strike, record.option_type, record.interval,
                    record.start_date, record.end_date, record.last_successful_date,
                    record.total_records, record.missing_segments, record.retry_count,
                    record.status, record.last_updated, record.error_message
                ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
            return False
    
    def _json_create_or_update(self, record: ProgressRecord) -> bool:
        """Create or update in JSON."""
        try:
            # Load existing
            if self.json_path.exists():
                with open(self.json_path, 'r') as f:
                    records = json.load(f)
            else:
                records = []
            
            # Find and update or append
            found = False
            for i, r in enumerate(records):
                if r['token'] == record.token and r['interval'] == record.interval:
                    records[i] = record.to_dict()
                    found = True
                    break
            
            if not found:
                records.append(record.to_dict())
            
            # Save
            with open(self.json_path, 'w') as f:
                json.dump(records, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating JSON progress: {e}")
            return False
    
    def get(self, token: str, interval: str) -> Optional[ProgressRecord]:
        """Retrieve a progress record."""
        if self.backend == "sqlite":
            return self._sqlite_get(token, interval)
        elif self.backend == "json":
            return self._json_get(token, interval)
    
    def _sqlite_get(self, token: str, interval: str) -> Optional[ProgressRecord]:
        """Get from SQLite."""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute(
                'SELECT * FROM progress WHERE token = ? AND interval = ?',
                (token, interval)
            )
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                return None
            
            # Convert row to ProgressRecord
            cols = [col[0] for col in cursor.description]
            data = dict(zip(cols, row))
            return ProgressRecord(**data)
            
        except Exception as e:
            logger.error(f"Error retrieving progress: {e}")
            return None
    
    def _json_get(self, token: str, interval: str) -> Optional[ProgressRecord]:
        """Get from JSON."""
        try:
            if not self.json_path.exists():
                return None
            
            with open(self.json_path, 'r') as f:
                records = json.load(f)
            
            for r in records:
                if r.get('token') == token and r.get('interval') == interval:
                    return ProgressRecord(**r)
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving JSON progress: {e}")
            return None
    
    def get_by_status(self, status: str) -> List[ProgressRecord]:
        """Get all records with a specific status."""
        if self.backend == "sqlite":
            return self._sqlite_get_by_status(status)
        elif self.backend == "json":
            return self._json_get_by_status(status)
    
    def _sqlite_get_by_status(self, status: str) -> List[ProgressRecord]:
        """Get by status from SQLite."""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM progress WHERE status = ?', (status,))
            rows = cursor.fetchall()
            conn.close()
            
            records = []
            for row in rows:
                cols = [description[0] for description in cursor.description]
                data = dict(zip(cols, row))
                records.append(ProgressRecord(**data))
            
            return records
            
        except Exception as e:
            logger.error(f"Error retrieving by status: {e}")
            return []
    
    def _json_get_by_status(self, status: str) -> List[ProgressRecord]:
        """Get by status from JSON."""
        try:
            if not self.json_path.exists():
                return []
            
            with open(self.json_path, 'r') as f:
                records = json.load(f)
            
            results = []
            for r in records:
                if r.get('status') == status:
                    results.append(ProgressRecord(**r))
            
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving JSON by status: {e}")
            return []
    
    def get_all(self) -> List[ProgressRecord]:
        """Get all progress records."""
        if self.backend == "sqlite":
            return self._sqlite_get_all()
        elif self.backend == "json":
            return self._json_get_all()
    
    def _sqlite_get_all(self) -> List[ProgressRecord]:
        """Get all from SQLite."""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM progress ORDER BY last_updated DESC')
            rows = cursor.fetchall()
            conn.close()
            
            records = []
            for row in rows:
                cols = [description[0] for description in cursor.description]
                data = dict(zip(cols, row))
                records.append(ProgressRecord(**data))
            
            return records
            
        except Exception as e:
            logger.error(f"Error retrieving all: {e}")
            return []
    
    def _json_get_all(self) -> List[ProgressRecord]:
        """Get all from JSON."""
        try:
            if not self.json_path.exists():
                return []
            
            with open(self.json_path, 'r') as f:
                records_dicts = json.load(f)
            
            return [ProgressRecord(**r) for r in records_dicts]
            
        except Exception as e:
            logger.error(f"Error retrieving all from JSON: {e}")
            return []
    
    def get_summary(self) -> Dict[str, int]:
        """Get summary statistics of progress."""
        records = self.get_all()
        
        summary = {
            'total_tokens': len(records),
            'pending': len([r for r in records if r.status == 'pending']),
            'downloading': len([r for r in records if r.status == 'downloading']),
            'completed': len([r for r in records if r.status == 'completed']),
            'partial': len([r for r in records if r.status == 'partial']),
            'failed': len([r for r in records if r.status == 'failed']),
            'total_records_downloaded': sum(r.total_records for r in records),
        }
        
        return summary
