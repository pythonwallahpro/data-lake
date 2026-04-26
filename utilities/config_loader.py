# ============================================================================
# utilities/config_loader.py
# ============================================================================
# Load and validate YAML configuration

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Load and validate configuration from YAML file."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize config loader.
        
        Args:
            config_path: Path to config.yaml file
        """
        self.config_path = Path(config_path)
        self.config = None
        
    def load(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict containing configuration
            
        Raises:
            FileNotFoundError: If config file not found
            yaml.YAMLError: If YAML is invalid
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        if not self.config:
            raise ValueError("Config file is empty")
        
        self._validate_config()
        self._create_directories()
        
        logger.info(f"Config loaded from {self.config_path}")
        return self.config
    
    def _validate_config(self):
        """Validate essential config keys."""
        required_keys = ['indices', 'expiry_mode', 'intervals', 'data_lake_path']
        
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required config key: {key}")
        
        # Validate expiry mode
        if self.config['expiry_mode'] not in ['current', 'specific', 'all']:
            raise ValueError(f"Invalid expiry_mode: {self.config['expiry_mode']}")
        
        # Validate indices
        if not isinstance(self.config['indices'], list) or len(self.config['indices']) == 0:
            raise ValueError("indices must be a non-empty list")
        
        logger.info(f"Config validation passed")
    
    def _create_directories(self):
        """Create necessary data lake directories."""
        base_path = Path(self.config['data_lake_path'])
        
        dirs_to_create = [
            base_path / 'raw',
            base_path / 'cleaned',
            base_path / 'metadata' / 'instruments',
            base_path / 'metadata' / 'progress',
            base_path / 'metadata' / 'validation',
            base_path / 'metadata' / 'missing_data',
            Path(self.config['logging']['log_dir']),
        ]
        
        for dir_path in dirs_to_create:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Created/verified {len(dirs_to_create)} directories")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get config value by key."""
        if self.config is None:
            raise RuntimeError("Config not loaded. Call load() first.")
        return self.config.get(key, default)
    
    def get_index_by_name(self, index_name: str) -> Optional[Dict[str, Any]]:
        """Get index configuration by name."""
        for index in self.config['indices']:
            if index['name'] == index_name:
                return index
        return None
    
    def get_all_indices(self) -> list:
        """Get all configured indices."""
        return self.config.get('indices', [])
