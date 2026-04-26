# ============================================================================
# utilities/logging_setup.py
# ============================================================================
# Centralized logging configuration

import logging
import logging.handlers
from pathlib import Path
from typing import Optional


class LoggerSetup:
    """Configure logging for the data lake system."""
    
    @staticmethod
    def setup_logging(
        log_dir: str = "./logs",
        level: str = "INFO",
        console_output: bool = True
    ) -> logging.Logger:
        """
        Configure root logger with file and console handlers.
        
        Args:
            log_dir: Directory for log files
            level: Log level (DEBUG, INFO, WARNING, ERROR)
            console_output: Enable console output
            
        Returns:
            Configured logger
        """
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, level.upper()))
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # Format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, level.upper()))
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir_path / "data_lake.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        return logger
    
    @staticmethod
    def setup_module_loggers(log_dir: str = "./logs"):
        """Set up separate loggers for different modules."""
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(parents=True, exist_ok=True)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        modules = {
            'download': 'download.log',
            'validation': 'validation.log',
            'error': 'errors.log',
            'resume': 'resume.log',
        }
        
        for module_name, log_file in modules.items():
            logger = logging.getLogger(module_name)
            logger.setLevel(logging.DEBUG)
            
            handler = logging.FileHandler(log_dir_path / log_file)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
