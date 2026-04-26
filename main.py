# ============================================================================
# main.py
# ============================================================================
# Main entry point for the Multi-Index Options Data Lake Builder

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from utilities.config_loader import ConfigLoader
from utilities.logging_setup import LoggerSetup
from cli import main as cli_main


def main():
    """Main entry point."""
    # Setup logging before anything else
    config_loader = ConfigLoader('config.yaml')
    config = config_loader.load()
    
    log_dir = config.get('logging', {}).get('log_dir', './logs')
    log_level = config.get('logging', {}).get('level', 'INFO')
    LoggerSetup.setup_logging(log_dir, log_level, console_output=True)
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Multi-Index Options Data Lake Builder")
    
    # Run CLI
    try:
        cli_main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
