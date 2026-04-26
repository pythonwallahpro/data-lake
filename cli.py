# ============================================================================
# cli.py
# ============================================================================
# Command-line interface for data lake management

import argparse
import sys
from datetime import datetime, date
import logging

from utilities.config_loader import ConfigLoader
from utilities.logging_setup import LoggerSetup
from modules.data_lake_builder import DataLakeBuilder
from modules.data_lake_manager import DataLakeManager
from modules.query_engine import QueryEngine
from modules.progress_tracker import ProgressTracker
from modules.missing_data_tracker import MissingDataTracker


def setup_auth():
    """Setup Angel Broking API authentication."""
    try:
        from resource.auth_v2 import smartApi, authToken, feedToken
        return smartApi
    except Exception as e:
        logging.error(f"Failed to authenticate: {e}")
        sys.exit(1)


def cmd_download(args, config, api):
    """Execute download command."""
    logger = logging.getLogger(__name__)
    
    try:
        builder = DataLakeBuilder(api, config)
        
        # Prepare tokens
        tokens = builder.prepare_token_universe(
            args.index or 'NIFTY',
            args.expiry or config.get('expiry_mode', 'current'),
            args.specific_expiry
        )
        
        if not tokens:
            logger.error("No tokens to download")
            return
        
        logger.info(f"Found {len(tokens)} tokens to download")
        
        # Download
        intervals = args.interval.split(',') if args.interval else config.get('intervals', ['ONE_MINUTE'])
        lookback_days = args.lookback or config.get('download_config', {}).get('lookback_days', 365)
        
        stats = builder.download_for_tokens(
            tokens,
            intervals,
            lookback_days,
            resume=config.get('download_config', {}).get('resume_on_restart', True)
        )
        
        print(f"\nDownload Summary:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    except Exception as e:
        logger.error(f"Download failed: {e}")
        sys.exit(1)


def cmd_validate(args, config):
    """Execute validation command."""
    logger = logging.getLogger(__name__)
    
    try:
        builder = DataLakeBuilder(None, config)
        
        logger.info("Starting validation...")
        results = builder.validate_all()
        
        print(f"\nValidation Summary:")
        print(f"  Total files: {results.get('total_files', 0)}")
        print(f"  Valid: {results.get('valid', 0)}")
        print(f"  Invalid: {results.get('invalid', 0)}")
        
        if results.get('errors'):
            print(f"\nFound {len(results['errors'])} errors. See logs for details.")
    
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


def cmd_retry_missing(args, config):
    """Retry missing data segments."""
    logger = logging.getLogger(__name__)
    
    try:
        tracker = MissingDataTracker(config.get('data_lake_path', './data_lake'))
        
        pending = tracker.get_pending_segments(max_retry_count=args.max_retries or 3)
        
        print(f"\nPending Missing Segments: {len(pending)}")
        for segment in pending[:10]:  # Show first 10
            print(f"  {segment.token}: {segment.date} {segment.missing_from} -> {segment.missing_to}")
        
        if len(pending) > 10:
            print(f"  ... and {len(pending) - 10} more")
    
    except Exception as e:
        logger.error(f"Retry failed: {e}")
        sys.exit(1)


def cmd_summary(args, config):
    """Display data lake summary."""
    logger = logging.getLogger(__name__)
    
    try:
        query_engine = QueryEngine(config.get('data_lake_path', './data_lake'))
        
        summary = query_engine.get_data_lake_summary()
        
        print(f"\nData Lake Summary:")
        print(f"  Raw Parquets: {summary.get('total_raw_parquets', 0)}")
        print(f"  Cleaned Parquets: {summary.get('total_cleaned_parquets', 0)}")
        print(f"  Indices in Raw: {summary.get('indices_in_raw', [])}")
        print(f"  Indices in Cleaned: {summary.get('indices_in_cleaned', [])}")
        
        # Progress summary
        progress_tracker = ProgressTracker(
            backend=config.get('progress_tracking', {}).get('backend', 'sqlite'),
            db_path=str(config.get('data_lake_path', './data_lake'))
        )
        
        progress = progress_tracker.get_summary()
        print(f"\nProgress Summary:")
        for key, value in progress.items():
            print(f"  {key}: {value}")
        
        # Missing data summary
        missing_tracker = MissingDataTracker(config.get('data_lake_path', './data_lake'))
        missing = missing_tracker.get_summary()
        print(f"\nMissing Data Summary:")
        for key, value in missing.items():
            print(f"  {key}: {value}")
    
    except Exception as e:
        logger.error(f"Summary failed: {e}")
        sys.exit(1)


def cmd_query(args, config):
    """Execute data query."""
    logger = logging.getLogger(__name__)
    
    try:
        query_engine = QueryEngine(config.get('data_lake_path', './data_lake'))
        
        if args.query_type == 'load':
            # Load specific option
            expiry = datetime.strptime(args.expiry, '%Y-%m-%d').date()
            df = query_engine.load_option_data(
                args.index or 'NIFTY',
                expiry,
                args.option_type or 'CE',
                float(args.strike or 23000),
                args.interval or 'ONE_MINUTE'
            )
            
            if df is not None:
                print(f"\nLoaded {len(df)} records")
                print(df.head())
            else:
                print("No data found")
        
        elif args.query_type == 'expiries':
            expiries = query_engine.get_available_expiries(args.index)
            print(f"\nAvailable Expiries:")
            for idx_name, expiry_list in expiries.items():
                print(f"  {idx_name}: {expiry_list}")
        
        elif args.query_type == 'strikes':
            expiry = datetime.strptime(args.expiry, '%Y-%m-%d').date()
            strikes = query_engine.get_available_strikes(
                args.index or 'NIFTY',
                expiry,
                args.interval or 'ONE_MINUTE'
            )
            print(f"\nAvailable Strikes: {strikes}")
        
        elif args.query_type == 'completeness':
            expiry = datetime.strptime(args.expiry, '%Y-%m-%d').date()
            report = query_engine.get_data_completeness_report(
                args.index or 'NIFTY',
                expiry,
                args.interval or 'ONE_MINUTE'
            )
            print(f"\nData Completeness Report:")
            for key, value in report.items():
                print(f"  {key}: {value}")
    
    except Exception as e:
        logger.error(f"Query failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Multi-Index Options Data Lake Builder',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python cli.py download --index NIFTY --expiry current --interval ONE_MINUTE,FIVE_MINUTE
  python cli.py download --index SENSEX --expiry all --interval ONE_MINUTE
  python cli.py validate --index NIFTY
  python cli.py retry-missing
  python cli.py summary
  python cli.py query expiries
  python cli.py query load --index NIFTY --expiry 2025-11-04 --strike 23000 --type CE
        '''
    )
    
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Download command
    download_parser = subparsers.add_parser('download', help='Download historical data')
    download_parser.add_argument('--index', help='Index name (NIFTY, SENSEX)')
    download_parser.add_argument('--expiry', choices=['current', 'all'], help='Expiry mode')
    download_parser.add_argument('--specific-expiry', help='Specific expiry (YYYY-MM-DD)')
    download_parser.add_argument('--interval', help='Intervals (comma-separated)')
    download_parser.add_argument('--lookback', type=int, help='Lookback days')
    download_parser.set_defaults(func=cmd_download)
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate downloaded data')
    validate_parser.add_argument('--index', help='Index name')
    validate_parser.set_defaults(func=cmd_validate)
    
    # Retry missing command
    retry_parser = subparsers.add_parser('retry-missing', help='Retry missing data segments')
    retry_parser.add_argument('--max-retries', type=int, help='Max retry count')
    retry_parser.set_defaults(func=cmd_retry_missing)
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Display data lake summary')
    summary_parser.set_defaults(func=cmd_summary)
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Query data lake')
    query_subparsers = query_parser.add_subparsers(dest='query_type', help='Query type')
    
    # Query: load
    query_load = query_subparsers.add_parser('load', help='Load option data')
    query_load.add_argument('--index', help='Index name')
    query_load.add_argument('--expiry', required=True, help='Expiry (YYYY-MM-DD)')
    query_load.add_argument('--strike', help='Strike price')
    query_load.add_argument('--type', dest='option_type', help='Option type (CE/PE)')
    query_load.add_argument('--interval', help='Interval')
    
    # Query: expiries
    query_subparsers.add_parser('expiries', help='List available expiries')
    
    # Query: strikes
    query_strikes = query_subparsers.add_parser('strikes', help='List available strikes')
    query_strikes.add_argument('--index', help='Index name')
    query_strikes.add_argument('--expiry', required=True, help='Expiry (YYYY-MM-DD)')
    query_strikes.add_argument('--interval', help='Interval')
    
    # Query: completeness
    query_comp = query_subparsers.add_parser('completeness', help='Data completeness report')
    query_comp.add_argument('--index', help='Index name')
    query_comp.add_argument('--expiry', required=True, help='Expiry (YYYY-MM-DD)')
    query_comp.add_argument('--interval', help='Interval')
    
    query_parser.set_defaults(func=cmd_query)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Load config
    try:
        config_loader = ConfigLoader(args.config)
        config = config_loader.load()
    except Exception as e:
        print(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Setup logging
    log_dir = config.get('logging', {}).get('log_dir', './logs')
    log_level = config.get('logging', {}).get('level', 'INFO')
    LoggerSetup.setup_logging(log_dir, log_level, console_output=True)
    
    logger = logging.getLogger(__name__)
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
    
    # Execute command
    if args.command in ['download']:
        api = setup_auth()
        args.func(args, config, api)
    else:
        args.func(args, config)


if __name__ == '__main__':
    main()
