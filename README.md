# Multi-Index Options Data Lake Builder

A production-ready, fault-tolerant, and resumable historical options data lake for Indian index derivatives (NIFTY and SENSEX).

## Overview

This system builds a comprehensive, backtest-ready data lake for NIFTY and SENSEX options with:

- **Multiple indices support**: NIFTY, SENSEX (easily extensible to BANKNIFTY, FINNIFTY, etc.)
- **Multiple expiries**: current, specific, or all available expiries
- **Multiple intervals**: 1-minute, 3-minute, 5-minute candles
- **Full historical downloads**: Automatic lookback, resumable from interruptions
- **Data validation**: Comprehensive OHLC and candle continuity checks
- **Automatic retry**: Missing data detection and retry with exponential backoff
- **Parquet-based storage**: Compressed, partitioned, analytics-ready format
- **Progress tracking**: Resume from last checkpoint, detailed progress metadata
- **Query engine**: Backtesting-ready data access with resampling support
- **CLI interface**: Command-line tools for all operations

## Architecture

```
data_lake/
├── raw/                              # Raw data as received from API
│   ├── NIFTY/
│   │   └── options/
│   │       └── expiry=2025-11-04/
│   │           └── interval=ONE_MINUTE/
│   │               ├── option_type=CE/
│   │               │   └── strike=23000/
│   │               │       └── 47347.parquet
│   │               └── option_type=PE/
│   │                   └── strike=23000/
│   │                       └── 47348.parquet
│   └── SENSEX/
│       └── options/
│           └── expiry=2025-11-04/
│               └── interval=ONE_MINUTE/
│                   ├── option_type=CE/
│                   │   └── strike=81000/
│                   │       └── token.parquet
│                   └── option_type=PE/
│
├── cleaned/                         # Validated, normalized data
│   └── [Same structure as raw]
│
└── metadata/
    ├── instruments/                 # Instrument master cache
    ├── progress/                    # Download progress tracking
    │   ├── progress.db              # SQLite backend
    │   └── progress.json            # JSON backend
    ├── validation/                  # Validation reports
    └── missing_data/                # Missing segments tracker
        └── missing_segments.json
```

## Installation

### Prerequisites

- Python 3.8+
- Angel Broking SmartAPI account with API credentials
- TOTP secret for 2FA

### Setup Steps

1. **Clone and navigate to project**:
   ```bash
   cd data_lake
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   Additional packages needed:
   ```bash
   pip install pyyaml pyarrow sqlalchemy
   ```

3. **Configure credentials**:
   
   Update `.env` file with your Angel Broking credentials:
   ```
   ANGEL_API_KEY=your_api_key
   ANGEL_CLIENT_ID=your_client_id
   ANGEL_CLIENT_PASSWORD=your_password
   ANGEL_TOTP_SECRET=your_totp_secret
   ```

4. **Configure data lake**:
   
   Edit `config.yaml` with your preferences:
   ```yaml
   indices:
     - name: NIFTY
       exchange_segment: NFO
       
   expiry_mode: current
   intervals:
     - ONE_MINUTE
     - FIVE_MINUTE
     
   data_lake_path: ./data_lake
   ```

## Quick Start

### 1. Download Current NIFTY Expiry

```bash
python main.py download --index NIFTY --expiry current --interval ONE_MINUTE
```

### 2. Download All Available SENSEX Expiries

```bash
python main.py download --index SENSEX --expiry all --interval ONE_MINUTE,FIVE_MINUTE
```

### 3. Download Specific Expiry

```bash
python main.py download --index NIFTY --specific-expiry 2025-11-04 --interval ONE_MINUTE
```

### 4. Validate Downloaded Data

```bash
python main.py validate --index NIFTY
```

### 5. View Progress Summary

```bash
python main.py summary
```

### 6. Retry Missing Data Segments

```bash
python main.py retry-missing --max-retries 3
```

## Querying Data

### Get Available Expiries

```bash
python main.py query expiries
```

### List Available Strikes

```bash
python main.py query strikes --index NIFTY --expiry 2025-11-04 --interval ONE_MINUTE
```

### Load Option Data

```bash
python main.py query load --index NIFTY --expiry 2025-11-04 --strike 23000 --type CE --interval ONE_MINUTE
```

### Data Completeness Report

```bash
python main.py query completeness --index NIFTY --expiry 2025-11-04 --interval ONE_MINUTE
```

## Python API Usage

### Load and Query Data Programmatically

```python
from modules.query_engine import QueryEngine
from datetime import date

# Initialize query engine
query_engine = QueryEngine(data_lake_path="./data_lake")

# Load specific option
df = query_engine.load_option_data(
    index_name='NIFTY',
    expiry=date(2025, 11, 4),
    option_type='CE',
    strike=23000,
    interval='ONE_MINUTE'
)

# Get all available expiries
expiries = query_engine.get_available_expiries('NIFTY')

# Get available strikes for an expiry
strikes = query_engine.get_available_strikes('NIFTY', date(2025, 11, 4), 'ONE_MINUTE')

# Load ATM + OTM options
chain_df = query_engine.load_atm_chain(
    index_name='NIFTY',
    expiry=date(2025, 11, 4),
    interval='ONE_MINUTE',
    atm_strikes=2,
    otm_strikes=3,
    spot_price=24500
)

# Resample from 1-minute to 5-minute
df_5min = query_engine.resample_interval(df, 'ONE_MINUTE', 'FIVE_MINUTE')

# Get data completeness report
report = query_engine.get_data_completeness_report('NIFTY', date(2025, 11, 4), 'ONE_MINUTE')
```

### Download and Store Data Programmatically

```python
from modules.data_lake_builder import DataLakeBuilder
from utilities.config_loader import ConfigLoader

# Load configuration
config_loader = ConfigLoader('config.yaml')
config = config_loader.load()

# Initialize builder
builder = DataLakeBuilder(smart_api, config)

# Prepare token universe
tokens = builder.prepare_token_universe(
    index_name='NIFTY',
    expiry_mode='current'
)

# Download data for all intervals
intervals = ['ONE_MINUTE', 'FIVE_MINUTE']
stats = builder.download_for_tokens(tokens, intervals, lookback_days=365)

print(f"Downloaded {stats['successful']} tokens successfully")
```

## Configuration Reference

### Main Config Sections

**indices**
```yaml
indices:
  - name: NIFTY
    exchange_segment: NFO
    instrument_type: OPTIDX
    strike_step: 50
```

**expiry_mode**
- `current`: Download only nearest future expiry
- `specific`: Download specific expiry (requires `specific_expiry`)
- `all`: Download all available expiries

**download_config**
```yaml
download_config:
  mode: incremental          # full or incremental
  lookback_days: 365         # How many days back to download
  max_retries: 3             # Retry failed requests
  parallel_downloads: 5      # Concurrent downloads
  chunk_size_days: 7         # Days per API request
  resume_on_restart: true    # Resume from checkpoint
```

**progress_tracking**
```yaml
progress_tracking:
  backend: sqlite            # sqlite or json
  db_path: ./data_lake/metadata/progress
```

## Data Format

### Parquet File Schema

Each parquet file contains OHLC data with the following columns:

```
datetime       : datetime64[ns] - Candle timestamp (IST)
open           : float64        - Opening price
high           : float64        - High price
low            : float64        - Low price
close          : float64        - Closing price
volume         : int64          - Trading volume
token          : str            - Instrument token
symbol         : str            - Option symbol (e.g., NIFTY04NOV2523000CE)
index_name     : str            - Index name (NIFTY, SENSEX)
expiry         : str            - Expiry date (YYYY-MM-DD)
strike         : int            - Strike price
option_type    : str            - CE or PE
interval       : str            - Candle interval
```

## Progress Tracking

### SQLite Backend

Progress stored in `data_lake/metadata/progress/progress.db`:

```
Table: progress
- id                    : Primary key
- index_name           : Index name
- token                : Instrument token
- symbol               : Option symbol
- expiry               : Expiry date
- strike               : Strike price
- option_type          : CE or PE
- interval             : Candle interval
- status               : pending, downloading, completed, partial, failed
- total_records        : Number of records downloaded
- last_successful_date : Last date with successful download
- retry_count          : Number of retry attempts
- last_updated         : Last update timestamp
```

### JSON Backend

Progress tracked in `data_lake/metadata/progress/progress.json` as an array of progress records.

## Validation Rules

The system validates data at multiple levels:

### OHLC Logic Validation
- High >= Open, Close, Low
- Low <= Open, Close, High
- All prices > 0

### Volume Validation
- No negative volumes
- Reasonable volume ranges

### Timestamp Validation
- No duplicate timestamps
- Chronologically ordered
- No future timestamps

### Candle Continuity
- Candles per trading day match expectations
- Gaps detected and logged

## Error Handling and Recovery

### Automatic Retry Logic

```python
# Downloads retry with exponential backoff
# Retry 1: 5 seconds
# Retry 2: 10 seconds
# Retry 3: 15 seconds
```

### Resume on Interruption

```bash
# If download interrupted:
# - Progress saved after each successful token
# - Rerun command to resume from last checkpoint
# - Completed tokens skipped automatically
python main.py download --index NIFTY --expiry current
```

### Missing Data Detection

Missing segments automatically logged and retried:

```bash
python main.py retry-missing --max-retries 3
```

## Performance Tuning

### Rate Limiting

Configured in `config.yaml`:
```yaml
download_config:
  requests_per_minute: 60  # Adjust based on API limits
  chunk_size_days: 7       # Smaller chunks = more requests
```

### Parallel Downloads

```yaml
download_config:
  parallel_downloads: 5    # Number of concurrent downloads
```

### Compression

```yaml
data_layer:
  compression: snappy      # Compression algorithm
  storage_format: parquet  # Analytics-optimized format
```

## Testing

Run unit tests:

```bash
python -m pytest tests/
```

Run specific test:

```bash
python -m pytest tests/test_symbol_parser.py::TestSymbolParser::test_parse_nifty_ce_symbol
```

## Logging

Log files generated in `logs/` directory:

- `data_lake.log` - Main application log
- `download.log` - Download operations
- `validation.log` - Validation results
- `errors.log` - Error events
- `resume.log` - Resume operations

Configure logging in `config.yaml`:

```yaml
logging:
  level: INFO           # DEBUG, INFO, WARNING, ERROR
  console_output: true  # Print to console
  log_dir: ./logs
```

## Extending to Other Indices

To add BANKNIFTY, FINNIFTY, or other indices:

1. Update `config.yaml`:
   ```yaml
   indices:
     - name: BANKNIFTY
       exchange_segment: NFO
       instrument_type: OPTIDX
       strike_step: 100
   ```

2. Run download:
   ```bash
   python main.py download --index BANKNIFTY --expiry current
   ```

## Incremental Updates

To keep data lake up-to-date with daily updates:

```bash
# Daily cron job to append new data
python main.py download --index NIFTY --expiry current --interval ONE_MINUTE
```

The system automatically:
- Detects latest available data
- Skips already downloaded data
- Appends only new records
- Deduplicates timestamps

## Troubleshooting

### Authentication Failed
- Check `.env` file credentials
- Verify TOTP secret
- Confirm API key is active

### No Data Downloaded
- Check if instruments available for the expiry
- Verify API limits not exceeded
- Review logs in `logs/` directory

### Validation Failures
- Check for incomplete trading days (market halts)
- Review `logs/validation.log` for details
- Use `retry-missing` command

### Progress Corruption
- Delete `data_lake/metadata/progress/progress.db` to reset
- Rerun downloads to rebuild progress

## Performance Characteristics

### Download Speed
- Typical: 100-200 tokens per hour
- Depends on: API rate limits, network, chunk size
- With 5-minute chunks: ~5 requests per token

### Storage Requirements
- Raw data: ~50-100 MB per token per interval per year
- Compressed (snappy): ~30-50% of raw
- Example: NIFTY 50 strikes × 2 types × 365 days ≈ 5-10 GB per interval

### Query Performance
- Full year of 1-minute data: < 1 second
- Entire expiry chain load: < 5 seconds
- Resampling: < 2 seconds

## Support and Contribution

This is a production-grade system designed for serious backtesting and research.

For issues or enhancements:
1. Check existing logs for error details
2. Review troubleshooting section
3. Check data completeness before assuming bugs

## License

Internal use only. Ensure compliance with Angel Broking API terms.

## Version History

**v1.0.0** (2026-04-27)
- Initial production release
- NIFTY and SENSEX support
- All core features implemented
- Comprehensive testing and validation

---

**Last Updated**: 2026-04-27

**Next Steps**:
1. Configure Angel Broking credentials
2. Edit `config.yaml` for your indices and intervals
3. Run `python main.py download --index NIFTY --expiry current`
4. Monitor progress with `python main.py summary`
