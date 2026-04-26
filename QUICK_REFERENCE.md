# ============================================================================
# QUICK_REFERENCE.md
# ============================================================================
# Quick Reference Guide - Command Cheatsheet

## Environment Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate      # Windows

# Install dependencies
pip install -r requirements.txt

# Configure credentials
# Edit .env with Angel API credentials

# Test authentication
python resource/auth_v2.py
```

## Core Commands

### Download Operations

```bash
# Download current NIFTY expiry (1-minute candles)
python main.py download --index NIFTY --expiry current --interval ONE_MINUTE

# Download all SENSEX expiries (multiple intervals)
python main.py download --index SENSEX --expiry all --interval ONE_MINUTE,FIVE_MINUTE

# Download specific expiry (YYYY-MM-DD format)
python main.py download --index NIFTY --specific-expiry 2025-11-04 --interval ONE_MINUTE

# Download with custom lookback period (days)
python main.py download --index NIFTY --expiry current --lookback 180

# Download from CLI with full options
python cli.py download --index NIFTY --expiry all --interval ONE_MINUTE,THREE_MINUTE,FIVE_MINUTE
```

### Validation & Verification

```bash
# Validate all downloaded data
python main.py validate --index NIFTY

# View overall data lake summary
python main.py summary

# Retry missing data segments (up to max retries)
python main.py retry-missing --max-retries 3
```

### Query Operations

```bash
# List all available expiries
python main.py query expiries

# List available strikes for an expiry
python main.py query strikes --index NIFTY --expiry 2025-11-04 --interval ONE_MINUTE

# Load specific option data
python main.py query load --index NIFTY --expiry 2025-11-04 --strike 23000 --type CE --interval ONE_MINUTE

# Get data completeness report
python main.py query completeness --index NIFTY --expiry 2025-11-04 --interval ONE_MINUTE
```

## Python API Usage

### Basic Data Loading

```python
from modules.query_engine import QueryEngine
from datetime import date

# Initialize
engine = QueryEngine(data_lake_path="./data_lake")

# Load option
df = engine.load_option_data(
    index_name='NIFTY',
    expiry=date(2025, 11, 4),
    option_type='CE',
    strike=23000,
    interval='ONE_MINUTE'
)

print(f"Loaded {len(df)} records")
```

### Expiry Chain Operations

```python
# Load all options for an expiry
chain_df = engine.load_expiry_chain(
    'NIFTY', date(2025, 11, 4), 'ONE_MINUTE'
)

# Get available strikes
strikes = engine.get_available_strikes(
    'NIFTY', date(2025, 11, 4), 'ONE_MINUTE'
)

# Load ATM + OTM
atm_chain = engine.load_atm_chain(
    'NIFTY', 
    date(2025, 11, 4), 
    'ONE_MINUTE',
    atm_strikes=2,
    otm_strikes=3,
    spot_price=24500
)
```

### Data Analysis & Resampling

```python
# Resample to different interval
df_5min = engine.resample_interval(df, 'ONE_MINUTE', 'FIVE_MINUTE')

# Check data completeness
report = engine.get_data_completeness_report(
    'NIFTY', date(2025, 11, 4), 'ONE_MINUTE'
)

# Find missing segments
missing = engine.find_missing_data_segments(
    'NIFTY', date(2025, 11, 4), 'ONE_MINUTE'
)

# Get data lake summary
summary = engine.get_data_lake_summary()
```

## Configuration Reference

### config.yaml Sections

```yaml
# Which indices to download
indices:
  - name: NIFTY
    exchange_segment: NFO
    strike_step: 50

# Expiry selection
expiry_mode: current        # current, specific, or all
specific_expiry: null       # YYYY-MM-DD format

# Time intervals to download
intervals:
  - ONE_MINUTE
  - THREE_MINUTE
  - FIVE_MINUTE

# Download settings
download_config:
  mode: incremental         # full or incremental
  lookback_days: 365
  max_retries: 3
  requests_per_minute: 60
  chunk_size_days: 7

# Progress tracking backend
progress_tracking:
  backend: sqlite          # sqlite or json

# Logging
logging:
  level: INFO
  log_dir: ./logs
```

## Data Lake Navigation

```bash
# View folder structure
ls -la data_lake/
ls -la data_lake/raw/
ls -la data_lake/cleaned/
ls -la data_lake/metadata/

# View parquet files for an expiry
find data_lake/raw/NIFTY -name "*.parquet" | head -5

# Check progress database
sqlite3 data_lake/metadata/progress/progress.db "SELECT COUNT(*) FROM progress;"

# View missing segments
cat data_lake/metadata/missing_data/missing_segments.json
```

## Monitoring & Logging

```bash
# View main log
tail -f logs/data_lake.log

# View download log
tail -f logs/download.log

# View validation log
tail -f logs/validation.log

# View errors
tail -f logs/errors.log

# Count successful downloads
grep -c "Downloaded.*records" logs/download.log
```

## Common Workflows

### Daily Update (Cron)

```bash
#!/bin/bash
# daily_update.sh

cd /path/to/data_lake

# Download new data
python main.py download --index NIFTY --expiry current --interval ONE_MINUTE

# Retry missing
python main.py retry-missing --max-retries 3

# Validate
python main.py validate --index NIFTY

# Log summary
python main.py summary >> logs/summary.log
```

### Backtesting Data Prep

```python
from modules.query_engine import QueryEngine
from datetime import date
import pandas as pd

engine = QueryEngine()

# Load all data for analysis
df = engine.load_expiry_chain(
    'NIFTY',
    date(2025, 11, 4),
    'ONE_MINUTE'
)

# Resample to 5-minute for faster analysis
df_5min = engine.resample_interval(df, 'ONE_MINUTE', 'FIVE_MINUTE')

# Export for backtesting
df_5min.to_csv('backtest_data.csv', index=False)
```

### Data Integrity Check

```bash
# Run validation
python main.py validate --index NIFTY

# Check completeness
python main.py query completeness --index NIFTY --expiry 2025-11-04

# Retry any missing data
python main.py retry-missing

# Verify again
python main.py summary
```

## Troubleshooting Quick Fixes

```bash
# Auth failed?
python resource/auth_v2.py

# No data?
python main.py query expiries

# Reset progress?
rm data_lake/metadata/progress/progress.db

# Clear logs?
rm logs/*.log

# Check disk usage
du -sh data_lake/

# Find large files
find data_lake -name "*.parquet" -size +100M

# List all symbols downloaded
find data_lake/raw -name "*.parquet" | xargs -I {} basename {} | sort
```

## Performance Tips

```bash
# For faster downloads, increase parallel downloads in config.yaml
parallel_downloads: 10

# For better compression, use zstd instead of snappy
compression: zstd

# For incremental updates, use mode: incremental
download_config:
  mode: incremental  # Only downloads new data

# For faster queries, use cleaned layer (smaller files)
df = query_engine.load_option_data(..., layer='cleaned')
```

---

**Last Updated**: 2026-04-27
**Version**: 1.0.0
