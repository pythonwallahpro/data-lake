# ============================================================================
# DEPLOYMENT.md
# ============================================================================
# Deployment and Setup Checklist

## Pre-Deployment Checklist

- [ ] Python 3.8+ installed
- [ ] Virtual environment created and activated
- [ ] All dependencies installed from requirements.txt
- [ ] Angel Broking credentials obtained
- [ ] TOTP secret for 2FA configured
- [ ] `.env` file created with credentials

## Installation Steps

### 1. Environment Setup

```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Credentials

Create `.env` file in project root:

```
ANGEL_API_KEY=your_api_key_here
ANGEL_CLIENT_ID=your_client_id_here
ANGEL_CLIENT_PASSWORD=your_password_here
ANGEL_TOTP_SECRET=your_totp_secret_here
```

### 3. Configure Data Lake

Edit `config.yaml`:

```yaml
indices:
  - name: NIFTY
    exchange_segment: NFO
    instrument_type: OPTIDX
    strike_step: 50

expiry_mode: current
intervals:
  - ONE_MINUTE
  - FIVE_MINUTE

data_lake_path: ./data_lake
```

### 4. Test Authentication

```bash
python resource/auth_v2.py
```

Expected output:
```
===== ANGEL API SESSION DETAILS =====
API Key       : ...
Client ID     : ...
Auth Token    : ...
Refresh Token : ...
Feed Token    : ...

===== USER PROFILE =====
...
```

## Deployment Options

### Option 1: Local Machine (Development/Research)

```bash
# Run directly
python main.py download --index NIFTY --expiry current --interval ONE_MINUTE

# Or use CLI
python cli.py download --index NIFTY --expiry all
```

### Option 2: Server with Cron (Daily Updates)

Add to crontab:

```bash
# Download daily at 16:00 (after market close)
0 16 * * 1-5 cd /path/to/data_lake && /usr/bin/python3 main.py download --index NIFTY --expiry current --interval ONE_MINUTE >> logs/cron.log 2>&1

# Retry missing data daily at 17:00
0 17 * * 1-5 cd /path/to/data_lake && /usr/bin/python3 main.py retry-missing >> logs/cron.log 2>&1

# Validate data daily at 18:00
0 18 * * 1-5 cd /path/to/data_lake && /usr/bin/python3 main.py validate >> logs/cron.log 2>&1
```

### Option 3: Docker Deployment

Create `Dockerfile`:

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py", "download", "--index", "NIFTY", "--expiry", "current"]
```

Build and run:

```bash
docker build -t data-lake-builder .
docker run -e ANGEL_API_KEY=... -e ANGEL_CLIENT_ID=... data-lake-builder
```

## Post-Deployment Verification

### 1. Check Data Lake Structure

```bash
ls -la data_lake/
ls -la data_lake/raw/
ls -la data_lake/cleaned/
ls -la data_lake/metadata/
```

Expected output:
```
data_lake/
├── raw/
├── cleaned/
├── metadata/
└── logs/
```

### 2. Verify Progress Tracking

```bash
python main.py summary
```

Expected output:
```
Data Lake Summary:
  Raw Parquets: 250
  Cleaned Parquets: 0
  Indices in Raw: ['NIFTY']
  Indices in Cleaned: []

Progress Summary:
  total_tokens: 100
  completed: 75
  partial: 20
  failed: 5
  ...
```

### 3. Test Query Engine

```bash
python main.py query expiries
python main.py query load --index NIFTY --expiry 2025-11-04 --strike 23000 --type CE
```

### 4. Validate Data

```bash
python main.py validate --index NIFTY
```

## Production Monitoring

### Key Metrics to Monitor

1. **Download Success Rate**
   - Target: > 95%
   - Action: If < 95%, check API limits and network

2. **Data Completeness**
   - Target: > 99% of expected candles
   - Action: Run `retry-missing` if < 99%

3. **Validation Pass Rate**
   - Target: 100%
   - Action: Investigate failed validations

4. **Disk Usage**
   - Monitor: `data_lake/` size
   - Alert: If grows > 50% of available space

### Health Check Script

```bash
#!/bin/bash
# health_check.sh

echo "Data Lake Health Check"
echo "======================"

# Check disk space
echo -n "Disk Usage: "
du -sh data_lake/

# Check progress
echo -n "Download Progress: "
python main.py summary | grep "completed"

# Check latest download
echo -n "Latest Download: "
ls -lt logs/download.log | head -1

# Check for errors
echo -n "Recent Errors: "
tail -5 logs/errors.log
```

## Troubleshooting Deployment

### Issue: Import Errors

**Solution**: Ensure virtual environment is activated and all packages installed

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Issue: Authentication Failures

**Solution**: Verify credentials and TOTP secret

```bash
python resource/auth_v2.py
```

### Issue: No Data Downloaded

**Solution**: Check if expiry exists

```bash
python main.py query expiries
```

### Issue: Disk Space

**Solution**: Archive old expiries

```bash
# List expiries
python main.py query expiries

# Archive old expiry
tar -czf archive/NIFTY_2025-10-30.tar.gz \
  data_lake/raw/NIFTY/options/expiry=2025-10-30 \
  data_lake/cleaned/NIFTY/options/expiry=2025-10-30

# Remove from active
rm -rf data_lake/raw/NIFTY/options/expiry=2025-10-30
rm -rf data_lake/cleaned/NIFTY/options/expiry=2025-10-30
```

## Backup Strategy

### Daily Backups

```bash
#!/bin/bash
# backup.sh

BACKUP_DIR="/backups/data_lake"
DATE=$(date +%Y%m%d)

mkdir -p $BACKUP_DIR

# Backup progress database
cp data_lake/metadata/progress/progress.db \
   $BACKUP_DIR/progress_${DATE}.db.bak

# Backup metadata
tar -czf $BACKUP_DIR/metadata_${DATE}.tar.gz \
    data_lake/metadata/

# Keep only last 30 days
find $BACKUP_DIR -type f -mtime +30 -delete
```

## Scaling Considerations

### For Multiple Indices

```bash
# Run in parallel (screen/tmux)
screen -S nifty -d -m bash -c "python main.py download --index NIFTY --expiry all"
screen -S sensex -d -m bash -c "python main.py download --index SENSEX --expiry all"
```

### For Multiple Intervals

Data downloads all configured intervals automatically:

```yaml
intervals:
  - ONE_MINUTE
  - THREE_MINUTE
  - FIVE_MINUTE
```

### For High-Volume Queries

```python
# Use cleaned layer (fewer records)
df = query_engine.load_option_data(..., layer='cleaned')

# Use resampling to higher intervals
df_5min = query_engine.resample_interval(df, 'ONE_MINUTE', 'FIVE_MINUTE')
```

## Version Updates

### Update Procedure

1. Backup existing data lake
2. Update codebase
3. Update requirements.txt: `pip install -r requirements.txt --upgrade`
4. Test with small data download
5. Resume full downloads

## Maintenance

### Monthly Tasks

- [ ] Review error logs
- [ ] Check disk usage
- [ ] Verify data completeness
- [ ] Archive old expiries
- [ ] Update documentation

### Quarterly Tasks

- [ ] Review configuration
- [ ] Audit instrument master
- [ ] Performance analysis
- [ ] Update dependencies

---

**Deployment Date**: _________________
**Deployed By**: _________________
**Approval**: _________________
