# Agmarknet Data Pipeline - Quick Start Guide

## What This Does

Automatically fetches agricultural market price data from India's Agmarknet dataset using a year-wise incremental approach. The pipeline:

- ✅ Fetches data year by year (not global offsets)
- ✅ Only downloads missing years per crop
- ✅ Never duplicates existing data
- ✅ Prevents year gaps through sequential processing
- ✅ Saves progress automatically (resumable)
- ✅ Handles API failures gracefully with retry logic
- ✅ Works with GitHub Actions (runs every 3 hours)

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set API key
export DATA_GOV_API_KEY="your_api_key_here"
```

## Quick Start

### Run the Pipeline

```bash
python fetch_data.py
```

The pipeline will:
1. Check existing CSVs to determine what years are already fetched
2. Fetch missing years sequentially (2007 → 2026)
3. Save data to `data/crops/{crop_name}.csv`
4. Track progress in `data/progress.json`
5. Run for 2h55m then exit cleanly

### Validate Data

```bash
python validate_data.py
```

Checks for:
- Duplicate records
- Year coverage
- Progress consistency

### Check Progress

```bash
cat data/progress.json
```

Shows last completed year for each crop.

## How It Works

### Year-wise Architecture

**Old Approach (❌ Problems):**
```
API Call: offset=0      → 1000 records
API Call: offset=1000   → 1000 records
...
API Call: offset=4000000 → 💥 Timeout, data loss
```

**New Approach (✅ Solution):**
```
Year 2024:
  API Call: offset=0    → 1000 records
  API Call: offset=1000 → 1000 records
  ...
  API Call: offset=45000 → Last records ✅
  
Year 2025:
  API Call: offset=0    → 1000 records (offset resets!)
  ...
```

### API Filtering

The pipeline uses year-based filtering:

```python
params = {
    "filters[Arrival_Date]": "2025"  # Get all year 2025 data
}
```

This returns ALL commodities for that year. The pipeline then:
1. Groups by commodity
2. Saves to respective CSV files
3. Updates progress for each crop

### Progress Tracking

**progress.json structure:**
```json
{
  "wheat": 2024,
  "rice": 2024,
  "onion": 2025
}
```

Each crop tracks its last **completed** year independently.

### Deduplication

Uses composite key to ensure uniqueness:
- State
- District  
- Market
- Commodity
- Arrival_Date

Deduplication happens at two levels:
1. Within each API batch
2. When appending to existing CSV

### No Overlap, No Gaps

**Overlap Prevention:**
- Progress only marked after year fully completes
- CSV analysis detects existing years
- Skip logic prevents re-downloading

**Gap Prevention:**
- Sequential year processing (2007 → 2026)
- No year skipping
- Progress saved after each successful page
- Interrupted years restart from beginning (no partial state)

## File Structure

```
.
├── fetch_data.py           # Main pipeline script
├── validate_data.py        # Data validation script
├── PIPELINE_DOCS.md        # Technical documentation
├── requirements.txt        # Python dependencies
├── data/
│   ├── progress.json       # Progress tracking
│   └── crops/
│       ├── wheat.csv       # One CSV per crop
│       ├── rice.csv
│       ├── onion.csv
│       └── ...
```

## CSV Format

Each CSV file contains:

```csv
State,District,Market,Commodity,Variety,Grade,Arrival_Date,Min_Price,Max_Price,Modal_Price,Commodity_Code
Maharashtra,Pune,Pune,Wheat,Local,FAQ,2024-01-15,2500,2800,2650,1
```

## GitHub Actions Integration

### Workflow Configuration

```yaml
name: Fetch Agmarknet Data

on:
  schedule:
    - cron: '0 */3 * * *'  # Every 3 hours
  workflow_dispatch:  # Manual trigger

jobs:
  fetch:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: pip install -r requirements.txt
      
      - name: Fetch data
        env:
          DATA_GOV_API_KEY: ${{ secrets.DATA_GOV_API_KEY }}
        run: python fetch_data.py
      
      - name: Commit changes
        run: |
          git config --global user.name "GitHub Actions"
          git config --global user.email "actions@github.com"
          git add data/
          git commit -m "Update agricultural data" || echo "No changes"
          git push
```

### Execution Pattern

- **First run**: Fetches 2007-2008 (depends on data density)
- **Second run (3h later)**: Continues from 2009
- **Subsequent runs**: Sequentially work through years
- **After catch-up**: Each run checks for new data in current year

## Configuration

### Time Limits

```python
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60  # 2h 55m
```

Safe for 3-hour GitHub Actions limit.

### API Settings

```python
LIMIT = 1000              # Records per call (API max)
REQUEST_TIMEOUT = 30      # seconds
API_CALL_DELAY = 1.0      # Sleep between calls
```

### Retry Configuration

```python
SHORT_RETRIES = 5         # Exponential backoff attempts
SHORT_BACKOFF_BASE = 2    # 2, 4, 8, 16, 32 seconds
LONG_SLEEP = 300          # 5 min when API very unstable
```

### Year Range

```python
START_YEAR = 2007         # Earliest data in Agmarknet
CURRENT_YEAR = 2026       # Auto-updated to datetime.now().year
```

## Error Handling

### Network Failures

- **Retry**: 5 attempts with exponential backoff (2s → 32s)
- **Long sleep**: 5-minute wait if all retries fail
- **Final retry**: One more attempt after long sleep
- **Fallback**: Stop year, save progress, resume next run

### Rate Limiting (HTTP 429)

- Same retry strategy as network failures
- Exponential backoff prevents hammering API

### Time Limit Reached

- Pipeline checks time before each API call
- Exits cleanly when approaching 2h55m
- Progress saved, resumes from next year

### API Instability

- Does NOT advance year/offset unless data successfully fetched
- Progress only saved after successful processing
- Resume picks up exactly where it left off

## Monitoring

### Check Data Coverage

```python
import pandas as pd
import glob

for csv in glob.glob("data/crops/*.csv")[:5]:
    df = pd.read_csv(csv)
    df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])
    print(f"{csv}: {df['Arrival_Date'].dt.year.min()}-{df['Arrival_Date'].dt.year.max()} ({len(df)} rows)")
```

### Find Missing Years

```python
import pandas as pd

df = pd.read_csv("data/crops/wheat.csv")
df['Year'] = pd.to_datetime(df['Arrival_Date']).dt.year

all_years = set(range(2007, 2027))
existing_years = set(df['Year'].unique())
missing = all_years - existing_years

if missing:
    print(f"Missing: {sorted(missing)}")
else:
    print("Complete!")
```

### Check for Duplicates

```python
import pandas as pd

df = pd.read_csv("data/crops/wheat.csv")
key_cols = ["State", "District", "Market", "Commodity", "Arrival_Date"]

before = len(df)
df_clean = df.drop_duplicates(subset=key_cols)
after = len(df_clean)

print(f"Duplicates: {before - after}")
```

## Troubleshooting

### Progress.json Corrupted

```bash
# Delete and rebuild from CSVs
rm data/progress.json
python fetch_data.py  # Will auto-detect existing years
```

### Re-fetch a Specific Crop

```bash
# Remove crop CSV
rm data/crops/wheat.csv

# Remove from progress.json
# Edit data/progress.json, remove "wheat" entry

# Next run will re-fetch
python fetch_data.py
```

### API Key Issues

```bash
# Verify API key is set
echo $DATA_GOV_API_KEY

# Test API manually
curl "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24?api-key=$DATA_GOV_API_KEY&format=json&limit=10"
```

### Year Never Completes

Some years have massive amounts of data. Check API response:

```bash
curl "https://api.data.gov.in/resource/35985678-0d79-46b4-9ed6-6f13308a1d24?api-key=$DATA_GOV_API_KEY&format=json&limit=10&filters[Arrival_Date]=2024&offset=0" | jq '.total'
```

If total > 100,000, that year will take multiple runs to complete.

### Natural Year Gaps

**Important**: Not all crops are traded every year! Year gaps are often natural:

- Seasonal crops (only certain months)
- Regional crops (only specific states)
- New introductions (started trading recently)
- Discontinued commodities

The pipeline correctly handles this - it doesn't force-fetch years with no data.

## Performance

### Expected Throughput

- **API call**: ~2-5 seconds
- **Records per call**: 1000
- **Year completion**: 30-60K records (varies)
- **Per 3h run**: ~2-3 years of data

### Bottlenecks

1. API rate limits (1s delay between calls)
2. Network latency
3. Large years (2023-2024 have more data)

## Data Quality Notes

### Current Data Findings

Based on validation of existing data:

- **Total crops**: 328 CSV files
- **Date range**: 2007-2024
- **Minor duplicates**: ~0.08% (will be cleaned on next run)
- **Year gaps**: Natural - many crops not traded every year
- **Data integrity**: Good overall quality

### Why Year Gaps Exist

Example from validation:
- `ajwan.csv`: Missing years [2008, 2010, 2015, 2017, 2020]
- **Reason**: Ajwan is a specialty spice, not consistently traded

This is **expected behavior**, not a pipeline bug.

## Production Readiness

### ✅ Implemented

- Year-wise fetching with small offsets
- Per-crop progress tracking
- Deduplication via composite key
- Exponential backoff retry logic
- Time-limited execution (GitHub Actions safe)
- Resumable architecture
- CSV append-only writes
- Graceful error handling

### ⚠️ Limitations

- No parallel year fetching (sequential only)
- CSV storage (not database)
- 3-hour execution window (may need multiple runs to catch up)
- No real-time updates (3-hour polling)
- Natural data gaps not backfilled

### 🚀 Future Enhancements

- Parallel year fetching (async/await)
- Database backend (PostgreSQL/SQLite)
- Data validation and anomaly detection
- Compression for older years
- Real-time WebSocket updates
- Alerting on pipeline failures

## API Reference

### Data.gov.in Agmarknet

- **Resource ID**: `35985678-0d79-46b4-9ed6-6f13308a1d24`
- **Base URL**: `https://api.data.gov.in/resource/{resource_id}`
- **Format**: JSON
- **Authentication**: API key via query parameter
- **Rate limit**: ~1 req/sec (soft limit)

### Supported Filters

```python
"filters[Arrival_Date]": "YYYY"       # Year filter
"filters[Commodity]": "Wheat"          # Specific commodity
"filters[State]": "Maharashtra"        # Specific state
```

## License

Data source: data.gov.in - Open Government Data Platform  
License: Government Open Data License - India

## Support

For issues:
1. Check `PIPELINE_DOCS.md` for technical details
2. Run `validate_data.py` to diagnose issues
3. Check `data/progress.json` for pipeline state
4. Review API response manually if needed

---

**Last Updated**: February 2026  
**Pipeline Version**: 2.0 (Year-wise Architecture)
