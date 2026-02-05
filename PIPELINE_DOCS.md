# Agmarknet Data Pipeline - Technical Documentation

## Overview

This is a production-grade data pipeline for fetching agricultural market data from the Agmarknet dataset via data.gov.in API. The pipeline uses a **year-wise incremental approach** to fetch data efficiently while avoiding API instability and data loss.

## Architecture

### Why Year-wise Fetching?

The previous global offset-based approach had critical issues:

1. **High Offsets (4M+)**: As data accumulates, offsets become extremely large, causing API timeouts
2. **API Instability**: Large offsets make the API unreliable
3. **Silent Data Loss**: Missing records in large offset ranges go unnoticed
4. **Non-resumable**: No way to track which data is fetched

**Year-wise fetching solves these problems:**

- ✅ **Small Offsets**: Within each year, offsets reset (typically < 100K per year)
- ✅ **API Stability**: Smaller requests are more reliable
- ✅ **No Gaps**: Sequential year processing prevents missing data
- ✅ **Resumable**: Progress tracked per crop per year
- ✅ **Incremental**: Only fetches missing years

### API Filtering

The Agmarknet API supports year-based filtering:

```python
params = {
    "filters[Arrival_Date]": "2024"  # Fetches all records where year = 2024
}
```

This returns ALL records where the `Arrival_Date` year matches, regardless of commodity. The pipeline then groups by commodity and saves to respective CSV files.

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Start                          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Load progress.json (crop → last_completed_year)            │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  For each year: START_YEAR (2007) → CURRENT_YEAR (2026)    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
        ┌─────────────┴─────────────┐
        │   Fetch year with         │
        │   offset-based pagination │
        │   (offset resets per year)│
        └─────────────┬─────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  API: filters[Arrival_Date] │
        │  Returns all commodities    │
        │  for that year              │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Process & Clean:           │
        │  - Parse dates              │
        │  - Convert prices           │
        │  - Drop invalid rows        │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Group by Commodity         │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  For each crop:             │
        │  1. Load existing CSV       │
        │  2. Append new data         │
        │  3. Deduplicate using:      │
        │     (State, District,       │
        │      Market, Commodity,     │
        │      Arrival_Date)          │
        │  4. Save CSV                │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Update progress:           │
        │  crop_key → year            │
        │  Save progress.json         │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  offset += 1000             │
        │  Continue until year done   │
        └─────────────┬───────────────┘
                      │
                      ▼
        ┌─────────────────────────────┐
        │  Move to next year          │
        └─────────────────────────────┘
```

## Progress Tracking

### progress.json Structure

```json
{
  "wheat": 2024,
  "rice": 2023,
  "onion": 2024,
  "potato": 2025
}
```

**Key Points:**
- Each crop tracks its last **completed** year
- Progress is saved after each successful page fetch
- If interrupted, pipeline resumes from last checkpoint
- Different crops can be at different years (no global offset!)

### How Overlap is Prevented

1. **Year-level tracking**: `progress[crop] = year` only after year completes
2. **CSV analysis**: On startup, pipeline scans existing CSVs to detect which years already exist
3. **Skip logic**: Before processing, checks if year is already in CSV
4. **Deduplication**: Final safety net using composite key

### How Gaps are Prevented

1. **Sequential processing**: Years processed in order (2007 → 2026)
2. **Progress only on success**: Year marked complete only after all data fetched
3. **No skipping**: Pipeline never skips a year
4. **Resume safety**: Interrupted years restart from beginning (no partial year state)

## Deduplication Strategy

### Composite Key

```python
KEY_COLUMNS = ["State", "District", "Market", "Commodity", "Arrival_Date"]
```

This uniquely identifies each market price record.

### Multi-level Deduplication

1. **Batch-level**: Within each API response, drop duplicates
2. **Append-level**: When appending to CSV, merge with existing and deduplicate
3. **In-place**: Pandas `drop_duplicates()` with `keep='first'`

## Error Handling

### Retry Strategy

```
API Call Failed
    │
    ▼
Retry 1: Wait 2^1 = 2 seconds
    │
    ▼
Retry 2: Wait 2^2 = 4 seconds
    │
    ▼
Retry 3: Wait 2^3 = 8 seconds
    │
    ▼
Retry 4: Wait 2^4 = 16 seconds
    │
    ▼
Retry 5: Wait 2^5 = 32 seconds
    │
    ▼
All Failed → Sleep 300 seconds (5 minutes)
    │
    ▼
Retry once more → If failed, stop year
```

### Failure Modes

| Failure Type | Behavior | Resume Behavior |
|-------------|----------|----------------|
| Network timeout | Exponential backoff retry | Continue from same offset |
| HTTP 429 (rate limit) | Exponential backoff retry | Continue from same offset |
| HTTP 5xx | Exponential backoff retry | Continue from same offset |
| All retries fail | Long sleep (5 min) + 1 retry | If still fails, stop year |
| Time limit reached | Save progress, exit cleanly | Next run continues from next year |

## Configuration

### Time Limits

```python
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60  # 2h 55m
```

- Pipeline runs for **2 hours 55 minutes**
- Leaves 5-minute buffer for GitHub Actions (3-hour limit)
- Checks time before each API call
- Exits cleanly when limit approached

### API Constraints

```python
LIMIT = 1000              # Records per API call (max supported)
REQUEST_TIMEOUT = 30      # seconds
API_CALL_DELAY = 1.0      # Sleep between calls to avoid rate limiting
```

### Retry Configuration

```python
SHORT_RETRIES = 5         # Quick retries with exponential backoff
SHORT_BACKOFF_BASE = 2    # 2^attempt seconds
LONG_SLEEP = 300          # 5 minutes when API consistently fails
```

## CSV Structure

### File Naming

```
data/crops/{crop_name}.csv
```

Commodity names are sanitized:
- Lowercase
- Special characters removed
- Spaces → underscores

Examples:
- "Wheat" → `wheat.csv`
- "Bengal Gram Dal (Chana Dal)" → `bengal_gram_dal_chana_dal.csv`

### CSV Columns

```
State,District,Market,Commodity,Variety,Grade,Arrival_Date,Min_Price,Max_Price,Modal_Price,Commodity_Code
```

### Append-only Behavior

- Never overwrites existing CSVs
- Always appends new data
- Deduplicates after append
- Maintains chronological order not enforced (sorted on read if needed)

## Execution Flow

### GitHub Actions Schedule

```yaml
schedule:
  - cron: '0 */3 * * *'  # Every 3 hours
```

### Run Sequence

1. **First Run (0:00)**: Fetches 2007-2008 (2h55m limit)
2. **Second Run (3:00)**: Resumes from 2009
3. **Third Run (6:00)**: Continues years...
4. **Eventually**: Catches up to current year
5. **Maintenance Mode**: Each run checks for new years, fetches incrementally

### Cold Start vs Warm Start

**Cold Start** (no progress.json):
- Scans all existing CSVs
- Detects existing years
- Starts from max(existing_years) + 1

**Warm Start** (progress.json exists):
- Loads progress
- Skips completed years
- Continues from last year

### Example Execution

```
🚜 AGMARKNET DATA PIPELINE - YEAR-WISE INCREMENTAL FETCHING
======================================================================
Started at: 2026-02-05 10:00:00
Data range: 2007 to 2026
Max runtime: 2h 55m 0s
Data directory: data/crops
======================================================================

📋 Loaded progress for 324 crops

============================================================
📅 Fetching Year: 2025
============================================================
📊 Year 2025 | Offset 0 | Batch: 1000 records | Valid: 987 | Saved: 987 | Crops: 143
📊 Year 2025 | Offset 1000 | Batch: 1000 records | Valid: 995 | Saved: 1982 | Crops: 201
...
✅ Year 2025 completed | Total fetched: 45230 | Total saved: 45100 | Crops updated: 324

============================================================
📅 Fetching Year: 2026
============================================================
...
```

## Data Integrity Guarantees

### What is Guaranteed

✅ **No duplicates within a crop**: Composite key ensures uniqueness  
✅ **No missing years**: Sequential processing prevents gaps  
✅ **No data loss on interruption**: Progress saved after each page  
✅ **Idempotent reruns**: Safe to run multiple times, won't duplicate  
✅ **Resumable**: Always continues from last checkpoint  

### What is NOT Guaranteed

❌ **Data completeness if API drops records**: Pipeline can't detect if API silently drops data  
❌ **Immediate consistency**: 3-hour delay between updates  
❌ **Chronological CSV order**: Rows appended as received (sort on read if needed)  

## Monitoring & Debugging

### Check Progress

```bash
cat data/progress.json
```

Shows last completed year for each crop.

### Check Data Coverage

```python
import pandas as pd
import glob

for csv_file in glob.glob("data/crops/*.csv"):
    df = pd.read_csv(csv_file)
    df['Arrival_Date'] = pd.to_datetime(df['Arrival_Date'])
    min_year = df['Arrival_Date'].dt.year.min()
    max_year = df['Arrival_Date'].dt.year.max()
    print(f"{csv_file}: {min_year}-{max_year} ({len(df)} rows)")
```

### Look for Gaps

```python
import pandas as pd

df = pd.read_csv("data/crops/wheat.csv")
df['Year'] = pd.to_datetime(df['Arrival_Date']).dt.year
years = set(df['Year'].unique())

all_years = set(range(2007, 2027))
missing = all_years - years
if missing:
    print(f"Missing years: {sorted(missing)}")
else:
    print("No gaps!")
```

## Maintenance

### Adding New Crops

No action needed - pipeline automatically discovers new commodities from API responses.

### Handling API Changes

If API structure changes:
1. Update `KEY_COLUMNS` if primary key changes
2. Update `process_and_clean_records()` for new fields
3. Update `filters[Arrival_Date]` if filtering logic changes

### Resetting a Crop

To re-fetch a crop from scratch:

```bash
# Remove CSV
rm data/crops/wheat.csv

# Edit progress.json to remove crop entry
# Pipeline will re-fetch all years
```

### Force Re-fetch a Year

Progress is year-level, not page-level. To re-fetch a year:

```python
# Edit progress.json
{
  "wheat": 2023  # Change from 2024 to 2023
}
# Next run will re-fetch 2024
```

## Performance

### Expected Throughput

- **Per API call**: ~1000 records (LIMIT)
- **Per year**: ~30-60K records (varies by year)
- **Per 3-hour run**: ~2-3 years of data (depends on record density)

### Bottlenecks

1. **API rate limits**: 1-second delay between calls
2. **API response time**: ~2-5 seconds per request
3. **CSV I/O**: Minimal impact (pandas is fast)

### Optimization Opportunities

- Use async/await for parallel year fetching (but increases complexity)
- Increase `LIMIT` if API supports >1000
- Reduce `API_CALL_DELAY` if rate limits allow

## Dependencies

```
requests>=2.28.0
pandas>=1.5.0  
```

Both are standard and stable.

## Environment Variables

```bash
export DATA_GOV_API_KEY="your_api_key_here"
```

**Required** - Pipeline will not run without it.

## License & Attribution

Data source: data.gov.in Agmarknet dataset  
API: data.gov.in Open Government Data Platform  

## Support & Troubleshooting

### Common Issues

**Issue**: `progress.json` corrupt  
**Solution**: Delete file, pipeline will rebuild from CSVs

**Issue**: API consistently returning 429  
**Solution**: Increase `API_CALL_DELAY` to 2.0 seconds

**Issue**: Year never completes  
**Solution**: Check API response for that year, may be extremely large

**Issue**: Duplicate records appearing  
**Solution**: Check if `KEY_COLUMNS` matches your data schema

## Testing

### Dry Run

```python
# Set short runtime for testing
MAX_RUNTIME = 60  # 1 minute
START_YEAR = 2024  # Only test recent year
```

### Validate Deduplication

```python
import pandas as pd

df = pd.read_csv("data/crops/wheat.csv")
before = len(df)
df_dedup = df.drop_duplicates(subset=KEY_COLUMNS)
after = len(df_dedup)

print(f"Duplicates: {before - after}")
```

### Check Progress Integrity

```python
import json
import glob

with open("data/progress.json") as f:
    progress = json.load(f)

for crop, year in progress.items():
    csv_path = f"data/crops/{crop}.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df['Year'] = pd.to_datetime(df['Arrival_Date']).dt.year
        max_year = df['Year'].max()
        if max_year != year:
            print(f"⚠️ {crop}: progress={year}, CSV max={max_year}")
```

## Future Enhancements

1. **Parallel year fetching**: Fetch multiple years concurrently
2. **Delta detection**: Check for API updates to already-fetched years
3. **Data validation**: Statistical checks for anomalies
4. **Compression**: Store older years in compressed format
5. **Database backend**: Replace CSV with SQL for better querying
6. **Real-time mode**: WebSocket or polling for live updates
7. **Alerting**: Notify on pipeline failures or gaps detected

---

**Last Updated**: February 2026  
**Pipeline Version**: 2.0 (Year-wise Architecture)
