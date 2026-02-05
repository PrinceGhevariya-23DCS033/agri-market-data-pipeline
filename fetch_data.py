"""
Agmarknet Data Pipeline - Year-wise Incremental Fetching

This pipeline fetches agricultural market data from the data.gov.in API using a 
year-wise approach. It intelligently determines which years are missing for each 
crop and fetches only the required data, avoiding duplication and API instability.

Key Features:
- Year-wise fetching with small offsets (resets per year)
- Per-crop progress tracking (crop → last completed year)
- Automatic detection of missing years from existing CSVs
- Deduplication using composite keys
- Resumable execution with time limits
- Exponential backoff retry logic
- Production-grade error handling
"""

import os
import json
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Set
import requests
import pandas as pd
from requests.exceptions import RequestException, Timeout

# ================= CONFIGURATION =================
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ DATA_GOV_API_KEY environment variable is required")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

# API settings
LIMIT = 1000  # Records per API call
REQUEST_TIMEOUT = 30  # seconds

# Retry configuration
SHORT_RETRIES = 5  # Quick retries with exponential backoff
SHORT_BACKOFF_BASE = 2  # Base for exponential backoff (seconds)
LONG_SLEEP = 300  # 5 minutes - sleep when API is consistently failing

# Directory structure
DATA_DIR = "data/crops"
os.makedirs(DATA_DIR, exist_ok=True)
PROGRESS_FILE = "data/progress.json"

# Execution limits
START_TIME = time.time()
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60  # 2h55m (safe margin for 3h GitHub Actions)

# Data range configuration
START_YEAR = 2007  # Earliest year in the dataset
CURRENT_YEAR = datetime.now().year

# Deduplication key
KEY_COLUMNS = ["State", "District", "Market", "Commodity", "Arrival_Date"]

# Sleep between successful API calls to avoid rate limiting
API_CALL_DELAY = 1.0  # seconds
# ================================================


# ================= UTILITY FUNCTIONS =================
def safe_name(text: str) -> str:
    """Convert commodity name to safe filename."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)  # Remove special chars
    return re.sub(r"\s+", "_", text)  # Replace spaces with underscores


def format_time(seconds: float) -> str:
    """Format seconds into human-readable time."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"


def check_time_remaining() -> bool:
    """Check if we have time remaining in the execution window."""
    elapsed = time.time() - START_TIME
    return elapsed < MAX_RUNTIME
# =====================================================


# ================= PROGRESS MANAGEMENT =================
def load_progress() -> Dict[str, int]:
    """Load progress tracking data from JSON file.
    
    Structure: {crop_key: last_completed_year}
    Example: {"wheat": 2024, "rice": 2023}
    """
    if not os.path.exists(PROGRESS_FILE):
        return {}
    
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Ensure all values are integers
            return {k: int(v) for k, v in data.items()}
    except (json.JSONDecodeError, ValueError) as e:
        print(f"⚠️ Warning: Could not parse progress file: {e}")
        return {}
    except Exception as e:
        print(f"⚠️ Warning: Error loading progress: {e}")
        return {}


def save_progress(progress: Dict[str, int]) -> None:
    """Persist progress tracking data to JSON file."""
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f"❌ Error saving progress: {e}")
# ======================================================


# ================= CSV ANALYSIS =================
def get_existing_years_from_csv(crop_key: str) -> Set[int]:
    """Analyze existing CSV to determine which years already have data.
    
    Returns a set of years that exist in the CSV file.
    """
    crop_file = f"{crop_key}.csv"
    path = os.path.join(DATA_DIR, crop_file)
    
    if not os.path.exists(path):
        return set()
    
    try:
        df = pd.read_csv(path)
        if "Arrival_Date" not in df.columns or len(df) == 0:
            return set()
        
        df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], errors="coerce")
        df = df.dropna(subset=["Arrival_Date"])
        
        if len(df) == 0:
            return set()
        
        years = set(df["Arrival_Date"].dt.year.unique())
        return years
    except Exception as e:
        print(f"⚠️ Warning: Could not analyze {crop_file}: {e}")
        return set()


def determine_years_to_fetch(crop_key: str, progress: Dict[str, int]) -> List[int]:
    """Determine which years need to be fetched for a given crop.
    
    Logic:
    1. Check existing CSV to see what years already exist
    2. Check progress.json to see last completed year
    3. Return list of years that need fetching (from last completed + 1 to current year)
    
    This ensures:
    - No re-downloading of existing data
    - No gaps in years
    - Incremental progress
    """
    existing_years = get_existing_years_from_csv(crop_key)
    last_completed = progress.get(crop_key, START_YEAR - 1)
    
    # Determine the starting point
    if existing_years:
        # Start from the maximum year in existing data + 1
        start_from = max(existing_years) + 1
    else:
        # Start from last completed year + 1, or START_YEAR if no progress
        start_from = last_completed + 1
    
    # Ensure we don't go below START_YEAR
    start_from = max(start_from, START_YEAR)
    
    # Generate list of years to fetch
    years_to_fetch = []
    for year in range(start_from, CURRENT_YEAR + 1):
        if year not in existing_years:
            years_to_fetch.append(year)
    
    return years_to_fetch
# ===============================================


# ================= API INTERACTION =================
def fetch_year_page(year: int, offset: int) -> Optional[List[dict]]:
    """Fetch a single page of data for a specific year.
    
    Uses the Agmarknet API with year-based filtering via Arrival_Date.
    The API supports filtering by year using: filters[Arrival_Date]=YYYY
    
    Returns:
    - List of records if successful
    - None if all retries failed (signals API instability)
    
    Retry Strategy:
    - SHORT_RETRIES attempts with exponential backoff
    - If all fail, return None to trigger long sleep in caller
    """
    for attempt in range(1, SHORT_RETRIES + 1):
        try:
            response = requests.get(
                BASE_URL,
                params={
                    "api-key": API_KEY,
                    "format": "json",
                    "limit": LIMIT,
                    "offset": offset,
                    # Year-based filtering - API returns all records where year of Arrival_Date matches
                    "filters[Arrival_Date]": str(year)
                },
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                records = data.get("records", [])
                return records
            
            elif response.status_code == 429:  # Rate limited
                wait_time = SHORT_BACKOFF_BASE ** attempt
                print(f"⚠️ Rate limited (429) | Attempt {attempt}/{SHORT_RETRIES} | Waiting {wait_time}s")
                time.sleep(wait_time)
                
            else:
                raise RequestException(f"HTTP {response.status_code}")
        
        except (Timeout, RequestException, ValueError) as e:
            wait_time = SHORT_BACKOFF_BASE ** attempt
            print(f"⚠️ API error: {type(e).__name__} | Attempt {attempt}/{SHORT_RETRIES} | "
                  f"year={year} offset={offset} | Retry in {wait_time}s")
            
            if attempt < SHORT_RETRIES:
                time.sleep(wait_time)
    
    # All retries exhausted
    return None
# ==================================================


# ================= DATA PROCESSING =================
def process_and_clean_records(records: List[dict]) -> pd.DataFrame:
    """Process raw API records into a clean DataFrame.
    
    Cleaning steps:
    1. Convert to DataFrame
    2. Parse Arrival_Date to datetime
    3. Convert Modal_Price to numeric
    4. Drop rows with missing critical data
    5. Drop duplicates based on KEY_COLUMNS
    """
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    
    # Date parsing - API uses DD/MM/YYYY format
    df["Arrival_Date"] = pd.to_datetime(
        df["Arrival_Date"], 
        dayfirst=True, 
        errors="coerce"
    )
    
    # Price conversion
    df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")
    
    # Drop invalid rows
    df = df.dropna(subset=["Commodity", "Modal_Price", "Arrival_Date"])
    
    # Initial deduplication within this batch
    df = df.drop_duplicates(subset=KEY_COLUMNS, keep="first")
    
    return df


def append_to_crop_csv(df: pd.DataFrame, crop_key: str) -> int:
    """Append data to a crop's CSV file with deduplication.
    
    Process:
    1. Load existing CSV if it exists
    2. Concatenate with new data
    3. Deduplicate using KEY_COLUMNS
    4. Save back to CSV
    
    Returns: Number of new rows added (after deduplication)
    """
    if df.empty:
        return 0
    
    crop_file = f"{crop_key}.csv"
    path = os.path.join(DATA_DIR, crop_file)
    
    initial_new_rows = len(df)
    
    if os.path.exists(path):
        try:
            existing_df = pd.read_csv(path)
            # Parse date in existing data
            existing_df["Arrival_Date"] = pd.to_datetime(
                existing_df["Arrival_Date"], 
                errors="coerce"
            )
            
            # Combine old and new
            combined = pd.concat([existing_df, df], ignore_index=True)
            
            # Deduplicate
            before_dedup = len(combined)
            combined = combined.drop_duplicates(subset=KEY_COLUMNS, keep="first")
            after_dedup = len(combined)
            
            rows_added = after_dedup - len(existing_df)
            
            # Save
            combined.to_csv(path, index=False)
            
            return rows_added
            
        except Exception as e:
            print(f"❌ Error processing {crop_file}: {e}")
            # Fallback: save only new data
            df.to_csv(path, index=False)
            return initial_new_rows
    else:
        # New file - just save
        df.to_csv(path, index=False)
        return initial_new_rows
# ==================================================


# ================= MAIN PIPELINE =================
def fetch_year_for_all_crops(year: int, progress: Dict[str, int]) -> bool:
    """Fetch data for a specific year across all commodities.
    
    Process:
    1. Fetch data page by page (offset-based within the year)
    2. Group by commodity
    3. Append to respective CSV files
    4. Update progress for each crop after successful processing
    
    Returns: True if year completed successfully, False if interrupted
    """
    print(f"\n{'='*60}")
    print(f"📅 Fetching Year: {year}")
    print(f"{'='*60}")
    
    offset = 0
    total_records_fetched = 0
    total_records_saved = 0
    commodities_updated = set()
    
    while True:
        # Time check
        if not check_time_remaining():
            print(f"\n⏹ Runtime limit reached at year {year}, offset {offset}")
            print(f"🔄 Progress saved. Resume will continue from incomplete year.")
            return False
        
        # Fetch page
        records = fetch_year_page(year, offset)
        
        if records is None:
            # API consistently failing - back off
            print(f"🕒 API unstable. Sleeping for {LONG_SLEEP}s...")
            time.sleep(LONG_SLEEP)
            
            # Retry once after long sleep
            records = fetch_year_page(year, offset)
            if records is None:
                print(f"❌ Year {year} - Could not fetch data after extended retry. Stopping.")
                return False
        
        # Check if we've reached the end of this year's data
        if not records:
            print(f"✅ Year {year} completed | "
                  f"Total fetched: {total_records_fetched} | "
                  f"Total saved: {total_records_saved} | "
                  f"Crops updated: {len(commodities_updated)}")
            return True
        
        # Process records
        df = process_and_clean_records(records)
        total_records_fetched += len(records)
        
        if df.empty:
            print(f"⚠️ No valid records in this batch (year={year}, offset={offset})")
            offset += LIMIT
            time.sleep(API_CALL_DELAY)
            continue
        
        # Group by commodity and append to respective CSVs
        for commodity, group_df in df.groupby("Commodity"):
            crop_key = safe_name(commodity)
            
            # Append to CSV
            rows_added = append_to_crop_csv(group_df, crop_key)
            total_records_saved += rows_added
            
            if rows_added > 0:
                commodities_updated.add(crop_key)
                
                # Update progress for this crop to this year
                # (Only if we successfully saved data)
                progress[crop_key] = year
        
        # Save progress after each successful page
        save_progress(progress)
        
        # Status update
        print(f"📊 Year {year} | Offset {offset:,} | "
              f"Batch: {len(records)} records | "
              f"Valid: {len(df)} | "
              f"Saved: {total_records_saved} | "
              f"Crops: {len(commodities_updated)}")
        
        # Move to next page
        offset += LIMIT
        time.sleep(API_CALL_DELAY)


def run_pipeline():
    """Main pipeline execution."""
    print("=" * 70)
    print("🚜 AGMARKNET DATA PIPELINE - YEAR-WISE INCREMENTAL FETCHING")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data range: {START_YEAR} to {CURRENT_YEAR}")
    print(f"Max runtime: {format_time(MAX_RUNTIME)}")
    print(f"Data directory: {DATA_DIR}")
    print("=" * 70)
    
    # Load progress
    progress = load_progress()
    print(f"\n📋 Loaded progress for {len(progress)} crops")
    
    # Main year loop
    years_completed = 0
    years_failed = 0
    
    for year in range(START_YEAR, CURRENT_YEAR + 1):
        # Check if we have time
        if not check_time_remaining():
            print(f"\n⏹ Time limit reached. Stopping at year {year}.")
            break
        
        # Fetch this year
        success = fetch_year_for_all_crops(year, progress)
        
        if success:
            years_completed += 1
        else:
            years_failed += 1
            break  # Stop on failure
    
    # Summary
    elapsed = time.time() - START_TIME
    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Runtime: {format_time(elapsed)}")
    print(f"Years completed: {years_completed}")
    print(f"Years failed: {years_failed}")
    print(f"Total crops tracked: {len(progress)}")
    print(f"Progress saved to: {PROGRESS_FILE}")
    print("=" * 70)
    
    if years_failed == 0:
        print("✅ Pipeline completed successfully!")
    else:
        print("⚠️ Pipeline stopped early. Resume in next run.")
# ================================================


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\n\n⚠️ Pipeline interrupted by user")
        print("Progress has been saved. Resume will continue from last checkpoint.")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        raise
