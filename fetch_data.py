import requests
import pandas as pd
import time
import os
import json
from sklearn.preprocessing import LabelEncoder
from requests.exceptions import RequestException, Timeout

# ================= CONFIG =================
API_KEY = os.getenv("DATA_GOV_API_KEY")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

LIMIT = 1000
MAX_RETRIES = 5          # retry attempts per request
REQUEST_TIMEOUT = 15    # seconds
BACKOFF_BASE = 2        # exponential backoff

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = f"{DATA_DIR}/progress.json"
# ==========================================


# ========== PROGRESS HANDLING ==========
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {"completed_crops": []}

    try:
        with open(PROGRESS_FILE, "r") as f:
            content = f.read().strip()
            if not content:
                return {"completed_crops": []}
            return json.loads(content)
    except json.JSONDecodeError:
        print("⚠️ progress.json empty or corrupted, resetting progress")
        return {"completed_crops": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)
# ======================================


# ========== SAFE API CALL ==========
def fetch_page(offset, filters=None):
    """
    Robust API fetch with:
    - timeout
    - retries
    - exponential backoff
    - safe JSON parsing
    """
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": LIMIT,
        "offset": offset
    }

    if filters:
        for k, v in filters.items():
            params[f"filters[{k}]"] = v

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                BASE_URL,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")

            data = r.json()
            records = data.get("records", [])

            return records

        except (Timeout, RequestException, ValueError) as e:
            wait = BACKOFF_BASE ** attempt
            print(
                f"⚠️ API error (attempt {attempt}/{MAX_RETRIES}) | "
                f"offset={offset} | error={e} | retry in {wait}s"
            )
            time.sleep(wait)

    print(f"❌ Failed permanently at offset {offset}")
    return []   # fail gracefully
# ====================================


# ========== DISCOVER COMMODITIES ==========
print("🔍 Discovering commodities...")
first_page = fetch_page(0)

commodities = sorted({r.get("Commodity") for r in first_page if r.get("Commodity")})
print(f"Total commodities discovered: {len(commodities)}")
# ==========================================


# ========== LOAD PROGRESS ==========
progress = load_progress()
completed = set(progress["completed_crops"])
print("Already completed:", completed)
# ==================================


# ========== PROCESS ONE CROP ==========
def process_crop(crop):
    print(f"\n🚜 Processing crop: {crop}")
    rows = []
    offset = 0

    while True:
        records = fetch_page(offset, {"Commodity": crop})
        if not records:
            break

        rows.extend(records)
        offset += LIMIT
        time.sleep(0.3)

    if not rows:
        print(f"⚠️ No usable data for {crop}")
        return False

    df = pd.DataFrame(rows)

    # ---------- CLEANING ----------
    df["Arrival_Date"] = pd.to_datetime(
        df["Arrival_Date"], format="%d-%m-%Y", errors="coerce"
    )
    df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")

    df = df.dropna(subset=["Arrival_Date", "Modal_Price"])
    df = df[df["Modal_Price"] > 0]

    if df.empty:
        print(f"⚠️ Empty dataset after cleaning for {crop}")
        return False

    # ---------- LAST 3 YEARS ----------
    cutoff = df["Arrival_Date"].max() - pd.DateOffset(years=3)
    df = df[df["Arrival_Date"] >= cutoff]

    if df.empty:
        print(f"⚠️ No recent data for {crop}")
        return False

    # ---------- FEATURES ----------
    df = df.sort_values(["Market", "Arrival_Date"])

    df["price_lag_1"] = df.groupby("Market")["Modal_Price"].shift(1)
    df["rolling_mean_7"] = (
        df.groupby("Market")["Modal_Price"]
          .rolling(7).mean()
          .reset_index(level=0, drop=True)
    )

    df = df.dropna()

    if df.empty:
        print(f"⚠️ Not enough history for {crop}")
        return False

    # ---------- ENCODE MARKET ----------
    le = LabelEncoder()
    df["market_id"] = le.fit_transform(df["Market"])

    # ---------- SAVE ----------
    fname = crop.replace(" ", "_").lower()
    df.to_csv(f"{DATA_DIR}/{fname}_ml.csv", index=False)

    print(f"✅ Saved {fname}_ml.csv | rows={len(df)}")
    return True
# =====================================


# ========== MAIN LOOP (RESUME SAFE) ==========
for crop in commodities:
    if crop in completed:
        print(f"⏭️ Skipping {crop} (already done)")
        continue

    success = process_crop(crop)

    if success:
        progress["completed_crops"].append(crop)
        save_progress(progress)
        print(f"📌 Progress saved ({len(progress['completed_crops'])} crops done)")
    else:
        print(f"⚠️ Crop {crop} skipped due to errors")

    time.sleep(1)

print("\n🎉 PIPELINE FINISHED OR TIME LIMIT HIT")
# ===========================================
