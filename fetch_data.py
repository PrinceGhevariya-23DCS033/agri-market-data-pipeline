import os
import json
import time
import requests
import pandas as pd
from requests.exceptions import RequestException, Timeout

# ================= CONFIG =================
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise RuntimeError("DATA_GOV_API_KEY missing in GitHub Secrets")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

LIMIT = 1000
MAX_OFFSET = 200000          # SAFE pagination cap
MAX_RETRIES = 5
REQUEST_TIMEOUT = 20

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
PROGRESS_FILE = f"{DATA_DIR}/progress.json"
# ==========================================


# ========== PROGRESS ==========
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {"completed": [], "attempted": []}

    try:
        with open(PROGRESS_FILE, "r") as f:
            data = f.read().strip()
            if not data:
                return {"completed": [], "attempted": []}
            return json.loads(data)
    except json.JSONDecodeError:
        return {"completed": [], "attempted": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)
# ==============================


# ========== SAFE API ==========
def fetch_page(offset, filters=None):
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
            r = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")
            return r.json().get("records", [])
        except (Timeout, RequestException, ValueError) as e:
            wait = 2 ** attempt
            print(f"⚠️ API retry {attempt}/{MAX_RETRIES} | offset={offset} | wait={wait}s")
            time.sleep(wait)

    print(f"❌ API failed permanently at offset {offset}")
    return []
# ==============================


# ========== DISCOVER CROPS ==========
print("🔍 Discovering commodities...")
first_page = fetch_page(0)
commodities = sorted(set(r["Commodity"] for r in first_page if r.get("Commodity")))
print(f"Total commodities discovered: {len(commodities)}")
# ===================================


progress = load_progress()
print("Already completed:", progress["completed"])


# ========== PROCESS ONE CROP ==========
def process_crop(crop):
    print(f"\n🚜 STARTING: {crop}")
    all_rows = []
    offset = 0
    page_count = 0

    while offset <= MAX_OFFSET:
        records = fetch_page(offset, {"Commodity": crop})
        if not records:
            break

        all_rows.extend(records)
        page_count += 1
        print(f"  📦 Pages fetched: {page_count} | Rows so far: {len(all_rows)}")

        offset += LIMIT
        time.sleep(0.3)

    if not all_rows:
        print(f"⚠️ No data fetched for {crop}")
        return False

    df = pd.DataFrame(all_rows)

    # ---- BASIC CLEANING (SAFE) ----
    df["Arrival_Date"] = pd.to_datetime(
        df["Arrival_Date"], format="%d-%m-%Y", errors="coerce"
    )
    df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")

    df = df.dropna(subset=["Arrival_Date", "Modal_Price"])
    df = df[df["Modal_Price"] > 0]

    if df.empty:
        print(f"⚠️ No usable rows after cleaning for {crop}")
        return False

    # ---- LAST 3 YEARS ----
    cutoff = df["Arrival_Date"].max() - pd.DateOffset(years=3)
    df = df[df["Arrival_Date"] >= cutoff]

    if df.empty:
        print(f"⚠️ No recent data (3y) for {crop}")
        return False

    # ---- SAVE CSV (ALWAYS) ----
    fname = crop.replace(" ", "_").lower()
    csv_path = f"{DATA_DIR}/{fname}.csv"
    df.to_csv(csv_path, index=False)

    print(f"✅ SAVED CSV: {csv_path} | rows={len(df)}")
    return True
# =====================================


# ========== MAIN LOOP ==========
for idx, crop in enumerate(commodities, start=1):
    if crop in progress["attempted"]:
        print(f"⏭️ Skipping {crop} (already attempted)")
        continue

    print(f"\n📊 Progress: {idx}/{len(commodities)}")
    success = process_crop(crop)

    progress["attempted"].append(crop)
    if success:
        progress["completed"].append(crop)

    save_progress(progress)
    print(f"📌 Progress saved")

print("\n🎉 PIPELINE FINISHED SAFELY")
# =====================================
