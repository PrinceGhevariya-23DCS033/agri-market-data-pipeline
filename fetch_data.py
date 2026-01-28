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
MAX_RETRIES = 5
REQUEST_TIMEOUT = 20
MAX_OFFSET = 2_000_000  # safe upper bound

DATA_DIR = "data/crops"
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = "data/progress.json"
# ==========================================


# ========== PROGRESS (BACKWARD SAFE) ==========
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {"last_offset": 0}

    try:
        with open(PROGRESS_FILE, "r") as f:
            content = f.read().strip()
            if not content:
                return {"last_offset": 0}

            obj = json.loads(content)
            if "last_offset" not in obj:
                print("⚠️ Old progress.json format detected, resetting")
                return {"last_offset": 0}

            return obj
    except Exception:
        return {"last_offset": 0}


def save_progress(offset):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_offset": offset}, f, indent=2)
# ============================================


# ========== SAFE API ==========
def fetch_page(offset):
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": LIMIT,
        "offset": offset
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")
            return r.json().get("records", [])
        except (Timeout, RequestException, ValueError) as e:
            wait = 2 ** attempt
            print(f"⚠️ Retry {attempt}/{MAX_RETRIES} | offset={offset} | wait={wait}s")
            time.sleep(wait)

    print(f"❌ API failed permanently at offset {offset}")
    return []
# ==============================


# ========== APPEND / CREATE PER CROP ==========
def append_to_crop_csv(df, crop):
    crop_name = crop.replace(" ", "_").lower()
    path = os.path.join(DATA_DIR, f"{crop_name}.csv")

    if os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
        print(f"➕ Appended {len(df)} rows to {crop_name}.csv")
    else:
        df.to_csv(path, index=False)
        print(f"🆕 Created {crop_name}.csv with {len(df)} rows")
# =============================================


# ========== MAIN STREAMING LOOP ==========
progress = load_progress()
offset = progress["last_offset"]

print(f"▶ Resuming from offset: {offset}")

while offset <= MAX_OFFSET:
    records = fetch_page(offset)
    if not records:
        print("🚫 No more records, stopping")
        break

    df = pd.DataFrame(records)

    # minimal safe cleaning
    df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], errors="coerce")
    df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")
    df = df.dropna(subset=["Commodity", "Modal_Price"])

    if df.empty:
        print(f"⚠️ Empty page at offset {offset}")
        offset += LIMIT
        save_progress(offset)
        continue

    for crop, group in df.groupby("Commodity"):
        append_to_crop_csv(group, crop)

    offset += LIMIT
    save_progress(offset)

    print(f"📊 Progress saved | next offset = {offset}")
    time.sleep(0.3)

print("🎉 DATA COLLECTION COMPLETE")
# ============================================
