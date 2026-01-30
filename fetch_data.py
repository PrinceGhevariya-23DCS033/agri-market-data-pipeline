import os
import json
import time
import re
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
REQUEST_TIMEOUT = 20

SHORT_RETRIES = 5
SHORT_BACKOFF = 2

LONG_SLEEP_1 = 300    # 5 minutes
LONG_SLEEP_2 = 900    # 15 minutes

MAX_OFFSET = 3_000_000

DATA_DIR = "data/crops"
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = "data/progress.json"
# ==========================================


# ========== SAFE FILENAME ==========
def safe_name(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)   # remove (), etc
    text = re.sub(r"\s+", "_", text)
    return text
# ===================================


# ========== PROGRESS ==========
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {"last_offset": 0}

    try:
        with open(PROGRESS_FILE, "r") as f:
            data = json.load(f)
            return {"last_offset": data.get("last_offset", 0)}
    except Exception:
        return {"last_offset": 0}


def save_progress(offset):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_offset": offset}, f, indent=2)
# ==============================


# ========== API FETCH WITH LONG RETRY ==========
def fetch_page_with_resilience(offset):
    # first short retries
    for attempt in range(1, SHORT_RETRIES + 1):
        try:
            r = requests.get(
                BASE_URL,
                params={
                    "api-key": API_KEY,
                    "format": "json",
                    "limit": LIMIT,
                    "offset": offset
                },
                timeout=REQUEST_TIMEOUT
            )
            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")
            return r.json().get("records", [])
        except (Timeout, RequestException, ValueError):
            wait = SHORT_BACKOFF ** attempt
            print(f"⚠️ Short retry {attempt}/{SHORT_RETRIES} | offset={offset} | wait={wait}s")
            time.sleep(wait)

    # long sleep 1
    print(f"🕒 API unstable at offset {offset}. Sleeping {LONG_SLEEP_1//60} minutes...")
    time.sleep(LONG_SLEEP_1)

    # second round
    for attempt in range(1, SHORT_RETRIES + 1):
        try:
            r = requests.get(
                BASE_URL,
                params={
                    "api-key": API_KEY,
                    "format": "json",
                    "limit": LIMIT,
                    "offset": offset
                },
                timeout=REQUEST_TIMEOUT
            )
            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")
            return r.json().get("records", [])
        except (Timeout, RequestException, ValueError):
            wait = SHORT_BACKOFF ** attempt
            print(f"⚠️ Retry after cooldown {attempt}/{SHORT_RETRIES} | wait={wait}s")
            time.sleep(wait)

    # final long sleep
    print(f"🛑 API still failing. Sleeping {LONG_SLEEP_2//60} minutes before skip.")
    time.sleep(LONG_SLEEP_2)
    return []
# =============================================


# ========== APPEND / CREATE ==========
def append_to_crop_csv(df, crop):
    crop_file = safe_name(crop) + ".csv"
    path = os.path.join(DATA_DIR, crop_file)

    if os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
        print(f"➕ Appended {len(df)} rows → {crop_file}")
    else:
        df.to_csv(path, index=False)
        print(f"🆕 Created {crop_file} ({len(df)} rows)")
# ===================================


# ========== MAIN LOOP ==========
progress = load_progress()
offset = progress["last_offset"]

print(f"▶ Resuming from offset: {offset}")

while offset <= MAX_OFFSET:
    records = fetch_page_with_resilience(offset)
    if not records:
        print(f"⚠️ No records at offset {offset}, moving on")
        offset += LIMIT
        save_progress(offset)
        continue

    df = pd.DataFrame(records)

    # FIX DATE WARNING (Indian format)
    df["Arrival_Date"] = pd.to_datetime(
        df["Arrival_Date"],
        dayfirst=True,
        errors="coerce"
    )
    df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")

    df = df.dropna(subset=["Commodity", "Modal_Price"])

    for crop, group in df.groupby("Commodity"):
        append_to_crop_csv(group, crop)

    offset += LIMIT
    save_progress(offset)
    print(f"📊 Progress saved | next offset = {offset}")

    time.sleep(0.3)

print("🎉 DATA COLLECTION COMPLETED SAFELY")
# =====================================
