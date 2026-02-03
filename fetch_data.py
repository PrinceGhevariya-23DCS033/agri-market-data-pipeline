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

MAX_OFFSET = 10_100_000

DATA_DIR = "data/crops"
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = "data/progress.json"

# ⏱️ TIME CONTROL (KEY PART)
START_TIME = time.time()
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60   # 2h 55m
# ==========================================


# ========== SAFE FILENAME ==========
def safe_name(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text
# ===================================


# ========== PROGRESS ==========
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {"last_offset": 0}

    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"last_offset": 0}


def save_progress(offset):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_offset": offset}, f, indent=2)
# ==============================


# ========== API FETCH ==========
def fetch_page_with_resilience(offset):
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
            print(f"⚠️ Retry {attempt}/{SHORT_RETRIES} | offset={offset} | wait={wait}s")
            time.sleep(wait)

    print(f"🕒 API unstable. Sleeping {LONG_SLEEP_1//60} minutes...")
    time.sleep(LONG_SLEEP_1)
    return []
# =============================================


# ========== APPEND ==========
def append_to_crop_csv(df, crop):
    crop_file = safe_name(crop) + ".csv"
    path = os.path.join(DATA_DIR, crop_file)

    if os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)
# ===================================


# ========== MAIN LOOP ==========
progress = load_progress()
offset = progress.get("last_offset", 0)

print(f"▶ Resuming from offset: {offset}")

while offset <= MAX_OFFSET:

    # ⏱️ GRACEFUL STOP CHECK
    if time.time() - START_TIME >= MAX_RUNTIME:
        print("⏹️ Time window reached (2h55m). Saving progress & exiting safely.")
        save_progress(offset)
        break

    records = fetch_page_with_resilience(offset)
    if not records:
        offset += LIMIT
        save_progress(offset)
        continue

    df = pd.DataFrame(records)

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

print("✅ Run finished cleanly")
# =====================================
