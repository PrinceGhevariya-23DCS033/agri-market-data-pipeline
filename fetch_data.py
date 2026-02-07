import os
import json
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from difflib import get_close_matches

import requests
import pandas as pd
from requests.exceptions import RequestException, Timeout

# ================= CONFIG =================
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise RuntimeError("DATA_GOV_API_KEY missing")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

DATA_DIR = "data/crops"
PROGRESS_FILE = "data/progress.json"
UNMATCHED_FILE = "data/unmatched_commodities.json"

LIMIT = 1000
REQUEST_TIMEOUT = 30
SHORT_RETRIES = 5
BACKOFF_BASE = 2
API_DELAY = 1.0

MAX_WORKERS = 3            # safe parallelism
MAX_OFFSET_PER_RUN = 30000 # prevent one crop from hogging

# ⏱️ 2 HOURS 55 MINUTES
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60
START_TIME = time.time()

KEY_COLUMNS = ["State", "District", "Market", "Commodity", "Arrival_Date"]

os.makedirs(DATA_DIR, exist_ok=True)
lock = threading.Lock()
# ========================================


# ========== UTILS ==========
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def time_left():
    return (time.time() - START_TIME) < MAX_RUNTIME
# ===========================


# ========== JSON HELPERS ==========
def load_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
# ================================


# ========== EXISTING CROPS ==========
def load_existing_crops():
    crops = {}
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            key = f.replace(".csv", "")
            crops[key] = normalize(key.replace("_", " "))
    return crops
# ====================================


# ========== LAST DATE ==========
def get_last_date(csv_key):
    path = os.path.join(DATA_DIR, f"{csv_key}.csv")
    try:
        df = pd.read_csv(path, usecols=["Arrival_Date"])
        df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], errors="coerce")
        return df["Arrival_Date"].max()
    except Exception:
        return None
# =================================


# ========== API ==========
def fetch_page(offset):
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
            if r.status_code == 200:
                return r.json().get("records", [])
            raise RequestException(f"HTTP {r.status_code}")
        except (Timeout, RequestException, ValueError):
            time.sleep(BACKOFF_BASE ** attempt)
    return None
# ==========================


# ========== APPEND ==========
def append_to_csv(df, csv_key):
    path = os.path.join(DATA_DIR, f"{csv_key}.csv")
    old = pd.read_csv(path)
    combined = pd.concat([old, df], ignore_index=True)
    combined.drop_duplicates(subset=KEY_COLUMNS, inplace=True)
    combined.to_csv(path, index=False)
# ============================


# ========== WORKER ==========
def process_crop(csv_key, crops, progress, unmatched):
    last_date = get_last_date(csv_key)
    offset = 0

    while time_left() and offset <= MAX_OFFSET_PER_RUN:
        records = fetch_page(offset)
        if not records:
            break

        df = pd.DataFrame(records)
        df["Arrival_Date"] = pd.to_datetime(
            df["Arrival_Date"], dayfirst=True, errors="coerce"
        )
        df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")
        df = df.dropna(subset=["Commodity", "Arrival_Date", "Modal_Price"])

        for commodity, group in df.groupby("Commodity"):
            norm_api = normalize(commodity)
            target = None

            # substring match
            for k, v in crops.items():
                if norm_api in v or v in norm_api:
                    target = k
                    break

            # fuzzy fallback
            if not target:
                matches = get_close_matches(norm_api, crops.values(), n=1, cutoff=0.85)
                if matches:
                    for k, v in crops.items():
                        if v == matches[0]:
                            target = k
                            break

            # ❌ UNMATCHED → ONLY LOG
            if not target:
                with lock:
                    unmatched[commodity] = unmatched.get(commodity, 0) + len(group)
                continue

            # date filter
            if last_date is not None:
                group = group[group["Arrival_Date"] > last_date]

            if group.empty:
                continue

            append_to_csv(group, target)

            with lock:
                progress[target] = group["Arrival_Date"].max().strftime("%Y-%m-%d")

        with lock:
            save_json(PROGRESS_FILE, progress)
            save_json(UNMATCHED_FILE, unmatched)

        offset += LIMIT
        time.sleep(API_DELAY)


# ========== MAIN ==========
def run():
    print("🚜 FAST • PARALLEL • RESUMABLE (2h55m runtime)")
    print("📌 No new CSVs will be created for unmatched commodities")

    crops = load_existing_crops()
    progress = load_json(PROGRESS_FILE)
    unmatched = load_json(UNMATCHED_FILE)

    print(f"🌾 Using {len(crops)} existing crop CSV files")

    # process smaller CSVs first
    crop_keys = sorted(
        crops.keys(),
        key=lambda k: os.path.getsize(os.path.join(DATA_DIR, f"{k}.csv"))
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        for key in crop_keys:
            if not time_left():
                break
            exe.submit(process_crop, key, crops, progress, unmatched)

    save_json(PROGRESS_FILE, progress)
    save_json(UNMATCHED_FILE, unmatched)

    print("✅ Job finished safely")
    print(f"📄 Progress saved to: {PROGRESS_FILE}")
    print(f"📄 Unmatched commodities saved to: {UNMATCHED_FILE}")


if __name__ == "__main__":
    run()
