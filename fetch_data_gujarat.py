import os
import json
import time
import re
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
from requests.exceptions import RequestException, Timeout

# ================= CONFIG =================
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise RuntimeError("DATA_GOV_API_KEY missing")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

BASE_DIR = "data/gujarat"
CROP_DIR = os.path.join(BASE_DIR, "crops")
PROGRESS_FILE = os.path.join(BASE_DIR, "progress.json")
META_FILE = os.path.join(BASE_DIR, "meta.json")

LIMIT = 1000
REQUEST_TIMEOUT = 30
SHORT_RETRIES = 5
BACKOFF_BASE = 2
API_DELAY = 0.8

MAX_WORKERS = 4

# ⏱️ 3 HOURS
MAX_RUNTIME = 3 * 60 * 60
START_TIME = time.time()

STATE_FILTER = "Gujarat"

KEY_COLUMNS = ["State", "District", "Market", "Commodity", "Arrival_Date"]

os.makedirs(CROP_DIR, exist_ok=True)
lock = threading.Lock()
# ========================================


# ========== UTILS ==========
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def safe_name(text: str) -> str:
    return normalize(text).replace(" ", "_")


def time_left():
    return (time.time() - START_TIME) < MAX_RUNTIME
# ===========================


# ========== JSON ==========
def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
# ==========================


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
                    "offset": offset,
                    "filters[State]": STATE_FILTER
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
def append_to_crop(df, crop_key):
    path = os.path.join(CROP_DIR, f"{crop_key}.csv")
    if os.path.exists(path):
        old = pd.read_csv(path)
        combined = pd.concat([old, df], ignore_index=True)
        combined.drop_duplicates(subset=KEY_COLUMNS, inplace=True)
    else:
        combined = df
    combined.to_csv(path, index=False)
# ============================


# ========== WORKER ==========
def worker(progress):
    offset = progress.get("offset", 0)
    total_saved = progress.get("total_saved", 0)

    while time_left():
        records = fetch_page(offset)
        if not records:
            break

        df = pd.DataFrame(records)
        df["Arrival_Date"] = pd.to_datetime(
            df["Arrival_Date"], dayfirst=True, errors="coerce"
        )
        df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")

        df = df.dropna(subset=["Commodity", "Arrival_Date", "Modal_Price"])
        if df.empty:
            offset += LIMIT
            continue

        for commodity, group in df.groupby("Commodity"):
            crop_key = safe_name(commodity)
            append_to_crop(group, crop_key)
            total_saved += len(group)

        offset += LIMIT

        with lock:
            progress["offset"] = offset
            progress["total_saved"] = total_saved
            progress["last_run"] = datetime.utcnow().isoformat()
            save_json(PROGRESS_FILE, progress)

        if offset % 5000 == 0:
            mins = int((time.time() - START_TIME) / 60)
            print(f"⏱ {mins} min | offset={offset:,} | saved={total_saved:,}")

        time.sleep(API_DELAY)


# ========== MAIN ==========
def run():
    print("🚜 Gujarat-Only Agmarknet Pipeline")
    print("📍 State filter: Gujarat")
    print("⏱ Runtime: 3 hours")
    print("=" * 60)

    progress = load_json(PROGRESS_FILE, {
        "offset": 0,
        "total_saved": 0
    })

    meta = load_json(META_FILE, {})
    meta["state"] = STATE_FILTER
    meta["started_at"] = datetime.utcnow().isoformat()
    save_json(META_FILE, meta)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        exe.submit(worker, progress)

    save_json(PROGRESS_FILE, progress)

    print("✅ Gujarat job finished cleanly")
    print(f"📊 Total records saved: {progress['total_saved']:,}")
    print(f"📄 Resume offset: {progress['offset']:,}")


if __name__ == "__main__":
    run()
