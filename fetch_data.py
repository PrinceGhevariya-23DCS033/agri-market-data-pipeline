import os
import json
import time
import re
from datetime import datetime
import requests
import pandas as pd
from requests.exceptions import RequestException, Timeout

# ================= CONFIG =================
API_KEY = os.getenv("DATA_GOV_API_KEY")
if not API_KEY:
    raise RuntimeError("DATA_GOV_API_KEY missing")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

LIMIT = 1000
REQUEST_TIMEOUT = 30

SHORT_RETRIES = 5
BACKOFF_BASE = 2
LONG_SLEEP = 300

DATA_DIR = "data/crops"
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = "data/progress.json"

START_TIME = time.time()
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60  # 2h55m

API_DELAY = 1.2

KEY_COLUMNS = ["State", "District", "Market", "Commodity", "Arrival_Date"]
# ========================================


# ========== UTILS ==========
def safe_name(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "_", text)


def time_left() -> bool:
    return (time.time() - START_TIME) < MAX_RUNTIME
# ===========================


# ========== PROGRESS ==========
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {}
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)
# ============================


# ========== CSV ANALYSIS ==========
def get_last_date_from_csv(crop_key):
    path = os.path.join(DATA_DIR, f"{crop_key}.csv")
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path, usecols=["Arrival_Date"])
    df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], errors="coerce")
    if df.empty:
        return None

    return df["Arrival_Date"].max()
# =================================


# ========== API FETCH ==========
def fetch_page(commodity, offset):
    for attempt in range(1, SHORT_RETRIES + 1):
        try:
            r = requests.get(
                BASE_URL,
                params={
                    "api-key": API_KEY,
                    "format": "json",
                    "limit": LIMIT,
                    "offset": offset,
                    "filters[Commodity]": commodity
                },
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code == 200:
                return r.json().get("records", [])

            raise RequestException(f"HTTP {r.status_code}")

        except (Timeout, RequestException, ValueError):
            wait = BACKOFF_BASE ** attempt
            print(f"⚠️ Retry {attempt}/{SHORT_RETRIES} | {commodity} | offset={offset}")
            time.sleep(wait)

    return None
# ==============================


# ========== APPEND ==========
def append_to_csv(df, crop_key):
    path = os.path.join(DATA_DIR, f"{crop_key}.csv")

    if os.path.exists(path):
        old = pd.read_csv(path)
        combined = pd.concat([old, df], ignore_index=True)
        combined.drop_duplicates(subset=KEY_COLUMNS, inplace=True)
    else:
        combined = df

    combined.to_csv(path, index=False)
    return len(combined)
# ============================


# ========== MAIN ==========
def run():
    print("🚜 Agmarknet Commodity-Based Incremental Pipeline")

    progress = load_progress()

    # Initial commodity discovery (only once)
    if not progress:
        print("🌱 Discovering commodities...")
        sample = fetch_page("", 0)
        if not sample:
            raise RuntimeError("❌ Could not discover commodities")

        for r in sample:
            crop_key = safe_name(r["Commodity"])
            progress[crop_key] = None

        save_progress(progress)

    for crop_key in list(progress.keys()):
        commodity = crop_key.replace("_", " ").title()
        print(f"\n🌾 Processing: {commodity}")

        last_saved = get_last_date_from_csv(crop_key)
        offset = 0
        reached_old_data = False

        while time_left():
            records = fetch_page(commodity, offset)
            if records is None or not records:
                break

            df = pd.DataFrame(records)
            df["Arrival_Date"] = pd.to_datetime(
                df["Arrival_Date"], dayfirst=True, errors="coerce"
            )
            df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")
            df = df.dropna(subset=["Arrival_Date", "Modal_Price"])

            if last_saved is not None:
                df = df[df["Arrival_Date"] > last_saved]

            if df.empty:
                reached_old_data = True
                break

            append_to_csv(df, crop_key)
            last_saved = df["Arrival_Date"].max()
            progress[crop_key] = last_saved.strftime("%Y-%m-%d")
            save_progress(progress)

            offset += LIMIT
            time.sleep(API_DELAY)

        print(f"✅ {commodity} done")

    print("🎉 Pipeline finished cleanly")


if __name__ == "__main__":
    run()
