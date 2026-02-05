import os
import json
import time
import re
from datetime import datetime
import requests
import pandas as pd
from requests.exceptions import RequestException, Timeout
from difflib import get_close_matches

# ================= CONFIG =================
API_KEY = os.getenv("DATA_GOV_API_KEY")
# API_KEY="579b464db66ec23bdd000001219b38f4744345634263e9c7296b88f8"

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
PROGRESS_FILE = "data/progress.json"

START_TIME = time.time()
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60
API_DELAY = 1.2

KEY_COLUMNS = ["State", "District", "Market", "Commodity", "Arrival_Date"]
# ========================================


# ========== UTILS ==========
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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


# ========== EXISTING CROPS ==========
def load_existing_crops():
    crops = {}
    for file in os.listdir(DATA_DIR):
        if file.endswith(".csv"):
            key = file.replace(".csv", "")
            crops[key] = normalize(key.replace("_", " "))
    return crops  # {csv_key: normalized_name}
# ====================================


# ========== LAST DATE ==========
def get_last_date(csv_key):
    path = os.path.join(DATA_DIR, f"{csv_key}.csv")
    df = pd.read_csv(path, usecols=["Arrival_Date"])
    df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], errors="coerce")
    return df["Arrival_Date"].max()
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
            wait = BACKOFF_BASE ** attempt
            print(f"⚠️ Retry {attempt}/{SHORT_RETRIES} | offset={offset}")
            time.sleep(wait)

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


# ========== MAIN ==========
def run():
    print("🚜 Agmarknet Incremental Pipeline (Existing CSV Driven)")

    crops = load_existing_crops()
    progress = load_progress()

    print(f"🌾 Found {len(crops)} existing crop files")

    last_dates = {k: get_last_date(k) for k in crops}

    offset = 0
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

        for commodity, group in df.groupby("Commodity"):
            norm_api = normalize(commodity)

            # direct substring match
            target = None
            for csv_key, norm_csv in crops.items():
                if norm_api in norm_csv or norm_csv in norm_api:
                    target = csv_key
                    break

            # fuzzy fallback
            if not target:
                matches = get_close_matches(
                    norm_api, crops.values(), n=1, cutoff=0.85
                )
                if matches:
                    for k, v in crops.items():
                        if v == matches[0]:
                            target = k
                            break

            if not target:
                continue  # unknown commodity

            last_date = last_dates[target]
            if last_date is not None:
                group = group[group["Arrival_Date"] > last_date]

            if group.empty:
                continue

            append_to_csv(group, target)
            last_dates[target] = group["Arrival_Date"].max()
            progress[target] = last_dates[target].strftime("%Y-%m-%d")

        save_progress(progress)
        offset += LIMIT
        time.sleep(API_DELAY)

    print("✅ Pipeline finished cleanly")


if __name__ == "__main__":
    run()
