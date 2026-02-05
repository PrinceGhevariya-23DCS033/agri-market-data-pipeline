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
    raise RuntimeError("DATA_GOV_API_KEY missing")

RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"

LIMIT = 1000
REQUEST_TIMEOUT = 20

SHORT_RETRIES = 5
SHORT_BACKOFF = 2

LONG_SLEEP = 300  # 5 minutes

DATA_DIR = "data/crops"
os.makedirs(DATA_DIR, exist_ok=True)

PROGRESS_FILE = "data/progress.json"

START_TIME = time.time()
MAX_RUNTIME = 2 * 60 * 60 + 55 * 60  # 2h55m
# =========================================


# ========== UTILS ==========
def safe_name(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "_", text)
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
# =============================


# ========== CSV LAST YEAR ==========
def get_last_year_from_csv(crop):
    path = os.path.join(DATA_DIR, safe_name(crop) + ".csv")
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path, usecols=["Arrival_Date"])
    df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], errors="coerce")
    if df.empty:
        return None
    return int(df["Arrival_Date"].dt.year.max())
# ==================================


# ========== API FETCH ==========
def fetch_year_page(year, offset):
    for attempt in range(1, SHORT_RETRIES + 1):
        try:
            r = requests.get(
                BASE_URL,
                params={
                    "api-key": API_KEY,
                    "format": "json",
                    "limit": LIMIT,
                    "offset": offset,
                    "filters[Arrival_Date]": f"{year}-01-01:{year}-12-31"
                },
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code != 200:
                raise RequestException(f"HTTP {r.status_code}")

            return r.json().get("records", [])

        except (Timeout, RequestException, ValueError):
            wait = SHORT_BACKOFF ** attempt
            print(f"⚠️ Retry {attempt}/{SHORT_RETRIES} | year={year} | offset={offset}")
            time.sleep(wait)

    print("🕒 API unstable. Sleeping 5 minutes...")
    time.sleep(LONG_SLEEP)
    return None
# =============================


# ========== APPEND WITH DEDUP ==========
def append_to_crop_csv(df, crop):
    crop_file = safe_name(crop) + ".csv"
    path = os.path.join(DATA_DIR, crop_file)

    key_cols = ["State", "District", "Market", "Commodity", "Arrival_Date"]

    if os.path.exists(path):
        old = pd.read_csv(path)
        combined = pd.concat([old, df], ignore_index=True)
        combined.drop_duplicates(subset=key_cols, inplace=True)
    else:
        combined = df

    combined.to_csv(path, index=False)
# ======================================


# ========== MAIN ==========
print("🚜 Agri Market Data Pipeline (Year-wise, Incremental)")

progress = load_progress()
CURRENT_YEAR = pd.Timestamp.now().year

# Crop list will grow automatically
known_crops = list(progress.keys())

if not known_crops:
    print("🌱 First run detected. Crops will be discovered automatically.")

for crop in known_crops or ["__DISCOVER__"]:

    if crop != "__DISCOVER__":
        print(f"\n🌾 Crop: {crop}")

    last_csv_year = get_last_year_from_csv(crop) if crop != "__DISCOVER__" else None
    last_progress_year = progress.get(crop)

    start_year = (
        max(y for y in [last_csv_year, last_progress_year] if y is not None) + 1
        if (last_csv_year or last_progress_year)
        else 2010
    )

    for year in range(start_year, CURRENT_YEAR + 1):
        print(f"📅 Fetching year {year}")
        offset = 0

        while True:
            if time.time() - START_TIME >= MAX_RUNTIME:
                print("⏹ Runtime limit reached. Saving progress.")
                save_progress(progress)
                exit(0)

            records = fetch_year_page(year, offset)

            if records is None:
                break

            if not records:
                print(f"✅ Completed year {year}")
                if crop != "__DISCOVER__":
                    progress[crop] = year
                    save_progress(progress)
                break

            df = pd.DataFrame(records)
            df["Arrival_Date"] = pd.to_datetime(df["Arrival_Date"], dayfirst=True, errors="coerce")
            df["Modal_Price"] = pd.to_numeric(df["Modal_Price"], errors="coerce")
            df = df.dropna(subset=["Commodity", "Modal_Price"])

            for c, g in df.groupby("Commodity"):
                cname = safe_name(c)
                append_to_crop_csv(g, c)
                progress.setdefault(cname, year)

            save_progress(progress)
            offset += LIMIT
            time.sleep(1.2)

print("✅ Pipeline finished cleanly")
# ==============================
