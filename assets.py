import os
import time
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
from tqdm import tqdm

# --- Load environment variables ---
load_dotenv()

# --- Configuration ---
HEADERS = {"User-Agent": "albertlimani@yahoo.com"}
CONCEPT_TAG = "Assets"
ENDPOINT_TEMPLATE = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept}.json"

# --- Load company CIKs ---
try:
    ticker_resp = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS, timeout=15)
    ticker_resp.raise_for_status()
    tickers = ticker_resp.json()
    companies = pd.DataFrame.from_dict(tickers, orient="index")
    companies["cik"] = companies["cik_str"].astype(str).str.zfill(10)
    print(f"✅ Loaded {len(companies)} companies.")
except Exception as e:
    print(f"❌ Failed to load ticker list: {e}")
    exit(1)

# --- DB connection ---
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "locale"),
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    database=os.getenv("DB_NAME", "fsds")
)
engine = create_engine(db_url)

# --- Get already existing keys to avoid duplication ---
existing_keys = set()
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT cik, accession, ddate FROM assets"))
        existing_keys = set((str(r[0]), str(r[1]), str(r[2])) for r in result.fetchall())
    print(f"📌 Found {len(existing_keys)} existing entries to skip.")
except Exception:
    print("⚠️ Table not found or first run. All data will be collected.")

# --- Results Collection ---
results = []

progress = tqdm(companies.iterrows(), total=len(companies), desc="🔍 Fetching Assets Data", position=0, leave=True)

for _, row in progress:
    cik = row["cik"]
    name = row["title"]
    url = ENDPOINT_TEMPLATE.format(cik=cik, concept=CONCEPT_TAG)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            entries = resp.json().get("units", {}).get("USD", [])
            new_records = 0
            for e in entries:
                accn = e.get("accn")
                ddate = e.get("end")
                key = (cik, accn, ddate)
                if key in existing_keys:
                    continue  # Skip duplicate
                try:
                    results.append({
                        "company": name,
                        "cik": cik,
                        "accession": accn,
                        "form": e.get("form"),
                        "fy": e.get("fy"),
                        "fp": e.get("fp"),
                        "ddate": pd.to_datetime(ddate, errors='coerce').date() if ddate else None,
                        "value": e.get("val"),
                        "filed": pd.to_datetime(e.get("filed"), errors='coerce').date() if e.get("filed") else None
                    })
                    new_records += 1
                except Exception:
                    continue
            tqdm.write(f"✅ {name} ({cik}) → {new_records} new records")
        time.sleep(0.2)
    except Exception:
        continue

# --- Save to PostgreSQL ---
if results:
    df = pd.DataFrame(results)
    tqdm.write(f"✅ Collected {len(df)} new records.")

    try:
        df.to_sql("assets", engine, if_exists="append", index=False)
        tqdm.write("✅ Data appended to PostgreSQL table `assets`")
    except Exception as e:
        tqdm.write(f"❌ Failed to save to database: {e}")
else:
    tqdm.write("⚠️ No new data to insert.")
