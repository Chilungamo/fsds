import os
import time
import logging
import requests
import pandas as pd
from fsds.config import sec_headers, get_engine

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- Configuration ---
HEADERS = sec_headers()
CONCEPT_TAG = "NetCashProvidedByUsedInOperatingActivities"
ENDPOINT_TEMPLATE = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept}.json"

# --- Load company CIKs ---
try:
    ticker_resp = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS, timeout=15)
    ticker_resp.raise_for_status()
    tickers = ticker_resp.json()
    companies = pd.DataFrame.from_dict(tickers, orient="index")
    companies["cik"] = companies["cik_str"].astype(str).str.zfill(10)
    logging.info(f"✅ Loaded {len(companies)} companies.")
except Exception as e:
    logging.error(f"❌ Failed to load ticker list: {e}")
    exit(1)

# --- Results Collection ---
results = []

# Optional: limit number of companies for testing (e.g., .head(50))
for _, row in companies.iterrows():
    cik = row["cik"]
    name = row["title"]
    url = ENDPOINT_TEMPLATE.format(cik=cik, concept=CONCEPT_TAG)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("units", {}).get("USD", [])
            for e in data:
                results.append({
                    "company": name,
                    "cik": cik,
                    "accession": e.get("accn"),
                    "form": e.get("form"),
                    "fy": e.get("fy"),
                    "fp": e.get("fp"),
                    "start": e.get("start"),
                    "end": e.get("end"),
                    "value": e.get("val"),
                    "filed": e.get("filed"),
                    "frame": e.get("frame")
                })
        else:
            logging.warning(f"⚠️ No data or bad response for {name} ({cik}) - {resp.status_code}")
        time.sleep(0.2)  # Respect SEC rate limits
    except Exception as e:
        logging.warning(f"❌ Error fetching data for {cik}: {e}")
        continue

# --- Save to PostgreSQL ---
if results:
    df = pd.DataFrame(results)
    logging.info(f"✅ Collected {len(df)} records.")
    logging.info(f"\n📊 Sample Data:\n{df.head()}")

    # Database connection via environment variables
    
    try:
        engine = get_engine()
        df.to_sql("opscash2", engine, if_exists="replace", index=False)
        logging.info("✅ Data saved to PostgreSQL table `opscash2`")
        
        # Optional: validate
        row_count = pd.read_sql("SELECT COUNT(*) FROM opscash2", engine).iloc[0, 0]
        logging.info(f"📦 Total rows in DB table: {row_count}")
    except Exception as e:
        logging.error(f"❌ Failed to save to database: {e}")
else:
    logging.warning("⚠️ No data collected. Nothing saved.")
