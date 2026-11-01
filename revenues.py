import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from fsds.config import sec_headers, get_engine

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- Configuration ---
HEADERS = sec_headers()
CONCEPT_TAG = "Revenues"
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


def _date_key(d):
    """Normalise a date / ISO string / None so DB rows and API rows compare equal."""
    return None if d is None else str(d)[:10]


# --- DB connection ---
engine = get_engine()

# --- Get already existing keys to avoid duplication ---
existing_keys = set()
try:
    with engine.connect() as conn:
        # "end" is a reserved word in PostgreSQL and must be quoted.
        result = conn.execute(text('SELECT cik, accession, start, "end" FROM revenues2'))
        existing_keys = set(
            (str(r[0]), str(r[1]), _date_key(r[2]), _date_key(r[3])) for r in result.fetchall()
        )
    logging.info(f"📌 Found {len(existing_keys)} existing entries to skip.")
except Exception:
    logging.warning("⚠️ Table not found or first run. All data will be collected.")

# --- Results Collection ---
results = []

for _, row in companies.iterrows():
    cik = row["cik"]
    name = row["title"]
    url = ENDPOINT_TEMPLATE.format(cik=cik, concept=CONCEPT_TAG)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            entries = resp.json().get("units", {}).get("USD", [])
            for e in entries:
                accn = e.get("accn")
                start = e.get("start")
                end = e.get("end")
                key = (cik, accn, _date_key(start), _date_key(end))
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
                        "start": pd.to_datetime(start, errors='coerce').date() if start else None,
                        "end": pd.to_datetime(end, errors='coerce').date() if end else None,
                        "frame": e.get("frame"),
                        "value": e.get("val"),
                        "filed": pd.to_datetime(e.get("filed"), errors='coerce').date() if e.get("filed") else None
                    })
                except Exception as err:
                    logging.warning(f"⚠️ Skipped malformed record for {cik}: {err}")
        else:
            logging.warning(f"⚠️ No data or bad response for {name} ({cik}) - {resp.status_code}")
        time.sleep(0.2)
    except Exception as e:
        logging.warning(f"❌ Error fetching data for {cik}: {e}")
        continue

# --- Save to PostgreSQL ---
if results:
    df = pd.DataFrame(results)
    logging.info(f"✅ Collected {len(df)} new records.")

    try:
        df.to_sql("revenues2", engine, if_exists="append", index=False)
        logging.info("✅ Data appended to PostgreSQL table `revenues2`")
    except Exception as e:
        logging.error(f"❌ Failed to save to database: {e}")
else:
    logging.info("⚠️ No new data to insert.")
