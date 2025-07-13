import requests
import time
import pandas as pd
from sqlalchemy import create_engine

# DB connection
engine = create_engine("postgresql+psycopg2://postgres:locale@localhost:5432/fsds")

headers = {"User-Agent": "albertlimani@yahoo.com"}

# Get company tickers
tickers = requests.get("https://www.sec.gov/files/company_tickers.json", headers=headers).json()
df = pd.DataFrame.from_dict(tickers, orient='index')
df['cik'] = df['cik_str'].astype(str).str.zfill(10)

# Load existing CIKs from DB
try:
    existing = pd.read_sql("SELECT cik FROM ciksic", engine)
    existing_ciks = set(existing['cik'])
except Exception:
    existing_ciks = set()  # First run

# Prepare output
rows = []

for _, row in df.iterrows():
    cik = row['cik']
    if cik in existing_ciks:
        continue  # skip duplicates

    ticker = row['ticker']
    name = row['title']
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"

    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        sic = str(data.get('sic', ''))
        sic_desc = data.get('sicDescription', '')
        rows.append({
            'cik': cik,
            'ticker': ticker,
            'company_name': name,
            'sic': sic,
            'sic_description': sic_desc
        })
    time.sleep(0.2)  # Respect SEC rate limits

# Save to DB
if rows:
    df_sic_map = pd.DataFrame(rows)
    df_sic_map.to_sql("ciksic", engine, if_exists="append", index=False)
    print(f"✅ Saved {len(df_sic_map)} new entries.")
else:
    print("ℹ️ No new entries to save.")
