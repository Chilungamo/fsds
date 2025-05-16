import requests
import feedparser
import psycopg2
from datetime import datetime

# print("Bozo:", feed.bozo)
# print("Feed has", len(feed.entries), "entries") # checks number of feed entries

# === 1. Fetch and parse the SEC XBRL RSS feed ===
url = "https://www.sec.gov/Archives/edgar/xbrlrss.all.xml"
headers = {"User-Agent": "limanialbert@gmail.com"}
response = requests.get(url, headers=headers)
xbrlfeed = feedparser.parse(response.content)

if xbrlfeed.entries:
    for i, entry in enumerate(xbrlfeed.entries[:5]):
        #print(f"\nEntry {i} keys: {entry.keys()}")
        print(f"Company Name: {entry.get('edgar_companyname')}")
        print(f"Form Type: {entry.get('edgar_formtype')}")
        print(f"Filing Date: {entry.get('edgar_filingdate')}")
        print(f"CIK: {entry.get('edgar_ciknumber')}")

# === 2. Convert entries to a list of dictionaries ===
entries = []
for entry in xbrlfeed.entries:
    entries.append({
        "company_name": entry.get("edgar_companyname"),
        "form_type": entry.get("edgar_formtype"),
        "filing_date": entry.get("edgar_filingdate"),
        "cik": entry.get("edgar_ciknumber"),
        "title": entry.get("title"),
        "link": entry.get("link"),
        "pub_date": entry.get("published")
    })

# === 3. Connect to PostgreSQL ===
conn = psycopg2.connect(
    host="localhost",
    dbname="fsds",
    user="postgres",
    password="locale",
    port="5432"
)
cur = conn.cursor()

# === 4. Create table if it doesn't exist ===
cur.execute("""
    CREATE TABLE IF NOT EXISTS sec_xbrl_feed (
        id SERIAL PRIMARY KEY,
        company_name TEXT,
        form_type TEXT,
        filing_date DATE,
        cik TEXT,
        title TEXT,
        link TEXT,
        pub_date TIMESTAMP,
        UNIQUE(cik, form_type, filing_date)
    );
""")

# === 5. Insert entries into the table ===
for e in entries:
    try:
        # Convert date strings to proper formats
        filing_date = datetime.strptime(e["filing_date"], "%Y-%m-%d").date() if e["filing_date"] else None
        pub_date = datetime.strptime(e["pub_date"], "%a, %d %b %Y %H:%M:%S %Z") if e["pub_date"] else None

        cur.execute("""
            INSERT INTO sec_xbrl_feed (company_name, form_type, filing_date, cik, title, link, pub_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cik, form_type, filing_date) DO NOTHING;
        """, (e["company_name"], e["form_type"], filing_date, e["cik"],
              e["title"], e["link"], pub_date))
    except Exception as err:
        print(f"Skipping entry due to error: {err}")

# === 6. Finalize ===
conn.commit()
cur.close()
conn.close()

print(f"{len(entries)} entries processed and posted to PostgreSQL.")
