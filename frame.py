import requests
import feedparser
# import psycopg2
from datetime import datetime

#print("Bozo:", feed.bozo)
#print("Feed has", len(feed.entries), "entries") # checks number of feed entries


headers = {"User-Agent": "limanialbert@gmail.com"}

# List of concepts to check
concepts = [
    "AccountsReceivableNetCurrent",
    "Revenues",
    "NetIncomeLoss",
    "NetCashProvidedByUsedInOperatingActivities"
    
]

# Time periods
years = range(2020, 2024)
quarters = ["Q1", "Q2", "Q3", "Q4"]

# Check for each concept, year, and quarter
for concept in concepts:
    for y in years:
        for q in quarters:
            url = f"https://data.sec.gov/api/xbrl/frames/us-gaap/{concept}/USD/CY{y}{q}.json"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                print(f"✅ Found: {url}")
            else:
                print(f"❌ Not found: {url} (status {response.status_code})")








