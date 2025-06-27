import requests
from bs4 import BeautifulSoup
import re

# STEP 1: Set the 10-K filing URL (HTML format)
filing_url = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000092/aapl-20230930.htm"  # Example: Apple 10-K

# STEP 2: Request the HTML content
headers = {"User-Agent": "your-email@example.com"}
response = requests.get(filing_url, headers=headers)

if response.status_code != 200:
    raise Exception(f"Failed to retrieve filing: {response.status_code}")

# STEP 3: Parse the HTML content
soup = BeautifulSoup(response.text, "html.parser")
text = soup.get_text(separator="\n")  # get raw text for easier pattern matching

# STEP 4: Locate "Item 7" (MD&A) section using regex
pattern = r"(?i)item\s+7\.*\s*management(?:’s|'s)? discussion and analysis.*?(?=item\s+7A|item\s+8|item\s+9|\Z)"
match = re.search(pattern, text, re.DOTALL)

# STEP 5: Display result
if match:
    mda_section = match.group(0).strip()
    print("✅ Extracted MD&A Section:\n")
    print(mda_section[:3000])  # Print first 3000 chars
else:
    print("❌ MD&A section not found.")
