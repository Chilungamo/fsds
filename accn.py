
import requests
import pandas as pd

#Function extract and list accession numbers from submissions SEC API endpoint using CIKs and form_summary 
def get_accessions(cik):
    cik = str(cik).zfill(10)
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                     headers={"User-Agent": "albertlimani@yahoo.com"})
    r.raise_for_status()
    df = pd.DataFrame(r.json()["filings"]["recent"])
    return df[df.form.eq("10-K")][["accessionNumber", "filingDate"]].sort_values("filingDate", ascending=False)

# Example:
print(get_accessions("0001959348"))  # WK Kellogg


# def get_10k_accessions_from_facts(cik):
#     cik = str(cik).zfill(10)
#     r = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
#                      headers={"User-Agent": "your_email@example.com"})
#     r.raise_for_status()
    
#     accessions = set()
#     for concept_data in r.json().get("facts", {}).get("us-gaap", {}).values():
#         for unit in concept_data.get("units", {}).values():
#             for entry in unit:
#                 if "10-K" in entry.get("form", ""):
#                     accessions.add(entry.get("accn"))
#     return sorted(accessions, reverse=True)
#     return df[df.form.eq("10-K")][["accessionNumber", "filingDate"]].sort_values("filingDate", ascending=False)

# # Example:
# print(get_10k_accessions_from_facts("0000320193"))  # Apple

