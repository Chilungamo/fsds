import requests
import pandas as pd
from collections import Counter

def form_summary(cik): #prints a list and count of forms filed by a company by accepting cik as a string
    cik = str(cik).zfill(10)
    url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    forms = requests.get(url, headers={'User-Agent': 'limanialbert@gmail.com'}).json()['filings']['recent']['form']
    df = pd.Series(forms).value_counts().reset_index()
    df.columns = ['formType', 'count']
    print(df.to_string(index=False))

form_summary("0001959348")




