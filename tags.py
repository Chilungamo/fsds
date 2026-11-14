import requests
import pandas as pd

cik = input()  # '1959348' K Kellog.
headers = {"User-Agent": "albertlimani@yahoo.com"}
url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik.zfill(10)}.json"

response = requests.get(url, headers=headers)
data = response.json()

all_tags = []

for namespace, tags in data['facts'].items():
    for tag, details in tags.items():
        all_tags.append({
            "namespace": namespace,
            "tag": tag,
            "description": details.get('label', ''),
            "periods": len(details.get('periods', [])),
            "balance_sheet": details.get('balance', [])[0] if 'balance' in details else None,
            "units": list(details.get('units', {}).keys())
        })

df_tags = pd.DataFrame(all_tags)

standard_namespaces = {"us-gaap", "ifrs-full", "dei", "srt", "ebpt", "grt"}  # can be expanded
df_tags["tag_type"] = df_tags["namespace"].apply(
    lambda ns: "standard" if ns in standard_namespaces else "custom"
)

# Print out all tags
print(df_tags)

#filter and show only custom tags
custom_tags = df_tags[df_tags['tag_type'] == 'custom']
print("\n🛠️ Custom Tags:")

# Count the number of category of tags
tagCount = len(df_tags[df_tags['tag_type'] == 'standard'])
print(f"🔢 Total standard tags: {tagCount}")


https://www.sec.gov/files/dera/data/financial-statement-data-sets/2025q3.zip