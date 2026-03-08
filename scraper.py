from apify_client import ApifyClient
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from datetime import datetime
import os
import json
from io import BytesIO

load_dotenv()

# ---------- APIFY SETUP ----------
client = ApifyClient(os.getenv("APIFY_API_KEY"))

run_input = {
    "startUrls": [
        "https://www.99acres.com/search/property/rent/serviced-apartments/central-mumbai?city=14&property_type=22&preference=R&area_unit=1&res_com=R&isPreLeased=N"
    ],
    "maxItems": 200,
    "sortBy": "relevance"
}

# Run scraper
run = client.actor("3bs4FX1ID9mwMQNB6").call(run_input=run_input)

# Collect results
data = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    data.append(item)

print(f"Collected {len(data)} records")

# ---------- ADLS SETUP ----------
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

file_system_client = service_client.get_file_system_client(container_name)

# ---------- CREATE DATE PARTITION ----------
now = datetime.now()

path = f"raw/real_estate/{now.year}/{now.month}/{now.day}/data.json"

file_client = file_system_client.get_file_client(path)

# Convert JSON to bytes
data_bytes = BytesIO(json.dumps(data, indent=2).encode())

# Upload
file_client.upload_data(data_bytes, overwrite=True)

print(f"Uploaded data to ADLS path: {path}")