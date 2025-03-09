import os
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Base URL for finance datasets
BASE_URL = "https://data.gov.ma/data/fr/dataset/?q=&groups=finance&sort=score+desc%2C+metadata_modified+desc"

# Create directories
DOWNLOAD_DIR = "downloaded_files"
CLEANED_DIR = "cleaned_files"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

# Fetch dataset pages using requests
response = requests.get(BASE_URL)
soup = BeautifulSoup(response.content, "html.parser")

# Extract dataset page links
dataset_links = ["https://data.gov.ma" + a["href"] for a in soup.select("h3.dataset-heading a")]
print(f"🔍 Found {len(dataset_links)} datasets.")

# Extract and download data files
file_paths = []
for dataset_url in dataset_links:
    dataset_response = requests.get(dataset_url)
    dataset_soup = BeautifulSoup(dataset_response.content, "html.parser")
    
    for file_link in dataset_soup.select("a.resource-url-analytics"):
        file_url = file_link["href"]
        if file_url.endswith((".csv", ".xls", ".xlsx", ".json")):
            file_name = os.path.join(DOWNLOAD_DIR, file_url.split("/")[-1])
            file_response = requests.get(file_url)
            with open(file_name, "wb") as f:
                f.write(file_response.content)
            print(f"✅ Downloaded: {file_name}")
            file_paths.append(file_name)

# Clean data function
def clean_data(file_path):
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path, encoding="utf-8")
        elif file_path.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path)
        else:
            print(f"❌ Unsupported format: {file_path}")
            return None

        # Drop duplicates
        df = df.drop_duplicates()

        # Fill missing values
        df = df.fillna(method="ffill")

        # Standardize column names
        df.columns = df.columns.str.replace(r"[^a-zA-Z0-9]", "_", regex=True).str.lower()

        # Save cleaned data
        cleaned_path = os.path.join(CLEANED_DIR, os.path.basename(file_path).replace(".xls", ".csv"))
        df.to_csv(cleaned_path, index=False)
        print(f"✅ Cleaned and saved: {cleaned_path}")
        return cleaned_path

    except Exception as e:
        print(f"⚠️ Error cleaning {file_path}: {e}")

# Clean downloaded files
for file_path in file_paths:
    clean_data(file_path)

print("🎯 **Process Complete!** All datasets scraped, cleaned, and saved.")
