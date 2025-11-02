from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
import requests
import os
import zipfile

DATA_DIR = "/opt/shared-data/abr_xml_data"
# https://data.gov.au/data/dataset/activity/abn-bulk-extract
URLS = [
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip"
]

@dag(
    dag_id="download_and_unzip_data",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["abr", "download", "unzip"]
)
def download_and_unzip_dag():

    @task
    def download_files():
        os.makedirs(DATA_DIR, exist_ok=True)
        file_paths = []
        for url in URLS:
            filename = os.path.join(DATA_DIR, url.split("/")[-1])
            print(f"Downloading {url} → {filename}")
            response = requests.get(url)
            response.raise_for_status()
            with open(filename, "wb") as f:
                f.write(response.content)
            file_paths.append(filename)
        return file_paths

    @task
    def unzip_and_delete(file_paths: list):
        for file_path in file_paths:
            print(f"Unzipping {file_path}")
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(DATA_DIR)
            print(f"Deleting {file_path}")
            os.remove(file_path)
        print(f"All files extracted and cleaned up in {DATA_DIR}")

    unzip_and_delete(download_files())


dag = download_and_unzip_dag()
