import os
import pandas as pd
import requests
import logging
from prefect import flow, task
from prefect.logging import get_run_logger

# -----------------------------
# Setup Directories & Logging
# -----------------------------
RAW_DIR = "data/raw"
STORAGE_DIR = "data/lake"
LOG_DIR = "logs"

# Create directories if not present
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STORAGE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# Configure a dedicated file handler
log_file = os.path.join(LOG_DIR, "ingest.log")
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Attach file handler to Prefect's logger
prefect_logger = logging.getLogger("prefect")
prefect_logger.addHandler(file_handler)
prefect_logger.setLevel(logging.INFO)

# -----------------------------
# Prefect Tasks
# -----------------------------
@task(retries=3, retry_delay_seconds=30)
def ingest_csv():
    logger = get_run_logger()
    url = "https://raw.githubusercontent.com/SohelRaja/Customer-Churn-Analysis/master/Decision%20Tree/WA_Fn-UseC_-Telco-Customer-Churn.csv"
    df = pd.read_csv(url)
    filepath = f"{RAW_DIR}/churn.csv"
    df.to_csv(filepath, index=False)
    logger.info(f"CSV ingestion successful. Data saved at {filepath}")

@task(retries=3, retry_delay_seconds=30)
def ingest_api():
    logger = get_run_logger()
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    filepath = f"{RAW_DIR}/users.csv"
    df.to_csv(filepath, index=False)
    logger.info(f"API ingestion successful. Data saved at {filepath}")

# -----------------------------
# Prefect Flow
# -----------------------------
@flow(name="customer-churn-ingestion-flow")
def run_all():
    logger = get_run_logger()
    logger.info("Starting ingestion flow...")
    ingest_csv()
    ingest_api()
    logger.info("Ingestion flow completed successfully.")

# -----------------------------
# Script Entry Point
# -----------------------------
if __name__ == "__main__":
    run_all()
