# local_storage.py
from pathlib import Path
from datetime import datetime
import shutil
import logging

# -----------------------------
# Setup Base Directories & Logging
# -----------------------------
BASE_DIR = Path(__file__).parent.parent.resolve()  # Project root
RAW_DIR = BASE_DIR / "data" / "raw"
STORAGE_DIR = BASE_DIR / "data" / "lake"
LOG_DIR = BASE_DIR / "logs"

STORAGE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "storage.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("storage")

# -----------------------------
# Helper: Store a file into partitioned folders
# -----------------------------
def store_file(filename: str, source_type: str):
    """Copy a raw file into a timestamped folder under its source type"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir = STORAGE_DIR / source_type / timestamp
    dest_dir.mkdir(parents=True, exist_ok=True)

    src_file = RAW_DIR / filename
    dest_file = dest_dir / filename

    if not src_file.exists():
        msg = f"[Storage] ERROR: File {filename} not found in {RAW_DIR}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    shutil.copy(src_file, dest_file)
    msg = f"[Storage] {filename} → {dest_file}"
    logger.info(msg)
    print(msg)


# -----------------------------
# Main Runner
# -----------------------------
def run_storage():
    logger.info("Starting local storage process...")

    # List of files to store
    files_to_store = [
        ("churn.csv", "csv_source"),
        ("users.csv", "api_source")
    ]

    for filename, source_type in files_to_store:
        store_file(filename, source_type)

    logger.info("Local storage process completed successfully.")


# -----------------------------
# For standalone run
# -----------------------------
if __name__ == "__main__":
    try:
        run_storage()
    except Exception as e:
        print(f"[Storage] Failed: {e}")
        raise
