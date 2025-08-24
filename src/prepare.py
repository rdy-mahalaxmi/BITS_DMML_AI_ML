# prepare.py
import pandas as pd
import os
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import logging

# -----------------------------
# Directories & Logging
# -----------------------------
RAW_DIR = "data/lake"
PROC_DIR = "data/processed"
REPORT_DIR = "data/reports"
LOG_DIR = "logs"

os.makedirs(PROC_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=f"{LOG_DIR}/prepare.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("prepare")

# -----------------------------
# Helper: get latest file for a source
# -----------------------------
def get_latest_file(folder):
    all_dates = sorted(os.listdir(folder), reverse=True)
    if not all_dates:
        raise FileNotFoundError(f"No folders found in {folder}")
    latest_folder = os.path.join(folder, all_dates[0])
    files = os.listdir(latest_folder)
    if not files:
        raise FileNotFoundError(f"No files found in {latest_folder}")
    return os.path.join(latest_folder, files[0])

# -----------------------------
# Data Preparation Function
# -----------------------------
def prepare_dataset(file_path, dataset_name):
    df = pd.read_csv(file_path)
    logger.info(f"Preparing dataset: {file_path}")
    print(f"[Prepare] Dataset: {file_path} | Rows: {df.shape[0]} | Columns: {df.shape[1]}")

    # -----------------------------
    # Handle missing values
    # -----------------------------
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
    
    # Convert TotalCharges to numeric if exists
    if 'TotalCharges' in df.columns:
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        df['TotalCharges'] = df['TotalCharges'].fillna(df['TotalCharges'].median())

    # -----------------------------
    # Encode categorical variables
    # -----------------------------
    if 'Churn' in df.columns:
        df["Churn"] = df["Churn"].map({"Yes": 1, "No": 0})  # target encoding

    # Drop identifiers if exists
    if 'customerID' in df.columns:
        df = df.drop('customerID', axis=1)

    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    if categorical_cols:
        df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

    # -----------------------------
    # Standardize numerical attributes
    # -----------------------------
    scaler = StandardScaler()
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

    # -----------------------------
    # Save prepared dataset
    # -----------------------------
    output_path = os.path.join(PROC_DIR, f"{dataset_name}_prepared.csv")
    df.to_csv(output_path, index=False)
    print(f"[Prepare] Prepared dataset saved at {output_path}")
    logger.info(f"Prepared dataset saved at {output_path}")

    # -----------------------------
    # EDA: visualizations and summary stats
    # -----------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Histograms for numeric columns
    for col in numeric_cols:
        plt.figure(figsize=(6,4))
        sns.histplot(df[col], bins=30, kde=True)
        plt.title(f"{dataset_name} - {col} distribution")
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, f"{dataset_name}_{col}_hist_{timestamp}.png"))
        plt.close()

    # Boxplots for numeric columns
    for col in numeric_cols:
        plt.figure(figsize=(6,4))
        sns.boxplot(y=df[col])
        plt.title(f"{dataset_name} - {col} boxplot")
        plt.tight_layout()
        plt.savefig(os.path.join(REPORT_DIR, f"{dataset_name}_{col}_box_{timestamp}.png"))
        plt.close()

    # Summary statistics
    summary_path = os.path.join(REPORT_DIR, f"{dataset_name}_summary_{timestamp}.csv")
    df.describe().to_csv(summary_path)
    print(f"[Prepare] Summary statistics saved at {summary_path}")
    logger.info(f"Summary statistics saved at {summary_path}")

# -----------------------------
# Main Runner
# -----------------------------
def run_preparation():
    sources = ["csv_source", "api_source"]
    for source in sources:
        source_path = os.path.join(RAW_DIR, source)
        try:
            latest_file = get_latest_file(source_path)
            dataset_name = os.path.splitext(os.path.basename(latest_file))[0]
            prepare_dataset(latest_file, dataset_name)
        except FileNotFoundError as e:
            print(f"[Prepare] Warning: {e}")
            logger.warning(e)

if __name__ == "__main__":
    run_preparation()
