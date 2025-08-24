import os
import json
import shutil
import pandas as pd
import requests
from sklearn.model_selection import train_test_split
from datetime import datetime

# -----------------------------
# Directories
# -----------------------------
RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
WH_DIR = "warehouse"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)
os.makedirs(WH_DIR, exist_ok=True)

# -----------------------------
# Feature Metadata (initial)
# -----------------------------
feature_metadata = {
    "customerID": {"description": "Unique customer ID", "source": "csv", "version": 1},
    "gender": {"description": "Customer gender", "source": "csv", "version": 1},
    "SeniorCitizen": {"description": "Is senior citizen (0/1)", "source": "csv", "version": 1},
    "tenure": {"description": "Number of months the customer has stayed", "source": "csv", "version": 1},
    "TotalCharges": {"description": "Total charges of the customer", "source": "csv", "version": 1},
    "Churn": {"description": "Churn label (Yes/No)", "source": "csv", "version": 1},
    "username": {"description": "API user name", "source": "api", "version": 1},
    "email": {"description": "API user email", "source": "api", "version": 1},
}

# -----------------------------
# Feature Store with Versioning
# -----------------------------
class FeatureStore:
    def __init__(self, raw_dir, proc_dir, warehouse_dir, metadata):
        self.raw_dir = raw_dir
        self.proc_dir = proc_dir
        self.wh_dir = warehouse_dir
        self.metadata = metadata
        self.df = None
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # -------- Load Data --------
    def load_csv(self, csv_url):
        df_csv = pd.read_csv(csv_url)
        # Version raw CSV
        raw_file_versioned = os.path.join(self.raw_dir, f"churn_raw_{self.timestamp}.csv")
        df_csv.to_csv(raw_file_versioned, index=False)
        print(f"[FeatureStore] Loaded CSV ({len(df_csv)} rows), versioned as {raw_file_versioned}")
        return df_csv

    def load_api(self, api_url):
        response = requests.get(api_url)
        response.raise_for_status()
        df_api = pd.DataFrame(response.json())
        print(f"[FeatureStore] Loaded API data ({len(df_api)} rows)")
        return df_api

    # -------- Build Feature Store --------
    def build_feature_store(self, csv_url, api_url=None):
        df_csv = self.load_csv(csv_url)
        
        if api_url:
            df_api = self.load_api(api_url)
            # Align API data with CSV rows
            df_api = df_api.reindex(df_csv.index, method='ffill')
            self.df = pd.concat([df_csv, df_api[['username','email']]], axis=1)
        else:
            self.df = df_csv

        # Version transformed data
        transformed_file_versioned = os.path.join(self.proc_dir, f"transformed_{self.timestamp}.csv")
        self.df.to_csv(transformed_file_versioned, index=False)
        print(f"[FeatureStore] Transformed dataset saved as {transformed_file_versioned}")

        # Save feature metadata
        meta_file_versioned = os.path.join(self.wh_dir, f"feature_metadata_{self.timestamp}.json")
        with open(meta_file_versioned, "w") as f:
            json.dump(self.metadata, f, indent=4)
        print(f"[FeatureStore] Feature metadata saved as {meta_file_versioned}")

        # Save version metadata
        version_meta = {
            "timestamp": self.timestamp,
            "raw_data": f"churn_raw_{self.timestamp}.csv",
            "transformed_data": f"transformed_{self.timestamp}.csv",
            "source": {"csv": csv_url, "api": api_url},
            "changes": "Initial ingestion + API merge"
        }
        version_file = os.path.join(self.proc_dir, f"version_metadata_{self.timestamp}.json")
        with open(version_file, "w") as f:
            json.dump(version_meta, f, indent=4)
        print(f"[FeatureStore] Version metadata saved as {version_file}")

    # -------- Split & Store --------
    def split_and_store(self, target_col, test_size=0.2, random_state=42):
        X = self.df.drop(target_col, axis=1)
        y = self.df[target_col]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

        # Save train/test sets with version
        X_train.to_csv(os.path.join(self.wh_dir, f"X_train_{self.timestamp}.csv"), index=False)
        X_test.to_csv(os.path.join(self.wh_dir, f"X_test_{self.timestamp}.csv"), index=False)
        y_train.to_csv(os.path.join(self.wh_dir, f"y_train_{self.timestamp}.csv"), index=False)
        y_test.to_csv(os.path.join(self.wh_dir, f"y_test_{self.timestamp}.csv"), index=False)
        print(f"[FeatureStore] Train/Test sets saved with timestamp {self.timestamp}")

    # -------- Feature Retrieval --------
    def get_feature(self, feature_name):
        if feature_name in self.df.columns:
            return self.df[feature_name]
        else:
            raise ValueError(f"Feature '{feature_name}' not found in feature store.")

# -----------------------------
# Run Example
# -----------------------------
if __name__ == "__main__":
    CSV_URL = "https://raw.githubusercontent.com/SohelRaja/Customer-Churn-Analysis/master/Decision%20Tree/WA_Fn-UseC_-Telco-Customer-Churn.csv"
    API_URL = "https://jsonplaceholder.typicode.com/users"

    fs = FeatureStore(RAW_DIR, PROC_DIR, WH_DIR, feature_metadata)
    fs.build_feature_store(CSV_URL, api_url=API_URL)
    fs.split_and_store(target_col="Churn")

    # Sample retrieval
    print(fs.get_feature("TotalCharges").head())
    print(fs.get_feature("username").head())
