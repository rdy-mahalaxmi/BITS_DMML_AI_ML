import os
import re
import joblib
import json
import pandas as pd
from datetime import datetime
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OrdinalEncoder, LabelEncoder
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import mlflow
import mlflow.sklearn

# -----------------------------
# Directories
# -----------------------------
WH_DIR = "warehouse"
os.makedirs(WH_DIR, exist_ok=True)
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# -----------------------------
# Load Latest Versioned Dataset
# -----------------------------
def get_latest_timestamp():
    files = [f for f in os.listdir(WH_DIR) if re.match(r"X_train_\d{8}_\d{6}\.csv", f)]
    if not files:
        raise FileNotFoundError("No versioned X_train file found. Run feature store first.")
    files.sort()
    latest_file = files[-1]
    timestamp = re.search(r"X_train_(\d{8}_\d{6})\.csv", latest_file).group(1)
    print(f"[Info] Using latest dataset version with timestamp: {timestamp}")
    return timestamp

timestamp = get_latest_timestamp()

X_train = pd.read_csv(os.path.join(WH_DIR, f"X_train_{timestamp}.csv"))
X_test  = pd.read_csv(os.path.join(WH_DIR, f"X_test_{timestamp}.csv"))
y_train = pd.read_csv(os.path.join(WH_DIR, f"y_train_{timestamp}.csv"))
y_test  = pd.read_csv(os.path.join(WH_DIR, f"y_test_{timestamp}.csv"))

# -----------------------------
# Drop ID-like columns
# -----------------------------
if 'customerID' in X_train.columns:
    X_train = X_train.drop(columns=['customerID'])
    X_test  = X_test.drop(columns=['customerID'])

# -----------------------------
# Encode Categorical Features Automatically
# -----------------------------
cat_columns = X_train.select_dtypes(include=['object']).columns.tolist()
encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

if cat_columns:
    X_train[cat_columns] = encoder.fit_transform(X_train[cat_columns].astype(str))
    X_test[cat_columns] = encoder.transform(X_test[cat_columns].astype(str))

# -----------------------------
# Encode Target
# -----------------------------
target_col = 'Churn'
le_target = LabelEncoder()
y_train_enc = le_target.fit_transform(y_train[target_col].astype(str))
y_test_enc  = le_target.transform(y_test[target_col].astype(str))

# -----------------------------
# MLflow Setup
# -----------------------------
mlflow.set_experiment("Churn_Prediction")
model_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# -----------------------------
# Train Models
# -----------------------------
models = {
    "DecisionTree": DecisionTreeClassifier(max_depth=5, random_state=42),
    "LogisticRegression": LogisticRegression(max_iter=500, random_state=42),
    "RandomForest": RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
}

performance_report = {}

for name, model in models.items():
    with mlflow.start_run(run_name=f"{name}_{model_timestamp}"):
        print(f"\n[Train] Training {name}...")
        model.fit(X_train, y_train_enc)
        preds = model.predict(X_test)

        acc  = accuracy_score(y_test_enc, preds)
        prec = precision_score(y_test_enc, preds)
        rec  = recall_score(y_test_enc, preds)
        f1   = f1_score(y_test_enc, preds)

        print(f"{name} Performance:")
        print(f"  Accuracy : {acc:.4f}")
        print(f"  Precision: {prec:.4f}")
        print(f"  Recall   : {rec:.4f}")
        print(f"  F1 Score : {f1:.4f}")

        performance_report[name] = {
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1
        }

        # Log model and metrics in MLflow
        mlflow.log_params(model.get_params())
        mlflow.log_metrics({
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1
        })
        mlflow.sklearn.log_model(
    sk_model=model,
    artifact_path="model",
    registered_model_name="churn_model"  
)


# -----------------------------
# Save Encoders
# -----------------------------
if cat_columns:
    joblib.dump(encoder, os.path.join(WH_DIR, f"encoder_{model_timestamp}.pkl"))
joblib.dump(le_target, os.path.join(WH_DIR, f"label_encoder_target_{model_timestamp}.pkl"))

# -----------------------------
# Save Performance Report
# -----------------------------
report_path = os.path.join(REPORT_DIR, f"model_performance_{model_timestamp}.json")
with open(report_path, "w") as f:
    json.dump(performance_report, f, indent=4)

print(f"\n[Info] Model performance report saved at {report_path}")
print(f"[Info] Models and encoders saved with timestamp {model_timestamp}")
print(f"[Info] MLflow logged all models and metrics for this run")
