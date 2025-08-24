from prefect import flow, task, get_run_logger
import subprocess
import sys

# -----------------------------
# Define Tasks
# -----------------------------
@task(retries=2, retry_delay_seconds=10)
def ingest():
    logger = get_run_logger()
    logger.info("Starting ingestion...")
    subprocess.check_call([sys.executable, "src/ingest.py"])
    logger.info("Ingestion completed.")

@task(retries=2, retry_delay_seconds=10)
def local_storage():
    logger = get_run_logger()
    logger.info("Storing data locally...")
    # subprocess.check_call([sys.executable, "../src/local_storage.py"])
    logger.info("Local storage done.")

@task(retries=2, retry_delay_seconds=10)
def validate():
    logger = get_run_logger()
    logger.info("Validating data...")
    subprocess.check_call([sys.executable, "src/validate.py"])
    logger.info("Validation completed.")

@task(retries=2, retry_delay_seconds=10)
def prepare():
    logger = get_run_logger()
    logger.info("Preparing data...")
    subprocess.check_call([sys.executable, "src/prepare.py"])
    logger.info("Preparation completed.")

@task(retries=2, retry_delay_seconds=10)
def transform():
    logger = get_run_logger()
    logger.info("Transforming data...")
    subprocess.check_call([sys.executable, "src/transform.py"])
    logger.info("Transformation completed.")

@task(retries=2, retry_delay_seconds=10)
def feature_store():
    logger = get_run_logger()
    logger.info("Storing features in feature store...")
    subprocess.check_call([sys.executable, "src/feature_store.py"])
    logger.info("Feature store updated.")

@task(retries=2, retry_delay_seconds=10)
def train():
    logger = get_run_logger()
    logger.info("Training model...")
    subprocess.check_call([sys.executable, "src/train.py"])
    logger.info("Model training completed.")

# -----------------------------
# Define Flow
# -----------------------------
@flow(name="Churn Prediction Pipeline")
def churn_pipeline():
    # Just call tasks in order, Prefect handles scheduling and retries
    ingest()
    local_storage()
    validate()
    prepare()
    transform()
    feature_store()
    train()
	
def setup_scheduled_deployment():
    """Set up scheduled deployment using modern Prefect 3.x serve() method"""
    print("Setting up scheduled deployment using flow.serve()...")
   
    # Use the modern serve() method with cron schedule
    churn_pipeline.serve(
        name="churn_pipeline_5min",
        cron="*/5 * * * *",  # runs every 5 minutes
        tags=["churn", "ml", "scheduled"],
        description="Automated churn prediction pipeline running every 5 minutes"
    )

if __name__ == "__main__":
    #churn_pipeline()
	setup_scheduled_deployment()

