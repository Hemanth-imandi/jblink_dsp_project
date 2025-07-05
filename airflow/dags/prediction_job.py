from airflow.decorators import dag, task
from airflow.utils.session import provide_session
from airflow.models import DagRun, TaskInstance
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from datetime import datetime, timedelta
import requests
import os
import json
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration using environment variables
BASE_DATA_DIR = os.getenv("BASE_DIR", "/opt/airflow")
GOOD_DATA_FOLDER = os.path.join(BASE_DATA_DIR, "data_ingestion", "good_data")
PROCESSED_DIR = os.path.join(BASE_DATA_DIR, "data_ingestion", "processed")
PROCESSED_FILES_PATH = os.path.join(PROCESSED_DIR, "processed_prediction_files.json")

# API URL - could also be from environment
API_URL = os.getenv("PREDICTION_API_URL", "http://jblink_fastapi:8000") + "/predict"

# Create logger
logger = LoggingMixin().log

# Log configuration
logger.info(f"Prediction Job Configuration:")
logger.info(f"BASE_DATA_DIR: {BASE_DATA_DIR}")
logger.info(f"GOOD_DATA_FOLDER: {GOOD_DATA_FOLDER}")
logger.info(f"API_URL: {API_URL}")

# Ensure required directories exist
for folder in [BASE_DATA_DIR, GOOD_DATA_FOLDER, PROCESSED_DIR]:
    try:
        os.makedirs(folder, exist_ok=True)
        logger.info(f"Created or confirmed directory: {folder}")
    except Exception as e:
        logger.error(f"Error creating directory {folder}: {str(e)}")

# Safe conversion function
def safe_int(val):
    """Safely convert value to integer, return 0 if conversion fails"""
    try:
        return int(val) if pd.notna(val) else 0
    except (ValueError, TypeError):
        return 0

def safe_float(val):
    """Safely convert value to float, return 0.0 if conversion fails"""
    try:
        return float(val) if pd.notna(val) else 0.0
    except (ValueError, TypeError):
        return 0.0

# Track processed files
def get_processed_files():
    """Load list of previously processed files from JSON"""
    if os.path.exists(PROCESSED_FILES_PATH):
        try:
            with open(PROCESSED_FILES_PATH, 'r') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} previously processed files")
                return data
        except json.JSONDecodeError as e:
            logger.warning(f"Error reading processed files JSON: {e}. Starting fresh.")
            return []
    return []

def update_processed_files(new_files):
    """Update the list of processed files with new files"""
    processed_files = get_processed_files()
    processed_files.extend(new_files)
    
    # Keep only the most recent 1000 files to prevent the file from growing too large
    if len(processed_files) > 1000:
        processed_files = processed_files[-1000:]
    
    os.makedirs(os.path.dirname(PROCESSED_FILES_PATH), exist_ok=True)
    with open(PROCESSED_FILES_PATH, 'w') as f:
        json.dump(processed_files, f, indent=2)
    logger.info(f"Updated processed files list. Total tracked: {len(processed_files)}")

# Define DAG
@dag(
    dag_id='prediction_job',
    description='Automated prediction job on newly ingested files',
    schedule_interval=timedelta(minutes=2),  # Every 2 minutes as required
    start_date=datetime(2024, 2, 13),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
)
def prediction_job():
    
    @task(task_id="check_for_new_data")
    def check_for_new_data(**kwargs):
        """
        Check for new files in the good_data folder that haven't been processed yet.
        Returns list of new files or raises AirflowSkipException if none found.
        """
        if not os.path.exists(GOOD_DATA_FOLDER):
            logger.warning(f"Good data folder {GOOD_DATA_FOLDER} doesn't exist, creating it")
            os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)

        try:
            # Get all CSV files in good_data folder
            all_files = []
            for f in os.listdir(GOOD_DATA_FOLDER):
                file_path = os.path.join(GOOD_DATA_FOLDER, f)
                if os.path.isfile(file_path) and f.endswith('.csv'):
                    all_files.append(file_path)
            
            logger.info(f"Found {len(all_files)} total CSV files in good_data folder")
            
        except Exception as e:
            logger.error(f"Error listing files in {GOOD_DATA_FOLDER}: {str(e)}")
            raise AirflowSkipException(f"Error accessing good_data folder: {str(e)}")

        # Get previously processed files
        processed_files = get_processed_files()
        
        # Find new files (not yet processed)
        new_files = [f for f in all_files if f not in processed_files]

        if not new_files:
            logger.info("No new files found. Skipping this DAG run.")
            raise AirflowSkipException("No new files to process")

        logger.info(f"Found {len(new_files)} new files to process:")
        for file_path in new_files:
            logger.info(f"  - {os.path.basename(file_path)}")
        
        return new_files

    @task(task_id="make_predictions")
    def make_predictions(new_files):
        """
        Make predictions on new files by calling the FastAPI model service.
        Uses batch processing for efficiency.
        """
        if not new_files:
            logger.warning("No files to process.")
            return {"processed_files": 0, "total_predictions": 0}

        results = []
        total_predictions = 0

        for file_path in new_files:
            logger.info(f"Processing file: {os.path.basename(file_path)}")
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                logger.info(f"Loaded {len(df)} rows from {os.path.basename(file_path)}")

                # Prepare customer data for API call
                customers = []
                for idx, row in df.iterrows():
                    customer = {
                        "customer_id": str(row.get("Customer ID", f"CUST{idx}")),
                        "tenure_in_months": safe_int(row.get("Tenure in Months")),
                        "monthly_charge": safe_float(row.get("Monthly Charge")),
                        "total_customer_svc_requests": safe_int(row.get("Total Customer Svc Requests"))
                    }
                    customers.append(customer)

                # Make API call with batch data
                payload = {"customers": customers}
                response = requests.post(
                    API_URL, 
                    json=payload, 
                    params={"source": "scheduled predictions"},
                    timeout=60  # Add timeout for large batches
                )

                if response.status_code == 200:
                    prediction_result = response.json()
                    logger.info(f"Successfully received predictions for {os.path.basename(file_path)}")

                    # Save predictions alongside original data
                    filename = os.path.basename(file_path)
                    prediction_file = os.path.join(os.path.dirname(file_path), f"pred_{filename}")

                    if isinstance(prediction_result, dict) and "predictions" in prediction_result:
                        predictions = prediction_result["predictions"]
                        if len(predictions) == len(df):
                            # Add predictions to the dataframe
                            df['prediction'] = [p.get("prediction", "Unknown") for p in predictions]
                            df['prediction_source'] = "scheduled predictions"
                            df['processed_at'] = datetime.now().isoformat()
                            
                            # Save enhanced file with predictions
                            df.to_csv(prediction_file, index=False)
                            logger.info(f"Saved {len(predictions)} predictions to {prediction_file}")
                            total_predictions += len(predictions)
                        else:
                            logger.warning(f"Prediction count mismatch: {len(predictions)} predictions for {len(df)} rows")

                    results.append({
                        "file": file_path, 
                        "status": "success", 
                        "row_count": len(df),
                        "prediction_count": len(prediction_result.get("predictions", [])),
                        "api_response": prediction_result.get("status", "unknown")
                    })
                else:
                    logger.error(f"Prediction API error for {os.path.basename(file_path)}: {response.status_code} - {response.text}")
                    results.append({
                        "file": file_path, 
                        "status": "error", 
                        "message": f"API error: {response.status_code} - {response.text}"
                    })

            except Exception as e:
                logger.error(f"Error processing file {os.path.basename(file_path)}: {str(e)}")
                results.append({
                    "file": file_path, 
                    "status": "error", 
                    "message": str(e)
                })

        # Update processed files list
        update_processed_files(new_files)
        
        # Summary logging
        successful_files = [r for r in results if r["status"] == "success"]
        failed_files = [r for r in results if r["status"] == "error"]
        
        logger.info(f"Prediction job completed:")
        logger.info(f"  - Successful files: {len(successful_files)}")
        logger.info(f"  - Failed files: {len(failed_files)}")
        logger.info(f"  - Total predictions made: {total_predictions}")
        
        return {
            "processed_files": len(new_files),
            "successful_files": len(successful_files),
            "failed_files": len(failed_files),
            "total_predictions": total_predictions,
            "results": results
        }

    # DAG flow - check for new data, then make predictions
    new_files = check_for_new_data()
    predictions = make_predictions(new_files)
    
    # Explicit dependency
    new_files >> predictions

# Instantiate the DAG
prediction_job_dag = prediction_job()