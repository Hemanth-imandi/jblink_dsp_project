from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.models import DagRun
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import requests
import os
import json
import pandas as pd


# Use host.docker.internal to access services running on the host machine from Docker
API_URL = "http://host.docker.internal:8000/predict"

# Directory path aligned with data_ingestion.py
GOOD_DATA_FOLDER = "/opt/airflow/data_ingestion/good_data"

# Initialize logger for consistent logging across tasks
logger = LoggingMixin().log

# Ensure required directories exist before execution
os.makedirs(GOOD_DATA_FOLDER, exist_ok=True)

def check_for_new_data(**kwargs):
    """
    Identifies CSV files in the good_data folder for processing.
    
    """
    logger.info(f"Checking for new data in folder: {GOOD_DATA_FOLDER}")
    
    # Validate directory existence
    if not os.path.exists(GOOD_DATA_FOLDER):
        logger.info(f"Good data folder {GOOD_DATA_FOLDER} doesn't exist")
        mark_dag_run_as_skipped(kwargs['run_id'])
        raise AirflowSkipException("Good data folder doesn't exist")
    
    try:
        # Inventory directory contents
        all_items = os.listdir(GOOD_DATA_FOLDER)
        logger.info(f"All items in good_data folder: {all_items}")
        
        # Filter for CSV files only
        files = [os.path.join(GOOD_DATA_FOLDER, f) for f in all_items 
                if os.path.isfile(os.path.join(GOOD_DATA_FOLDER, f)) and f.endswith('.csv')]
        
        if not files:
            logger.info("No files found in good_data folder. Skipping this DAG run.")
            mark_dag_run_as_skipped(kwargs['run_id'])
            raise AirflowSkipException("No files to process")
        
        logger.info(f"Found {len(files)} files to process: {files}")
        return files
    except Exception as e:
        logger.error(f"Error checking for new data: {str(e)}")
        mark_dag_run_as_skipped(kwargs['run_id'])
        raise AirflowSkipException(f"Error checking for new data: {str(e)}")

@provide_session
def mark_dag_run_as_skipped(dag_run_id, session=None):
    """
    Marks the current DAG run as skipped in the Airflow database.
    
    """
    dr = session.query(DagRun).filter(DagRun.id == dag_run_id).first()
    if dr:
        dr.state = "skipped"
        session.commit()
        logger.info(f"Marked DAG run {dag_run_id} as skipped")

def make_predictions(**context):
    """
    Processes CSV files and submits data to prediction API.
    """
    # Retrieve task instance from context
    ti = context['ti']
    # Get file paths from previous task using XCom
    files = ti.xcom_pull(task_ids='check_for_new_data')
    
    logger.info(f"Retrieved files from XCom: {files}")
    
    if not files:
        logger.warning("No files to process. This shouldn't happen as DAG should be skipped.")
        return []
    
    results = []
    
    for file_path in files:
        logger.info(f"Processing file: {file_path}")
        try:
            # Validate file existence
            if not os.path.exists(file_path):
                logger.error(f"File does not exist: {file_path}")
                results.append({"file": file_path, "status": "error", "message": "File does not exist"})
                continue
                
            # Load and validate CSV data
            df = pd.read_csv(file_path)
            logger.info(f"Successfully read file with {len(df)} rows")
            
            # Log sample data for debugging and validation
            logger.info(f"File sample data: {df.head(2).to_dict()}")
            
            # Prepare data for API submission
            data_records = df.to_dict(orient="records")
            payload = {"customers": data_records}
            
            # Verify API availability before sending request
            try:
                # Health check endpoint to confirm API is running
                health_check = requests.get(API_URL.rsplit('/', 1)[0] + "/health", timeout=5)
                logger.info(f"API health check response: {health_check.status_code}")
            except requests.exceptions.RequestException as e:
                logger.error(f"API not available: {str(e)}")
                results.append({"file": file_path, "status": "error", "message": f"API not available: {str(e)}"})
                continue
            
            # Submit data to prediction API
            logger.info(f"Sending request to {API_URL}")
            response = requests.post(API_URL, json=payload, timeout=30)
            
            if response.status_code == 200:
                try:
                    # Process successful API response
                    prediction_result = response.json()
                    logger.info(f"Successfully received predictions for {file_path}")
                    
                    # Prepare output file path
                    filename = os.path.basename(file_path)
                    prediction_file = os.path.join(os.path.dirname(file_path), f"pred_{filename}")
                    
                    # Validate API response format
                    if isinstance(prediction_result, dict) and "predictions" in prediction_result:
                        predictions = prediction_result["predictions"]
                        
                        # Ensure prediction count matches record count
                        if len(predictions) == len(df):
                            # Append predictions to original data and save
                            df['prediction'] = predictions
                            df.to_csv(prediction_file, index=False)
                            logger.info(f"Saved predictions to {prediction_file}")
                        else:
                            logger.error(f"Length mismatch: {len(predictions)} predictions for {len(df)} records")
                    else:
                        logger.error(f"Unexpected response format: {prediction_result}")
                    
                    results.append({"file": file_path, "status": "success"})
                except json.JSONDecodeError as je:
                    logger.error(f"Error parsing API response JSON: {str(je)}")
                    results.append({"file": file_path, "status": "error", "message": f"Invalid JSON response: {str(je)}"})
            else:
                logger.error(f"Error making predictions for {file_path}: {response.status_code} - {response.text}")
                results.append({"file": file_path, "status": "error", "message": f"API error: {response.status_code} - {response.text}"})
        
        # Handle specific CSV-related errors
        except pd.errors.EmptyDataError:
            logger.error(f"Empty data file: {file_path}")
            results.append({"file": file_path, "status": "error", "message": "Empty data file"})
        except pd.errors.ParserError:
            logger.error(f"CSV parsing error for file: {file_path}")
            results.append({"file": file_path, "status": "error", "message": "CSV parsing error"})
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            results.append({"file": file_path, "status": "error", "message": str(e)})
    
    logger.info(f"Prediction results: {results}")
    return results

# Define DAG configuration parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Initialize DAG object
dag = DAG(
    'prediction_job',
    default_args=default_args,
    description='Automated prediction job on files in good_data folder',
    schedule_interval=timedelta(minutes=5),  # Executes every 5 minutes
    catchup=False,                          # Does not backfill missed executions
)

# Define first task: check for new data files
check_data_task = PythonOperator(
    task_id='check_for_new_data',
    python_callable=check_for_new_data,
    provide_context=True,  # Passes Airflow context to the callable
    dag=dag,
)

# Define second task: process files and make predictions
predict_task = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    provide_context=True,  # Passes Airflow context to the callable
    dag=dag,
)

# Set task execution order
check_data_task >> predict_task