from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import os
import shutil
import random

# Define directory paths for Docker environment
RAW_DATA_FOLDER = "/opt/airflow/data_ingestion/raw_data"
GOOD_DATA_FOLDER = "/opt/airflow/data_ingestion/good_data"

# Ensure required directories exist before execution
for directory in [RAW_DATA_FOLDER, GOOD_DATA_FOLDER]:
    os.makedirs(directory, exist_ok=True)

# Initialize logger for consistent logging across tasks
logger = LoggingMixin().log

def read_data(**context):
    """
    Selects a random file from the raw data directory.
    
    """
    logger.info(f"Starting read_data function")
    logger.info(f"Raw data folder: {RAW_DATA_FOLDER}")
    
    try:
        # Inventory available files in source directory
        all_files = os.listdir(RAW_DATA_FOLDER)
        logger.info(f"Files in raw data folder: {len(all_files)} files found")
        
        # Filter for actual files (exclude directories and other items)
        files = [f for f in os.listdir(RAW_DATA_FOLDER) 
                if os.path.isfile(os.path.join(RAW_DATA_FOLDER, f))]
        
        if not files:
            logger.warning("No files found in the raw-data folder")
            return None
        
        # Select random file for processing
        random_file = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, random_file)
        
        # Verify file existence before returning
        if os.path.exists(file_path):
            logger.info(f"Selected file: {file_path}")
            return file_path
        else:
            logger.error(f"Selected file doesn't exist: {file_path}")
            return None
    
    except Exception as e:
        logger.error(f"Error in read_data: {str(e)}")
        return None

def save_file(**context):
    """
    Moves the selected file from raw data to good data directory.
    """
    # Retrieve task instance from context
    ti = context['ti']
    # Get file path from previous task using XCom
    file_path = ti.xcom_pull(task_ids='read_data')
    
    logger.info(f"Starting save_file function with file_path: {file_path}")
    logger.info(f"Good data folder: {GOOD_DATA_FOLDER}")

    try:
        if file_path is None:
            logger.warning("No file path provided")
            return "No file to process"
        
        # Validate source file still exists
        if not os.path.exists(file_path):
            logger.error(f"Source file doesn't exist: {file_path}")
            return f"Error: Source file not found"
        
        # Extract filename for destination path
        filename = os.path.basename(file_path)
        destination_path = os.path.join(GOOD_DATA_FOLDER, filename)
        
        # Verify write permissions on destination directory
        if not os.access(os.path.dirname(destination_path), os.W_OK):
            logger.error(f"No write permission for: {os.path.dirname(destination_path)}")
            return f"Error: No write permission for destination directory"
        
        # Execute file transfer
        shutil.move(file_path, destination_path)
        
        # Confirm successful transfer
        if os.path.exists(destination_path):
            logger.info(f"Moved file from {file_path} to {destination_path}")
            return f"File moved to {destination_path}"
        else:
            logger.error(f"Failed to move file to {destination_path}")
            return f"Error: Failed to move file" 
        
    except PermissionError as pe:
        logger.error(f"Permission error: {str(pe)}")
        return f"Error: Permission denied - {str(pe)}"
    except Exception as e:
        logger.error(f"Error in save_file: {str(e)}")
        return f"Error: {str(e)}"

# Define DAG configuration parameters
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,  # Prevents scheduling dependencies on previous runs
}

# Initialize DAG object
dag = DAG(
    "data_ingestion",
    default_args=default_args,
    schedule_interval="@daily",  # Executes once per day
    catchup=False,              # Does not backfill missed executions
    description="Ingestion pipeline to move files from raw-data to good-data"
)

# Define first task: read a file from raw data folder
read_task = PythonOperator(
    task_id="read_data",
    python_callable=read_data,
    provide_context=True,  # Passes Airflow context to the callable
    dag=dag,
)

# Define second task: save the file to good data folder
save_task = PythonOperator(
    task_id="save_file",
    python_callable=save_file,
    provide_context=True,  # Passes Airflow context to the callable
    dag=dag,
)

# Set task execution order
read_task >> save_task