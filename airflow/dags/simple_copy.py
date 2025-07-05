# test_file_operations.py - Place this in your DAGs folder
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil

# Explicitly use the mounted path
RAW_DATA_FOLDER = "/opt/airflow/data_ingestion/raw_data"
GOOD_DATA_FOLDER = "/opt/airflow/data_ingestion/good_data"
TEST_OUTPUT_FILE = "/opt/airflow/data_ingestion/good_data/test_output.txt"

def list_and_copy_files():
    """List all files in raw_data and copy one to good_data"""
    # Create detailed log of what we're seeing
    with open(TEST_OUTPUT_FILE, 'w') as f:
        # Log current time
        f.write(f"Test run at: {datetime.now()}\n\n")
        
        # Log directory existence
        f.write(f"RAW_DATA_FOLDER exists: {os.path.exists(RAW_DATA_FOLDER)}\n")
        f.write(f"GOOD_DATA_FOLDER exists: {os.path.exists(GOOD_DATA_FOLDER)}\n\n")
        
        # Try to list files
        try:
            files = os.listdir(RAW_DATA_FOLDER)
            f.write(f"Files in RAW_DATA_FOLDER: {files}\n\n")
            
            # If files exist, try to copy the first one
            if files:
                src_file = os.path.join(RAW_DATA_FOLDER, files[0])
                dst_file = os.path.join(GOOD_DATA_FOLDER, f"copied_{files[0]}")
                
                f.write(f"Attempting to copy {src_file} to {dst_file}\n")
                shutil.copy(src_file, dst_file)
                f.write("Copy successful\n")
        except Exception as e:
            f.write(f"Error: {str(e)}\n")
    
    return "Completed file operations test"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'retries': 0,
}

dag = DAG(
    'test_file_operations',
    default_args=default_args,
    description='Test file operations between mounted directories',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

test_task = PythonOperator(
    task_id='list_and_copy_files',
    python_callable=list_and_copy_files,
    dag=dag,
)