from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import random

# Directory paths
RAW_DATA_FOLDER = "./data_ingestion/raw_data"
GOOD_DATA_FOLDER = "./data_ingestion/good_data"

# Ensure directories exist
for directory in [RAW_DATA_FOLDER, GOOD_DATA_FOLDER]:
    os.makedirs(directory, exist_ok=True)

def read_data():

    # Get list of files in the raw data folder
    files = [f for f in os.listdir(RAW_DATA_FOLDER) 
             if os.path.isfile(os.path.join(RAW_DATA_FOLDER, f))]
    
    if not files:
        return None
    
    # Select a random file
    random_file = random.choice(files)
    file_path = os.path.join(RAW_DATA_FOLDER, random_file)
    
    return file_path

def save_file(file_path):
   
    if file_path is None:
        return 
    
    # Get the filename without the path
    filename = os.path.basename(file_path)
    destination_path = os.path.join(GOOD_DATA_FOLDER, filename)
    
    # Move the file
    shutil.move(file_path, destination_path)
    return 

# Set up the Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "data_ingestion",
    default_args=default_args,
    schedule_interval="@daily",  # Run once per day
    catchup=False,
    description=False
)

read_task = PythonOperator(
    task_id="read_data",
    python_callable=read_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_file",
    python_callable=save_file,
    op_kwargs={"file_path": "{{ task_instance.xcom_pull(task_ids='read_data') }}"},
    dag=dag,
)

# Set task dependencies
read_task >> save_task