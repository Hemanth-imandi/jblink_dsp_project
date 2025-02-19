from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import json



# API URL
API_URL = "http://127.0.0.1:8000/predict"

# Function to check for new files
def check_for_new_data():
    """Simulate checking for new files in the good_data folder"""
    data_dir = "airflow/data_ingestion/raw_data"
    files = os.listdir(data_dir)
    return [os.path.join(data_dir, file) for file in files if file.endswith(".csv")]

# Function to make batch predictions
def make_predictions():
    """Reads new data files and makes predictions via FastAPI"""
    new_files = check_for_new_data()

    if not new_files:
        print("No new files found. Skipping prediction.")
        return
    
    for file in new_files:
        print(f"Processing file: {file}")
        df = pd.read_csv(file)

        # Convert DataFrame to JSON for API request
        data_records = df.to_dict(orient="records")
        payload = {"customers": data_records}

        # Send API request
        response = requests.post(API_URL, json=payload)

        if response.status_code == 200:
            print(f"Predictions for {file}: {json.dumps(response.json(), indent=2)}")
        else:
            print(f"Error making predictions for {file}: {response.text}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'prediction_job',
    default_args=default_args,
    description='Automated prediction job',
    schedule_interval=timedelta(minutes=2),  # Runs every 2 minutes
    catchup=False,
)

# Task 1: Check for new data
check_data_task = PythonOperator(
    task_id='check_for_new_data',
    python_callable=check_for_new_data,
    dag=dag,
)

# Task 2: Make Predictions
predict_task = PythonOperator(
    task_id='make_predictions',
    python_callable=make_predictions,
    dag=dag,
)

# Define task dependencies
check_data_task >> predict_task
