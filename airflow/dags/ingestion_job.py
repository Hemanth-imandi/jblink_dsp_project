from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import shutil
import random
import pandas as pd
import numpy as np
import great_expectations as ge
import json
import requests
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration using environment variables - all from .env file
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_DIR = os.getenv("BASE_DIR", "/opt/airflow")  # Keep fallback for this one
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

# Validate required environment variables
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL is required but not found in environment variables")

# Construct paths using BASE_DIR
RAW_DATA_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "raw_data")
GOOD_DATA_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "good_data")
BAD_DATA_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "bad_data")
REPORT_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "reports")

# Create a logger instance 
logger = LoggingMixin().log

# Log configuration for debugging
logger.info(f"Configuration loaded:")
logger.info(f"DATABASE_URL: {DATABASE_URL}")
logger.info(f"BASE_DIR: {BASE_DIR}")
logger.info(f"TEAMS_WEBHOOK_URL: {'SET' if TEAMS_WEBHOOK_URL else 'NOT SET'}")
logger.info(f"RAW_DATA_FOLDER: {RAW_DATA_FOLDER}")

# Validate Teams webhook URL
if not TEAMS_WEBHOOK_URL:
    logger.warning("TEAMS_WEBHOOK_URL not found in environment variables. Teams notifications will be disabled.")

# Try to create directories with error handling
for directory in [RAW_DATA_FOLDER, GOOD_DATA_FOLDER, BAD_DATA_FOLDER, REPORT_FOLDER]:
    try:
        if not os.path.exists(directory):
            logger.info(f"Attempting to create directory: {directory}")
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Successfully created directory: {directory}")
        else:
            logger.info(f"Directory already exists: {directory}")
    except PermissionError as e:
        logger.warning(f"Permission denied when creating directory {directory}. This is expected if running in a container with mounted volumes. Error: {str(e)}")
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {str(e)}")

# Database connection setup
try:
    engine = create_engine(DATABASE_URL)
    Base = declarative_base()
    Session = sessionmaker(bind=engine)

    class DataQualityIssue(Base):
        __tablename__ = "data_quality_issues"

        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime, default=datetime.utcnow)
        filename = Column(String)
        total_rows = Column(Integer)
        valid_rows = Column(Integer)
        invalid_rows = Column(Integer)
        issue_type = Column(String)
        issue_count = Column(Integer)
        criticality = Column(String)

    Base.metadata.create_all(engine)
    logger.info("Successfully created database tables if they didn't exist")
except Exception as e:
    logger.error(f"Error setting up database: {str(e)}")

# Define the DAG using TaskFlow API
@dag(
    dag_id="data_ingestion_pipeline",
    description="Data ingestion pipeline with quality validation",
    schedule_interval="* * * * *",  # Every 1 minute as required
    start_date=datetime(2024, 2, 7),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
)
def data_ingestion_pipeline():
    
    @task(task_id="read_data")
    def read_data():
        """Read a random file from the raw data folder."""
        try:
            if not os.path.exists(RAW_DATA_FOLDER):
                logger.error(f"Raw data folder does not exist: {RAW_DATA_FOLDER}")
                return None

            files = [f for f in os.listdir(RAW_DATA_FOLDER) 
                    if os.path.isfile(os.path.join(RAW_DATA_FOLDER, f)) and f.endswith('.csv')]

            if not files:
                logger.info("No CSV files found in the raw-data folder")
                return None

            random_file = random.choice(files)
            file_path = os.path.join(RAW_DATA_FOLDER, random_file)

            logger.info(f"Selected file: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error in read_data: {str(e)}")
            return None

    @task(task_id="validate_data")
    def validate_data(file_path):
        """
        Validate data quality of the provided file using your actual dataset schema.
        Checks for the 7 error types you're generating in data_split.py
        """
        try:
            if not file_path:
                logger.warning("No file provided to validate_data")
                return None
                
            # Load the file
            df = pd.read_csv(file_path)
            
            # Define expected schema for your actual dataset (46 columns)
            required_columns = ['Customer ID', 'Tenure in Months', 'Monthly Charge', 'Total Customer Svc Requests']
            expected_offers = ['Offer A', 'Offer B', 'Offer C', 'Offer D', 'Offer E', 'None']
            expected_genders = ['Male', 'Female', 'M', 'F']
            
            # Initialize validation results
            row_validation = []
            data_issues = {
                "missing_columns": [],
                "missing_values": 0,
                "negative_values": 0,
                "string_in_numerical": 0,
                "outlier_values": 0,
                "duplicate_rows": 0,
                "unknown_categories": 0
            }
            
            # Set default criticality
            criticality = "low"
            
            # Check for missing required columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                data_issues["missing_columns"] = missing_columns
                criticality = "high"  # Missing columns is a high criticality issue
            
            # Check for duplicate rows
            duplicates = df.duplicated().sum()
            if duplicates > 0:
                data_issues["duplicate_rows"] = duplicates
            
            # For each row, check various data quality issues
            for idx, row in df.iterrows():
                row_issues = []
                
                # Check for missing required values
                for col in [c for c in required_columns if c in df.columns]:
                    if pd.isna(row[col]) or row[col] == '':
                        row_issues.append(f"missing_{col.replace(' ', '_').lower()}")
                        data_issues["missing_values"] += 1
                
                # Check for negative values in numerical columns
                numerical_columns = ['Tenure in Months', 'Monthly Charge', 'Total Customer Svc Requests', 'Age']
                for col in numerical_columns:
                    if col in df.columns and not pd.isna(row[col]):
                        try:
                            value = float(row[col])
                            if value < 0:
                                row_issues.append(f"negative_{col.replace(' ', '_').lower()}")
                                data_issues["negative_values"] += 1
                        except ValueError:
                            row_issues.append(f"string_in_{col.replace(' ', '_').lower()}")
                            data_issues["string_in_numerical"] += 1
                
                # Check for unknown categories in Offer column
                if 'Offer' in df.columns and not pd.isna(row['Offer']):
                    if row['Offer'] not in expected_offers:
                        row_issues.append("unknown_offer")
                        data_issues["unknown_categories"] += 1
                
                # Check for unknown categories in Gender column
                if 'Gender' in df.columns and not pd.isna(row['Gender']):
                    if row['Gender'] not in expected_genders:
                        row_issues.append("unknown_gender")
                        data_issues["unknown_categories"] += 1
                
                # Check for outlier values
                if 'Total Customer Svc Requests' in df.columns and not pd.isna(row['Total Customer Svc Requests']):
                    try:
                        requests_count = float(row['Total Customer Svc Requests'])
                        if requests_count > 20:  # Outlier threshold for service requests
                            row_issues.append("outlier_service_requests")
                            data_issues["outlier_values"] += 1
                    except ValueError:
                        pass  # Already caught as string_in_numerical
                
                # Check for age outliers
                if 'Age' in df.columns and not pd.isna(row['Age']):
                    try:
                        age = float(row['Age'])
                        if age < 0 or age > 120:  # Invalid age range
                            row_issues.append("invalid_age")
                            data_issues["outlier_values"] += 1
                    except ValueError:
                        pass  # Already caught as string_in_numerical
                
                # Add row validation result
                row_validation.append({
                    "row_index": idx,
                    "valid": len(row_issues) == 0,
                    "issues": row_issues
                })
            
            # Determine overall criticality based on issues found
            total_issues = sum(
                count for key, count in data_issues.items() 
                if key != "missing_columns" and isinstance(count, int)
            )
            
            # If more than 30% of rows have issues, mark as high criticality
            if total_issues > len(df) * 0.3:
                criticality = "high"
            # If more than 10% of rows have issues, mark as medium criticality
            elif total_issues > len(df) * 0.1:
                criticality = "medium"
            
            # Calculate valid and invalid rows
            valid_rows = sum(1 for r in row_validation if r["valid"])
            invalid_rows = len(row_validation) - valid_rows
            
            return {
                "filename": os.path.basename(file_path),
                "file_path": file_path,
                "total_rows": len(df),
                "valid_rows": valid_rows,
                "invalid_rows": invalid_rows,
                "row_validation": row_validation,
                "data_issues": data_issues,
                "criticality": criticality
            }
        except Exception as e:
            logger.error(f"Error in validate_data: {str(e)}")
            return None

    @task(task_id="save_statistics")
    def save_statistics(validation_results):
        """Save validation statistics to database."""
        if not validation_results:
            logger.info("No validation results to save")
            return None
        try:
            session = Session()
            
            # Process each issue type
            if validation_results.get("data_issues"):
                issues = validation_results["data_issues"]
                
                # Missing columns
                if issues.get("missing_columns") and len(issues["missing_columns"]) > 0:
                    record = DataQualityIssue(
                        filename=validation_results["filename"],
                        total_rows=validation_results["total_rows"],
                        valid_rows=validation_results["valid_rows"],
                        invalid_rows=validation_results["invalid_rows"],
                        issue_type="missing_columns",
                        issue_count=len(issues["missing_columns"]),
                        criticality="high"
                    )
                    session.add(record)
                
                # Process other issue types
                issue_criticality_map = {
                    "missing_values": "medium",
                    "negative_values": "medium",
                    "string_in_numerical": "high",
                    "outlier_values": "medium",
                    "duplicate_rows": "low",
                    "unknown_categories": "medium"
                }
                
                for issue_type, count in issues.items():
                    if issue_type != "missing_columns" and isinstance(count, int) and count > 0:
                        # Determine criticality based on percentage of affected rows
                        percentage = count / validation_results["total_rows"] * 100
                        if percentage > 30:
                            criticality = "high"
                        elif percentage > 10:
                            criticality = "medium"
                        else:
                            criticality = issue_criticality_map.get(issue_type, "low")
                            
                        record = DataQualityIssue(
                            filename=validation_results["filename"],
                            total_rows=validation_results["total_rows"],
                            valid_rows=validation_results["valid_rows"],
                            invalid_rows=validation_results["invalid_rows"],
                            issue_type=issue_type,
                            issue_count=count,
                            criticality=criticality
                        )
                        session.add(record)
            
            session.commit()
            logger.info(f"Successfully saved statistics for {validation_results['filename']}")
            return "Statistics saved successfully"
        except Exception as e:
            logger.error(f"Error saving statistics: {str(e)}")
            session.rollback()
            return f"Error saving statistics: {str(e)}"
        finally:
            session.close()

    @task(task_id="send_alerts")
    def send_alerts(validation_results):
        """Send alerts for data quality issues via Teams webhook."""
        if not validation_results:
            return "No results"
        
        # Check if Teams webhook is configured
        if not TEAMS_WEBHOOK_URL:
            logger.warning("Teams webhook URL not configured. Skipping Teams notification.")
            return "Teams webhook not configured"
        
        try:
            filename = validation_results["filename"]
            criticality = validation_results.get("criticality", "low")
            
            # Create a summary of the errors
            issues_summary = []
            if validation_results.get("data_issues"):
                issues = validation_results["data_issues"]
                
                if issues.get("missing_columns") and len(issues["missing_columns"]) > 0:
                    issues_summary.append(f"Missing columns: {', '.join(issues['missing_columns'])}")
                
                for issue_type, count in issues.items():
                    if issue_type != "missing_columns" and isinstance(count, int) and count > 0:
                        issues_summary.append(f"{issue_type.replace('_', ' ').title()}: {count}")
            
            # Generate report
            report_path = os.path.join(REPORT_FOLDER, f"report_{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            
            # Create detailed HTML report
            try:
                with open(report_path, 'w') as f:
                    f.write(f"""
                    <html>
                    <head><title>Data Quality Report: {filename}</title></head>
                    <body>
                        <h1>Data Quality Report: {filename}</h1>
                        <h2>Summary</h2>
                        <p><strong>Criticality:</strong> {criticality.upper()}</p>
                        <p><strong>Total Rows:</strong> {validation_results['total_rows']}</p>
                        <p><strong>Valid Rows:</strong> {validation_results['valid_rows']}</p>
                        <p><strong>Invalid Rows:</strong> {validation_results['invalid_rows']}</p>
                        <p><strong>Data Quality Score:</strong> {(validation_results['valid_rows']/validation_results['total_rows']*100):.1f}%</p>
                        
                        <h2>Issues Detected</h2>
                        <ul>
                    """)
                    
                    if issues_summary:
                        for issue in issues_summary:
                            f.write(f"<li>{issue}</li>")
                    else:
                        f.write("<li>No issues detected - clean data file</li>")
                        
                    f.write("""
                        </ul>
                        <p><em>Report generated on {}</em></p>
                    </body>
                    </html>
                    """.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            except Exception as e:
                logger.error(f"Error creating HTML report: {str(e)}")
                report_path = "Report creation failed"
            
            # Prepare Teams message
            if issues_summary:
                issues_text = "\n".join([f"• {issue}" for issue in issues_summary])
            else:
                issues_text = "• No data quality issues detected"
            
            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "FF6D00" if criticality == "high" else "FFA500" if criticality == "medium" else "00FF00",
                "summary": f"Data Quality Alert - {filename}",
                "sections": [{
                    "activityTitle": f"🔍 Data Quality Alert - {criticality.upper()} Priority",
                    "activitySubtitle": f"File: {filename}",
                    "facts": [
                        {"name": "Criticality", "value": criticality.upper()},
                        {"name": "Total Rows", "value": str(validation_results['total_rows'])},
                        {"name": "Valid Rows", "value": str(validation_results['valid_rows'])},
                        {"name": "Invalid Rows", "value": str(validation_results['invalid_rows'])},
                        {"name": "Data Quality Score", "value": f"{(validation_results['valid_rows']/validation_results['total_rows']*100):.1f}%"}
                    ],
                    "text": f"**Issues Summary:**\n{issues_text}"
                }],
                "potentialAction": [{
                    "@type": "OpenUri",
                    "name": "View Detailed Report", 
                    "targets": [{"os": "default", "uri": f"file://{report_path}"}]
                }]
            }
            
            # Send Teams notification
            try:
                response = requests.post(TEAMS_WEBHOOK_URL, json=message, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Successfully sent Teams alert for file: {filename}")
                    return "Teams alert sent successfully"
                else:
                    logger.warning(f"Teams webhook returned status {response.status_code}: {response.text}")
                    return f"Teams webhook error: {response.status_code}"
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to send Teams notification: {str(e)}")
                return f"Teams notification failed: {str(e)}"
            
        except Exception as e:
            logger.error(f"Error sending alert: {str(e)}")
            return f"Alert error: {str(e)}"

    @task(task_id="split_and_save_data")
    def split_and_save_data(validation_results):
        """Split data into good and bad based on validation results."""
        if not validation_results or "file_path" not in validation_results:
            logger.info("Missing validation results or file path")
            return "No file to process"
        try:
            file_path = validation_results["file_path"]
            filename = os.path.basename(file_path)

            # Ensure output folders exist
            for folder in [GOOD_DATA_FOLDER, BAD_DATA_FOLDER]:
                if not os.path.exists(folder):
                    try:
                        os.makedirs(folder, exist_ok=True)
                        logger.info(f"Created folder: {folder}")
                    except Exception as e:
                        logger.error(f"Cannot create folder {folder}: {str(e)}")
                        return f"Error: Could not create folder {folder}"

            # Process based on data quality results
            if validation_results["invalid_rows"] == 0:
                # All data is good
                destination_path = os.path.join(GOOD_DATA_FOLDER, filename)
                shutil.copy(file_path, destination_path)
                os.remove(file_path)
                logger.info(f"File {filename} moved to good_data folder")
                return "File moved to good_data folder"
                
            elif validation_results["valid_rows"] == 0:
                # All data is bad
                destination_path = os.path.join(BAD_DATA_FOLDER, filename)
                shutil.copy(file_path, destination_path)
                os.remove(file_path)
                logger.info(f"File {filename} moved to bad_data folder")
                return "File moved to bad_data folder"
                
            else:
                # Mixed data - split into good and bad
                df = pd.read_csv(file_path)
                valid_indices = [r["row_index"] for r in validation_results["row_validation"] if r["valid"]]
                invalid_indices = [r["row_index"] for r in validation_results["row_validation"] if not r["valid"]]
                
                # Save good data
                if valid_indices:
                    good_df = df.iloc[valid_indices]
                    good_path = os.path.join(GOOD_DATA_FOLDER, filename)
                    good_df.to_csv(good_path, index=False)
                    logger.info(f"Saved {len(valid_indices)} good rows to {good_path}")
                
                # Save bad data
                if invalid_indices:
                    bad_df = df.iloc[invalid_indices]
                    bad_path = os.path.join(BAD_DATA_FOLDER, filename)
                    bad_df.to_csv(bad_path, index=False)
                    logger.info(f"Saved {len(invalid_indices)} bad rows to {bad_path}")
                
                # Remove original file
                os.remove(file_path)
                return f"File split: {len(valid_indices)} good rows, {len(invalid_indices)} bad rows"
                
        except Exception as e:
            logger.error(f"Error in split_and_save_data: {str(e)}")
            return f"Error splitting data: {str(e)}"

    # Define the task flow
    file_path = read_data()
    validation_results = validate_data(file_path)

    # These three tasks run in parallel as required
    save_statistics_task = save_statistics(validation_results)
    send_alerts_task = send_alerts(validation_results)
    split_save_task = split_and_save_data(validation_results)
    
    # Define explicit dependencies
    file_path >> validation_results
    validation_results >> [save_statistics_task, send_alerts_task, split_save_task]

# Create the DAG instance
data_ingestion_dag = data_ingestion_pipeline()