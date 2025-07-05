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

# Configuration using environment variables from .env file
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_DIR = os.getenv("BASE_DIR", "/opt/airflow")
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")
GE_ROOT_DIR = os.getenv("GE_ROOT_DIR", "/opt/airflow/great_expectations")

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
logger.info(f"GE_ROOT_DIR: {GE_ROOT_DIR}")

# Validate Teams webhook URL
if not TEAMS_WEBHOOK_URL:
    logger.warning("TEAMS_WEBHOOK_URL not found in environment variables. Teams notifications will be disabled.")

# Try to create directories with error handling
for directory in [RAW_DATA_FOLDER, GOOD_DATA_FOLDER, BAD_DATA_FOLDER, REPORT_FOLDER, GE_ROOT_DIR]:
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

def setup_great_expectations_context():
    """Initialize or get Great Expectations context"""
    try:
        # For GE 1.4.2, create context with simplified approach
        context = ge.get_context(context_root_dir=GE_ROOT_DIR)
        logger.info("Using Great Expectations context")
        return context
    except Exception as e:
        logger.info(f"Creating new Great Expectations context: {str(e)}")
        context = ge.get_context(context_root_dir=GE_ROOT_DIR)
        return context

# Define the DAG using TaskFlow API
@dag(
    dag_id="data_ingestion_pipeline",
    description="Data ingestion pipeline with Great Expectations validation",
    schedule_interval="* * * * *",  # Every 1 minute 
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
        Validate data quality using Great Expectations.
        Checks for the 7 error types generated in data_split.py
        """
        try:
            if not file_path:
                logger.warning("No file provided to validate_data")
                return None
            
            # Initialize Great Expectations context
            context = setup_great_expectations_context()
            
            # Load the data
            df = pd.read_csv(file_path)
            
            # Create a simple expectation suite using GE 1.4.2 API
            suite_name = "data_quality_suite"
            
            # Use validator approach for GE 1.4.2
            batch_request = {"data": df, "batch_identifiers": {"default_identifier_name": "validation_batch"}}
            
            try:
                # Create validator with expectations
                validator = context.get_validator(
                    batch_request=batch_request,
                    create_expectation_suite_with_name=suite_name
                )
                
                # Add expectations using the validator
                expectations_added = 0
                
                # 1. Required columns should exist
                try:
                    validator.expect_table_columns_to_match_set(
                        column_set=["Customer ID", "Tenure in Months", "Monthly Charge", "Total Customer Svc Requests"]
                    )
                    expectations_added += 1
                except:
                    pass
                
                # 2. Missing values in required columns
                for col in ["Customer ID", "Tenure in Months", "Monthly Charge", "Total Customer Svc Requests"]:
                    if col in df.columns:
                        try:
                            validator.expect_column_values_to_not_be_null(column=col)
                            expectations_added += 1
                        except:
                            pass
                
                # 3. Negative values (should be positive)
                for col in ["Tenure in Months", "Monthly Charge", "Total Customer Svc Requests"]:
                    if col in df.columns:
                        try:
                            validator.expect_column_values_to_be_between(column=col, min_value=0, max_value=None)
                            expectations_added += 1
                        except:
                            pass
                
                # 4. Outlier values
                if "Total Customer Svc Requests" in df.columns:
                    try:
                        validator.expect_column_values_to_be_between(column="Total Customer Svc Requests", min_value=0, max_value=20)
                        expectations_added += 1
                    except:
                        pass
                
                # 5. Unknown categorical values
                if "Offer" in df.columns:
                    try:
                        validator.expect_column_values_to_be_in_set(
                            column="Offer", 
                            value_set=["Offer A", "Offer B", "Offer C", "Offer D", "Offer E", "None"]
                        )
                        expectations_added += 1
                    except:
                        pass
                
                if "Gender" in df.columns:
                    try:
                        validator.expect_column_values_to_be_in_set(
                            column="Gender", 
                            value_set=["Male", "Female", "M", "F"]
                        )
                        expectations_added += 1
                    except:
                        pass
                
                logger.info(f"Added {expectations_added} expectations")
                
                # Run validation
                validation_results = validator.validate()
                
                # Build Data Docs
                try:
                    context.build_data_docs()
                    logger.info("Successfully built Great Expectations Data Docs")
                except Exception as e:
                    logger.warning(f"Could not build Data Docs: {str(e)}")
                
                # Extract validation statistics
                total_expectations = len(validation_results.results)
                successful_expectations = sum(1 for result in validation_results.results if result.success)
                failed_expectations = total_expectations - successful_expectations
                
                # Calculate row-level validation results
                total_rows = len(df)
                # Estimate valid rows based on expectation success rate
                validation_success_rate = successful_expectations / total_expectations if total_expectations > 0 else 1
                valid_rows = int(total_rows * validation_success_rate)
                invalid_rows = total_rows - valid_rows
                
                # Determine criticality
                if failed_expectations > total_expectations * 0.5:
                    criticality = "high"
                elif failed_expectations > total_expectations * 0.2:
                    criticality = "medium"
                else:
                    criticality = "low"
                
                # Extract detailed issues
                data_issues = {}
                for result in validation_results.results:
                    if not result.success:
                        expectation_type = result.expectation_config.expectation_type
                        if expectation_type not in data_issues:
                            data_issues[expectation_type] = 0
                        data_issues[expectation_type] += 1
                
                return {
                    "filename": os.path.basename(file_path),
                    "file_path": file_path,
                    "total_rows": total_rows,
                    "valid_rows": valid_rows,
                    "invalid_rows": invalid_rows,
                    "validation_results": validation_results,
                    "data_issues": data_issues,
                    "criticality": criticality,
                    "ge_context": context,
                    "total_expectations": total_expectations,
                    "successful_expectations": successful_expectations,
                    "failed_expectations": failed_expectations
                }
                
            except Exception as e:
                logger.error(f"Error creating validator: {str(e)}")
                return None
            
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
            
            # Save overall validation statistics
            record = DataQualityIssue(
                filename=validation_results["filename"],
                total_rows=validation_results["total_rows"],
                valid_rows=validation_results["valid_rows"],
                invalid_rows=validation_results["invalid_rows"],
                issue_type="overall_validation",
                issue_count=validation_results["failed_expectations"],
                criticality=validation_results["criticality"]
            )
            session.add(record)
            
            # Process specific issue types from Great Expectations results
            if validation_results.get("data_issues"):
                for issue_type, count in validation_results["data_issues"].items():
                    # Map GE expectation types to readable names
                    issue_name_mapping = {
                        "expect_table_columns_to_match_set": "missing_columns",
                        "expect_column_values_to_not_be_null": "missing_values",
                        "expect_column_values_to_be_between": "value_range_issues",
                        "expect_column_values_to_be_of_type": "type_errors",
                        "expect_table_row_count_to_equal_other_table": "duplicate_rows",
                        "expect_column_values_to_be_in_set": "invalid_categories"
                    }
                    
                    readable_name = issue_name_mapping.get(issue_type, issue_type)
                    
                    record = DataQualityIssue(
                        filename=validation_results["filename"],
                        total_rows=validation_results["total_rows"],
                        valid_rows=validation_results["valid_rows"],
                        invalid_rows=validation_results["invalid_rows"],
                        issue_type=readable_name,
                        issue_count=count,
                        criticality=validation_results["criticality"]
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
        """Send alerts for data quality issues via Teams webhook using Great Expectations Data Docs."""
        if not validation_results:
            return "No results"
        
        # Check if Teams webhook is configured
        if not TEAMS_WEBHOOK_URL:
            logger.warning("Teams webhook URL not configured. Skipping Teams notification.")
            return "Teams webhook not configured"
        
        try:
            filename = validation_results["filename"]
            criticality = validation_results.get("criticality", "low")
            
            # Create a summary of the errors from Great Expectations results
            issues_summary = []
            if validation_results.get("data_issues"):
                for issue_type, count in validation_results["data_issues"].items():
                    readable_name = issue_type.replace("expect_", "").replace("_", " ").title()
                    issues_summary.append(f"{readable_name}: {count} failed checks")
            
            if not issues_summary:
                issues_summary.append("No data quality issues detected - all expectations passed")
            
            # Prepare Teams message
            issues_text = "\n".join([f"• {issue}" for issue in issues_summary])
            
            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "FF6D00" if criticality == "high" else "FFA500" if criticality == "medium" else "00FF00",
                "summary": f"Data Quality Alert - {filename}",
                "sections": [{
                    "activityTitle": f"🔍 Great Expectations Data Quality Alert - {criticality.upper()} Priority",
                    "activitySubtitle": f"File: {filename}",
                    "facts": [
                        {"name": "Criticality", "value": criticality.upper()},
                        {"name": "Total Rows", "value": str(validation_results['total_rows'])},
                        {"name": "Valid Rows", "value": str(validation_results['valid_rows'])},
                        {"name": "Invalid Rows", "value": str(validation_results['invalid_rows'])},
                        {"name": "Expectations Tested", "value": str(validation_results['total_expectations'])},
                        {"name": "Expectations Passed", "value": str(validation_results['successful_expectations'])},
                        {"name": "Expectations Failed", "value": str(validation_results['failed_expectations'])},
                        {"name": "Data Quality Score", "value": f"{(validation_results['valid_rows']/validation_results['total_rows']*100):.1f}%"}
                    ],
                    "text": f"**Great Expectations Validation Summary:**\n{issues_text}"
                }],
                "potentialAction": [{
                    "@type": "OpenUri",
                    "name": "View Great Expectations Data Docs", 
                    "targets": [{"os": "default", "uri": "Great Expectations Data Docs available in container"}]
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
        """Split data into good and bad based on Great Expectations validation results."""
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

            # Determine data quality based on Great Expectations results
            success_rate = validation_results["successful_expectations"] / validation_results["total_expectations"]
            
            if success_rate >= 0.9:  # 90% or more expectations passed
                # Data is considered good
                destination_path = os.path.join(GOOD_DATA_FOLDER, filename)
                shutil.copy(file_path, destination_path)
                os.remove(file_path)
                logger.info(f"File {filename} moved to good_data folder (success rate: {success_rate:.2f})")
                return f"File moved to good_data folder (success rate: {success_rate:.2f})"
                
            elif success_rate <= 0.5:  # 50% or fewer expectations passed
                # Data is considered bad
                destination_path = os.path.join(BAD_DATA_FOLDER, filename)
                shutil.copy(file_path, destination_path)
                os.remove(file_path)
                logger.info(f"File {filename} moved to bad_data folder (success rate: {success_rate:.2f})")
                return f"File moved to bad_data folder (success rate: {success_rate:.2f})"
                
            else:
                # Mixed quality - copy to good_data with quality warnings
                destination_path = os.path.join(GOOD_DATA_FOLDER, filename)
                shutil.copy(file_path, destination_path)
                os.remove(file_path)
                logger.info(f"File {filename} moved to good_data folder with quality issues (success rate: {success_rate:.2f})")
                return f"File moved to good_data folder with quality issues (success rate: {success_rate:.2f})"
                
        except Exception as e:
            logger.error(f"Error in split_and_save_data: {str(e)}")
            return f"Error splitting data: {str(e)}"

    # Define the task flow
    file_path = read_data()
    validation_results = validate_data(file_path)

    # These three tasks run in parallel
    save_statistics_task = save_statistics(validation_results)
    send_alerts_task = send_alerts(validation_results)
    split_save_task = split_and_save_data(validation_results)
    
    # Define explicit dependencies
    file_path >> validation_results
    validation_results >> [save_statistics_task, send_alerts_task, split_save_task]

# Create the DAG instance
data_ingestion_dag = data_ingestion_pipeline()