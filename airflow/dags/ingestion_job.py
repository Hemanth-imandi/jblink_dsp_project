from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import os
import shutil
import random
import pandas as pd
import great_expectations as gx
import requests
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_DIR = os.getenv("BASE_DIR", "/opt/airflow")
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")
GE_ROOT_DIR = os.getenv("GE_ROOT_DIR", "/opt/airflow/great_expectations")

if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL is required but not found in environment variables")

RAW_DATA_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "raw_data")
GOOD_DATA_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "good_data")
BAD_DATA_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "bad_data")
REPORT_FOLDER = os.path.join(BASE_DIR, "data_ingestion", "reports")

logger = LoggingMixin().log

for directory in [RAW_DATA_FOLDER, GOOD_DATA_FOLDER, BAD_DATA_FOLDER, REPORT_FOLDER, GE_ROOT_DIR]:
    os.makedirs(directory, exist_ok=True)

# SQLAlchemy setup
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

def setup_great_expectations_context():
    """Setup GX context with proper configuration for v1.4.2"""
    try:
        # Try to get existing context
        context = gx.get_context(project_root_dir=GE_ROOT_DIR)
        return context
    except Exception as e:
        logger.info(f"Creating new GX context: {e}")
        # Create new context if it doesn't exist
        context = gx.get_context(project_root_dir=GE_ROOT_DIR, mode="file")
        return context

@dag(
    dag_id="data_ingestion_pipeline",
    description="Data ingestion pipeline with Great Expectations validation",
    schedule_interval="* * * * *",  # Back to every 1 minute as per specification
    start_date=datetime(2024, 2, 7),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
)
def data_ingestion_pipeline():

    @task
    def read_data():
        logger.info(f"RAW_DATA_FOLDER path: {RAW_DATA_FOLDER}")
        
        if not os.path.exists(RAW_DATA_FOLDER):
            logger.error(f"Raw data folder does not exist: {RAW_DATA_FOLDER}")
            return None

        # CRITICAL: Only read files that start with "chunk_" to prevent infinite loop
        all_files = os.listdir(RAW_DATA_FOLDER)
        files = [f for f in all_files if f.endswith(".csv") and f.startswith("chunk_")]
        
        logger.info(f"Total files in raw_data: {len(all_files)}")
        logger.info(f"Valid chunk_ files: {len(files)}")
        
        if not files:
            logger.info("No valid chunk_*.csv files found in raw_data folder.")
            return None

        # Log first few files for debugging
        logger.info(f"Sample valid files: {files[:5]}")
        
        selected_file = random.choice(files)
        file_path = os.path.join(RAW_DATA_FOLDER, selected_file)
        logger.info(f"Selected file: {selected_file}")
        logger.info(f"Full file path: {file_path}")
        
        # Verify the file exists and is accessible
        if not os.path.exists(file_path):
            logger.error(f"Selected file does not exist: {file_path}")
            return None
            
        return file_path

    @task
    def validate_data(file_path):
        if not file_path:
            logger.warning("validate_data(): No file path provided")
            return None

        try:
            context = setup_great_expectations_context()
            df = pd.read_csv(file_path)

            # Create or get datasource (GX 1.x API)
            datasource_name = "pandas_datasource"
            try:
                datasource = context.get_datasource(datasource_name)
            except Exception:
                logger.info(f"Creating new datasource: {datasource_name}")
                datasource = context.data_sources.add_pandas(datasource_name)

            # Create or get data asset and batch definition (GX 1.x API)
            asset_name = "csv_data_asset"
            try:
                asset = datasource.get_asset(asset_name)
            except Exception:
                logger.info(f"Creating new asset: {asset_name}")
                asset = datasource.add_dataframe_asset(asset_name)

            # For GX 1.4.2, use the batch definition approach
            batch_def_name = "batch_def"
            try:
                batch_definition = asset.get_batch_definition(batch_def_name)
            except Exception:
                logger.info(f"Creating new batch definition: {batch_def_name}")
                batch_definition = asset.add_batch_definition_whole_dataframe(batch_def_name)
            
            batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

            # Create or get expectation suite
            suite_name = "data_quality_suite"
            try:
                suite = context.suites.get(suite_name)
                logger.info(f"Loaded existing expectation suite: {suite_name}")
            except Exception:
                logger.info(f"Creating new expectation suite: {suite_name}")
                suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

            # Create validator with batch
            validator = context.get_validator(
                batch=batch,
                expectation_suite=suite
            )

            # Add expectations if this is a new suite - Target the 7 specific data quality issues
            if len(suite.expectations) == 0:
                logger.info("Adding expectations for the 7 specific data quality issues")
                
                # Issue 1: Missing column - Check for required columns
                validator.expect_table_columns_to_match_set(
                    column_set=["Customer ID", "Tenure in Months", "Monthly Charge", "Total Customer Svc Requests", 
                               "Offer", "Avg Monthly Long Distance Charges", "Gender"]
                )
                
                # Issue 2: Missing values in Tenure in Months
                validator.expect_column_values_to_not_be_null(column="Tenure in Months")
                
                # Issue 3: Outlier values in Total Customer Svc Requests
                validator.expect_column_values_to_be_between(
                    column="Total Customer Svc Requests", 
                    min_value=0, 
                    max_value=50  # Reasonable upper limit
                )
                
                # Issue 4: Unknown categories in Offer
                validator.expect_column_values_to_be_in_set(
                    column="Offer", 
                    value_set=["Offer A", "Offer B", "Offer C", "Offer D", "Offer E", "None"]
                )
                
                # Issue 5: Wrong/negative values in Tenure in Months
                validator.expect_column_values_to_be_between(
                    column="Tenure in Months", 
                    min_value=0, 
                    max_value=100  # Reasonable tenure limit
                )
                
                # Issue 6: String in numerical column - Check data types
                validator.expect_column_values_to_be_of_type(
                    column="Avg Monthly Long Distance Charges", 
                    type_="float"
                )
                
                # Issue 7: Duplicate rows - Check for unique Customer IDs
                validator.expect_column_values_to_be_unique(column="Customer ID")

                # Save the expectation suite
                context.suites.add_or_update(suite)

            # Run validation
            results = validator.validate()
            
            # Build data docs
            context.build_data_docs()

            # Calculate validation metrics
            total_expectations = len(results.results)
            successful = sum(1 for r in results.results if r.success)
            failed = total_expectations - successful
            total_rows = len(df)
            valid_rows = int(total_rows * (successful / total_expectations)) if total_expectations else total_rows
            invalid_rows = total_rows - valid_rows

            criticality = (
                "high" if failed > total_expectations * 0.5
                else "medium" if failed > total_expectations * 0.2
                else "low"
            )

            issues = {}
            for r in results.results:
                if not r.success:
                    # In GX 1.x, expectation_type became 'type'
                    key = r.expectation_config.type
                    issues[key] = issues.get(key, 0) + 1

            logger.info(f"Validation complete for {file_path} — {successful}/{total_expectations} expectations passed")

            return {
                "filename": os.path.basename(file_path),
                "file_path": file_path,
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "invalid_rows": invalid_rows,
                "criticality": criticality,
                "data_issues": issues,
                "successful_expectations": successful,
                "failed_expectations": failed,
                "total_expectations": total_expectations
            }

        except Exception as e:
            logger.error(f"validate_data() failed: {str(e)}")
            logger.warning("validate_data() returning None due to error")
            return None

    @task
    def save_statistics(results):
        if not results:
            logger.warning("save_statistics() skipped — no results")
            return
        try:
            session = Session()
            session.add(DataQualityIssue(
                filename=results["filename"],
                total_rows=results["total_rows"],
                valid_rows=results["valid_rows"],
                invalid_rows=results["invalid_rows"],
                issue_type="overall_validation",
                issue_count=results["failed_expectations"],
                criticality=results["criticality"]
            ))
            for k, v in results["data_issues"].items():
                session.add(DataQualityIssue(
                    filename=results["filename"],
                    total_rows=results["total_rows"],
                    valid_rows=results["valid_rows"],
                    invalid_rows=results["invalid_rows"],
                    issue_type=k,
                    issue_count=v,
                    criticality=results["criticality"]
                ))
            session.commit()
        except Exception as e:
            logger.error(f"Database save failed: {str(e)}")
            session.rollback()
        finally:
            session.close()

    @task
    def send_alerts(results):
        if not results:
            logger.warning("send_alerts() skipped — no results")
            return
        if not TEAMS_WEBHOOK_URL:
            logger.warning("send_alerts() skipped — no webhook configured")
            return

        try:
            issues_text = "\n".join([
                f"• {k.replace('_', ' ').title()}: {v} failed checks"
                for k, v in results["data_issues"].items()
            ]) or "• No data quality issues detected"

            message = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "FF6D00" if results["criticality"] == "high" else "FFA500" if results["criticality"] == "medium" else "00FF00",
                "summary": f"Data Quality Alert - {results['filename']}",
                "sections": [{
                    "activityTitle": f"🔍 Data Quality Alert - {results['criticality'].upper()} Priority",
                    "activitySubtitle": f"File: {results['filename']}",
                    "facts": [
                        {"name": "Criticality", "value": results["criticality"].upper()},
                        {"name": "Valid Rows", "value": str(results["valid_rows"])},
                        {"name": "Invalid Rows", "value": str(results["invalid_rows"])},
                        {"name": "Data Quality Score", "value": f"{(results['valid_rows'] / results['total_rows'] * 100):.1f}%"}
                    ],
                    "text": f"**Summary:**\n{issues_text}"
                }]
            }

            requests.post(TEAMS_WEBHOOK_URL, json=message, timeout=10)
        except Exception as e:
            logger.error(f"Failed to send Teams alert: {str(e)}")

    @task
    def split_and_save_data(results):
        if not results or "file_path" not in results:
            logger.warning(f"split_and_save_data() skipped — results={results}")
            return
        try:
            src = results["file_path"]
            filename = results["filename"]
            
            logger.info(f"=== PROCESSING FILE: {filename} ===")
            logger.info(f"Source path: {src}")
            
            # Verify source file exists
            if not os.path.exists(src):
                logger.error(f"Source file {src} does not exist. Already processed?")
                return
            
            success_rate = results["successful_expectations"] / results["total_expectations"]
            if success_rate >= 0.95:
                dst = os.path.join(GOOD_DATA_FOLDER, filename)
                category = "good"
            elif success_rate >= 0.85:
                dst = os.path.join(GOOD_DATA_FOLDER, filename)
                category = "good (partial pass)"
            else:
                dst = os.path.join(BAD_DATA_FOLDER, filename)
                category = "bad"

            logger.info(f"Success rate: {success_rate:.2f} -> Category: {category}")
            logger.info(f"Moving to: {dst}")
            
            # CRITICAL: Move file (this deletes from source automatically)
            shutil.move(src, dst)
            
            # VERIFY the source file was actually deleted
            if os.path.exists(src):
                logger.error(f"CRITICAL ERROR: Source file {src} still exists after move!")
                logger.error("This will cause infinite loop - manually deleting...")
                os.remove(src)
            else:
                logger.info(f"CONFIRMED: Source file {src} successfully deleted")
            
            logger.info(f"SUCCESS: {filename} processed -> {category} folder")
            logger.info(f"=== END PROCESSING ===")
            
        except Exception as e:
            logger.error(f"split_and_save_data() failed: {str(e)}")
            # If move failed, make sure to delete source to prevent infinite loop
            if 'src' in locals() and os.path.exists(src):
                logger.warning(f"Cleaning up source file {src} due to error")
                try:
                    os.remove(src)
                    logger.info(f"Source file {src} cleaned up")
                except Exception as cleanup_error:
                    logger.error(f"Failed to cleanup source file: {cleanup_error}")

    file_path = read_data()
    validation = validate_data(file_path)
    
    # Execute these three tasks in parallel
    save_stats = save_statistics(validation)
    send_alert = send_alerts(validation)
    split_save = split_and_save_data(validation)
    
    # Set parallel execution no dependencies between these three
    [save_stats, send_alert, split_save]

data_ingestion_dag = data_ingestion_pipeline()