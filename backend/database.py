from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from pydantic import BaseModel
from typing import List
from dotenv import load_dotenv
from urllib.parse import urlparse
import os

# Load environment variables from .env file
load_dotenv()

# Get database connection string from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError("Missing DATABASE_URL in environment")

# Adjust DB host if running in Docker
if os.getenv("DOCKER_ENVIRONMENT") == "true":
    parsed = urlparse(DATABASE_URL)
    if "localhost" in parsed.hostname:
        DATABASE_URL = DATABASE_URL.replace(parsed.hostname, "prediction-db")

# Print configuration for debugging (remove in production)
print(f"Using database: {DATABASE_URL}")

# SQLAlchemy setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Dependency for DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# SQLAlchemy Models
class Prediction(Base):
    __tablename__ = "predictions"

    customer_id = Column(String, primary_key=True, index=True)
    tenure_in_months = Column(Integer)
    monthly_charge = Column(Float)
    total_customer_svc_requests = Column(Integer)
    prediction = Column(String)
    predicted_at = Column(DateTime, default=datetime.utcnow)
    source = Column(String, default="webapp")  # Added source column

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

# Pydantic Models for API
class CustomerFeatures(BaseModel):
    customer_id: str
    tenure_in_months: int
    monthly_charge: float
    total_customer_svc_requests: int

# Define the input schema for batch predictions
class BatchPredictionRequest(BaseModel):
    customers: List[CustomerFeatures]

# Create the database tables
Base.metadata.create_all(bind=engine)

# Uncomment the following lines to recreate tables (will lose all data)
# Base.metadata.drop_all(bind=engine)
# Base.metadata.create_all(bind=engine)
