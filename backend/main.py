from fastapi import FastAPI, Depends, Query
import pickle
import pandas as pd
import numpy as np
import os
from typing import Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from database import get_db, CustomerFeatures, BatchPredictionRequest
from crud import save_prediction, save_batch_predictions, get_past_predictions, get_data_quality_issues
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ChurnApp = FastAPI()

# Load trained model with environment variable configuration
MODEL_PATH = os.getenv("MODEL_PATH")

if not MODEL_PATH:
    raise EnvironmentError("MODEL_PATH is required but not found in environment variables")

try:
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    print(f"Successfully loaded model from {MODEL_PATH}")
except Exception as e:
    print(f"Error loading model from {MODEL_PATH}: {e}")
    raise Exception(f"Could not load model from configured path: {MODEL_PATH}")
    
# Prediction endpoint - OPTIMIZED FOR BATCH PROCESSING
@ChurnApp.post("/predict")
def predict(
    features: BatchPredictionRequest, 
    db: Session = Depends(get_db),
    source: str = Query("webapp", description="Source of the prediction (webapp/scheduled predictions)")
):
    try:
        # STEP 1: Prepare ALL data for batch prediction
        batch_data = []
        customer_info = []
        
        for customer in features.customers:
            batch_data.append({
                "Tenure in Months": customer.tenure_in_months,
                "Monthly Charge": customer.monthly_charge,
                "Total Customer Svc Requests": customer.total_customer_svc_requests
            })
            customer_info.append({
                "customer_id": customer.customer_id,
                "tenure_in_months": customer.tenure_in_months,
                "monthly_charge": customer.monthly_charge,
                "total_customer_svc_requests": customer.total_customer_svc_requests
            })
        
        # STEP 2: Single model prediction call for ALL customers
        batch_df = pd.DataFrame(batch_data)
        batch_predictions = model.predict(batch_df)  # Single prediction call
        
        # STEP 3: Prepare batch data for database
        predictions_for_db = []
        results = []
        
        for i, (customer_data, prediction) in enumerate(zip(customer_info, batch_predictions)):
            prediction_label = "Churn" if prediction == 1 else "No Churn"
            
            # Prepare for batch DB insert
            predictions_for_db.append({
                "customer_id": customer_data["customer_id"],
                "tenure_in_months": customer_data["tenure_in_months"],
                "monthly_charge": customer_data["monthly_charge"],
                "total_customer_svc_requests": customer_data["total_customer_svc_requests"],
                "prediction": prediction_label
            })
            
            # Prepare response
            results.append({
                "customer_id": customer_data["customer_id"],
                "prediction": prediction_label,
                "source": source
            })
        
        # STEP 4: Single database transaction for ALL predictions
        saved_count = save_batch_predictions(
            db=db,
            predictions_data=predictions_for_db,
            source=source
        )
        
        return {
            "status": "success", 
            "predictions": results,
            "total_processed": len(results),
            "saved_to_db": saved_count
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Past-predictions endpoint
@ChurnApp.get("/past-predictions")
def fetch_past_predictions(
    db: Session = Depends(get_db),
    customer_id: Optional[str] = Query(None, description="Filter by customer ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    source: Optional[str] = Query(None, description="Filter by source (webapp/scheduled predictions)")
):
    try:
        # Convert date strings to datetime objects
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
        
        # If end_date is provided, set it to end of day
        if end_date_obj:
            end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)

        predictions = get_past_predictions(
            db=db,
            customer_id=customer_id,
            start_date=start_date_obj,
            end_date=end_date_obj,
            source=source
        )
        return {"past_predictions": predictions}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Data quality issues endpoint
@ChurnApp.get("/data-quality-issues")
def fetch_data_quality_issues(
    db: Session = Depends(get_db),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    criticality: Optional[str] = Query(None, description="Filter by criticality (low/medium/high)"),
    issue_type: Optional[str] = Query(None, description="Filter by issue type")
):
    try:
        # Convert date strings to datetime objects
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.now() - timedelta(days=7)
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
        
        # If end_date is provided, set it to end of day
        if end_date_obj:
            end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)

        issues = get_data_quality_issues(
            db=db,
            start_date=start_date_obj,
            end_date=end_date_obj,
            criticality=criticality,
            issue_type=issue_type
        )
        return {"data_quality_issues": issues}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# Health check endpoint
@ChurnApp.get("/health")
def health_check():
    return {
        "status": "healthy", 
        "model_loaded": model is not None,
        "model_path": MODEL_PATH,
        "timestamp": datetime.now().isoformat()
    }