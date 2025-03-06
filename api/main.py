from fastapi import FastAPI, Depends, Query
import pickle
import pandas as pd
from typing import Optional
from datetime import datetime
from sqlalchemy.orm import Session
from .database import get_db, CustomerFeatures, BatchPredictionRequest
from .crud import save_prediction, get_past_predictions

ChurnApp = FastAPI()

# Load trained model
with open("./model/jbchurn_model.pkl", "rb") as f:
    model = pickle.load(f)
    
# Prediction endpoint
@ChurnApp.post("/predict")
def predict(features: BatchPredictionRequest, db: Session = Depends(get_db)):
    try:
        results = []
        for customer in features.customers:
            # DataFrame with expected feature names
            data = pd.DataFrame([{
                "Tenure in Months": customer.tenure_in_months,
                "Monthly Charge": customer.monthly_charge,
                "Total Customer Svc Requests": customer.total_customer_svc_requests
            }])

            # Make prediction
            prediction = model.predict(data)[0]
            prediction_label = "Churn" if prediction == 1 else "No Churn"

            # Save prediction in the database
            save_prediction(
                db=db,
                customer_id=customer.customer_id,
                tenure_in_months=customer.tenure_in_months,
                monthly_charge=customer.monthly_charge,
                total_customer_svc_requests=customer.total_customer_svc_requests,
                prediction=prediction_label
            )

            results.append({
                "customer_id": customer.customer_id,
                "prediction": prediction_label
            })

        return {"status": "success", "predictions": results}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
# Past-predictions endpoint
@ChurnApp.get("/past-predictions")
def fetch_past_predictions(
    db: Session = Depends(get_db),
    customer_id: Optional[str] = Query(None, description="Filter by customer ID"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
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
            end_date=end_date_obj
        )
        return {"past_predictions": predictions}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}