# crud.py
from sqlalchemy.orm import Session
from database import Prediction
from datetime import datetime
from typing import Optional

def save_prediction(db: Session, customer_id: str, tenure_in_months: int, 
                   monthly_charge: float, total_customer_svc_requests: int,
                   prediction: str):
    db_prediction = Prediction(
        customer_id=customer_id,
        tenure_in_months=tenure_in_months,
        monthly_charge=monthly_charge,
        total_customer_svc_requests=total_customer_svc_requests,
        prediction=prediction,
        predicted_at=datetime.now()
    )
    db.add(db_prediction)
    db.commit()
    db.refresh(db_prediction)
    return db_prediction

def get_past_predictions(db: Session, customer_id: Optional[str] = None, 
                         start_date: Optional[datetime] = None, 
                         end_date: Optional[datetime] = None):
    query = db.query(Prediction)
    if customer_id:
        query = query.filter(Prediction.customer_id == customer_id)
    if start_date:
        query = query.filter(Prediction.predicted_at >= start_date)
    if end_date:
        query = query.filter(Prediction.predicted_at <= end_date)
    
    return [{
        "customer_id": pred.customer_id,
        "tenure_in_months": pred.tenure_in_months,
        "monthly_charge": pred.monthly_charge,
        "total_customer_svc_requests": pred.total_customer_svc_requests,
        "prediction": pred.prediction,
        "predicted_at": pred.predicted_at.isoformat()
    } for pred in query.all()]