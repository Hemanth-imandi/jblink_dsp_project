from sqlalchemy.orm import Session
from database import Prediction, DataQualityIssue
from datetime import datetime
from typing import Optional, List

def save_prediction(db: Session, customer_id: str, tenure_in_months: int, 
                   monthly_charge: float, total_customer_svc_requests: int,
                   prediction: str, source: str = "webapp"):
    # Check if prediction already exists
    existing = db.query(Prediction).filter(Prediction.customer_id == customer_id).first()
    
    if existing:
        # Update existing prediction
        existing.tenure_in_months = tenure_in_months
        existing.monthly_charge = monthly_charge
        existing.total_customer_svc_requests = total_customer_svc_requests
        existing.prediction = prediction
        existing.predicted_at = datetime.now()
        existing.source = source
    else:
        # Create new prediction
        db_prediction = Prediction(
            customer_id=customer_id,
            tenure_in_months=tenure_in_months,
            monthly_charge=monthly_charge,
            total_customer_svc_requests=total_customer_svc_requests,
            prediction=prediction,
            predicted_at=datetime.now(),
            source=source
        )
        db.add(db_prediction)
    
    db.commit()
    return existing if existing else db.query(Prediction).filter(Prediction.customer_id == customer_id).first()

# NEW: Batch save function
def save_batch_predictions(db: Session, predictions_data: List[dict], source: str = "webapp"):
    """
    Save multiple predictions in a single database transaction
    """
    try:
        # Get existing customer IDs to handle updates vs inserts
        customer_ids = [pred["customer_id"] for pred in predictions_data]
        existing_predictions = db.query(Prediction).filter(
            Prediction.customer_id.in_(customer_ids)
        ).all()
        existing_ids = {pred.customer_id for pred in existing_predictions}
        
        # Prepare batch operations
        predictions_to_insert = []
        predictions_to_update = []
        
        for pred_data in predictions_data:
            if pred_data["customer_id"] in existing_ids:
                predictions_to_update.append(pred_data)
            else:
                predictions_to_insert.append(pred_data)
        
        # Batch insert new predictions
        if predictions_to_insert:
            new_predictions = [
                Prediction(
                    customer_id=pred["customer_id"],
                    tenure_in_months=pred["tenure_in_months"],
                    monthly_charge=pred["monthly_charge"],
                    total_customer_svc_requests=pred["total_customer_svc_requests"],
                    prediction=pred["prediction"],
                    predicted_at=datetime.now(),
                    source=source
                )
                for pred in predictions_to_insert
            ]
            db.add_all(new_predictions)
        
        # Batch update existing predictions
        if predictions_to_update:
            for pred_data in predictions_to_update:
                db.query(Prediction).filter(
                    Prediction.customer_id == pred_data["customer_id"]
                ).update({
                    "tenure_in_months": pred_data["tenure_in_months"],
                    "monthly_charge": pred_data["monthly_charge"],
                    "total_customer_svc_requests": pred_data["total_customer_svc_requests"],
                    "prediction": pred_data["prediction"],
                    "predicted_at": datetime.now(),
                    "source": source
                })
        
        # Single commit for all operations
        db.commit()
        return len(predictions_data)
        
    except Exception as e:
        db.rollback()
        raise e

def get_past_predictions(db: Session, customer_id: Optional[str] = None, 
                         start_date: Optional[datetime] = None, 
                         end_date: Optional[datetime] = None,
                         source: Optional[str] = None):
    query = db.query(Prediction)
    if customer_id:
        query = query.filter(Prediction.customer_id == customer_id)
    if start_date:
        query = query.filter(Prediction.predicted_at >= start_date)
    if end_date:
        query = query.filter(Prediction.predicted_at <= end_date)
    if source:
        query = query.filter(Prediction.source == source)
    
    return [{
        "customer_id": pred.customer_id,
        "tenure_in_months": pred.tenure_in_months,
        "monthly_charge": pred.monthly_charge,
        "total_customer_svc_requests": pred.total_customer_svc_requests,
        "prediction": pred.prediction,
        "predicted_at": pred.predicted_at.isoformat(),
        "source": pred.source
    } for pred in query.all()]

def get_data_quality_issues(db: Session, start_date: Optional[datetime] = None, 
                            end_date: Optional[datetime] = None, 
                            criticality: Optional[str] = None,
                            issue_type: Optional[str] = None):
    query = db.query(DataQualityIssue)
    
    if start_date:
        query = query.filter(DataQualityIssue.timestamp >= start_date)
    if end_date:
        query = query.filter(DataQualityIssue.timestamp <= end_date)
    if criticality:
        query = query.filter(DataQualityIssue.criticality == criticality)
    if issue_type:
        query = query.filter(DataQualityIssue.issue_type == issue_type)
    
    return [{
        "id": issue.id,
        "timestamp": issue.timestamp.isoformat(),
        "filename": issue.filename,
        "total_rows": issue.total_rows,
        "valid_rows": issue.valid_rows,
        "invalid_rows": issue.invalid_rows,
        "issue_type": issue.issue_type,
        "issue_count": issue.issue_count,
        "criticality": issue.criticality
    } for issue in query.all()]