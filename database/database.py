# database.py
from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

DATABASE_URL = "postgresql://postgres:jblink@localhost:4190/jblink_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Prediction(Base):
    __tablename__ = "predictions"

    #id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(String, primary_key=True, index=True)
    tenure_in_months = Column(Integer)
    monthly_charge = Column(Float)
    total_customer_svc_requests = Column(Integer)
    prediction = Column(String)
    predicted_at = Column(DateTime, default=datetime.utcnow)

# Create the database tables
Base.metadata.create_all(bind=engine)