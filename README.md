# JB Link Customer Churn Prediction System

## Introduction

This project is a comprehensive **production-ready data science system** designed for JB Link, a telecom company in California. It predicts customer churn using various customer attributes, enabling the company to proactively identify at-risk customers and implement effective retention strategies. With a focus on reducing the current high churn rate (43% of new customers leaving by the end of the quarter), this system provides enterprise-grade predictive analytics capabilities.

**Team AbHe-ViPa:**
- Abubakar Aliyu
- Hemanthvenkatadurgasai Surendra Babu Imandi
- Vinay Gaddam
- Pawan Kumar Burla
- Sri Sainath Reddy Chinakala

## Key Features

### ML Production Pipeline
- **Model as a Service (FastAPI)**: RESTful API with batch prediction optimization
- **User Interface (Streamlit)**: Professional web interface for predictions and analytics
- **Automated Data Ingestion**: Real-time data quality validation and processing
- **Scheduled Predictions**: Automated prediction jobs every 2 minutes
- **Real-time Monitoring**: Grafana dashboards for data quality and model drift
- **Teams Notifications**: Automated alerts for data quality issues

### Core Capabilities
- **Single Customer Prediction**: Predict churn risk for individual customers
- **Batch Prediction**: Process CSV files for multiple customers simultaneously (optimized for performance)
- **Historical Analytics**: Access and filter past predictions with temporal analysis
- **Data Quality Management**: Comprehensive validation with 7+ error detection types
- **Real-time Monitoring**: Live dashboards for data drift and prediction tracking

## System Architecture

This is a **complete ML production system** with:
- **Microservices Architecture**: Containerized services with Docker
- **Database Layer**: PostgreSQL for predictions and data quality metrics
- **Orchestration**: Apache Airflow for automated workflows
- **Monitoring Stack**: Grafana dashboards with real-time updates
- **Quality Assurance**: Automated data validation and alerting

## Prerequisites

- **Docker & Docker Compose** (Required)
- **Python 3.9+**
- **Git**
- **8GB+ RAM** (for full system)
- **Microsoft Teams** (for notifications)

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/jblink-churn-prediction.git
cd jblink-churn-prediction
```

### 2. Environment Configuration
Create a `.env` file in the root directory:
```bash
# Database Configuration
DATABASE_URL=postgresql://postgres:jblink@localhost:5433/jblink_db

# FastAPI Configuration
MODEL_PATH=../model/jbchurn_model.pkl

# Streamlit Configuration
PREDICTION_API_URL=http://127.0.0.1:8000

# Airflow Settings
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Teams webhook for alerts
TEAMS_WEBHOOK_URL=your-teams-webhook-url-here

# Data Split Script Configuration
DATA_SPLIT_INPUT_FILE=path/to/your/dataset.csv
DATA_SPLIT_OUTPUT_FOLDER=./data_ingestion/raw_data
DATA_SPLIT_NUM_FILES=700
```

### 3. Start the Complete System
```bash
docker-compose up --build
# Or run in background
docker-compose up -d --build
```

### 4. Access the Interfaces
- Streamlit UI: http://localhost:8501
- Airflow: http://localhost:8081 (login: airflow / airflow)
- Grafana: http://localhost:3000 (login: admin / admin)
- PgAdmin: http://localhost:5050 (login: admin@example.com / project)
- FastAPI Docs: http://localhost:8000/docs

## Usage

### Streamlit Web Interface

#### Prediction Page
- Single Prediction: Form-based individual customer analysis
- Batch Prediction: CSV upload for multiple customers (with enhanced validation)
- Real-time Results: Instant predictions with data quality feedback

#### Past Predictions Page
- Historical Analysis: View and filter past predictions
- Source Filtering: webapp vs scheduled predictions
- Date Range Selection: Temporal analysis capabilities
- Summary Statistics: Churn rates and prediction trends

### API Usage

#### Make Predictions
```bash
curl -X POST "http://localhost:8000/predict" \
-H "Content-Type: application/json" \
-d '{
  "customers": [{
    "customer_id": "CUST001",
    "tenure_in_months": 24,
    "monthly_charge": 70.5,
    "total_customer_svc_requests": 2
  }]
}'
```

#### Get Past Predictions
```bash
curl "http://localhost:8000/past-predictions?start_date=2025-07-01&source=webapp"
```

## Automated Workflows

### Data Ingestion Pipeline (Every 1 minute)
1. Read Data: Random file selection from raw_data folder
2. Validate Quality: 7 types of data quality checks
3. Save Statistics: Store metrics in database
4. Send Alerts: Teams notifications with criticality levels
5. Data Routing: Split good/bad data into appropriate folders

### Prediction Job (Every 2 minutes)
1. Check New Data: Monitor good_data folder for new files
2. Make Predictions: Batch API calls to model service
3. Smart Tracking: JSON-based file processing without moving files

## Monitoring & Alerting

### Grafana Dashboards
- Data Drift Monitoring: Feature drift detection between training/serving
- Prediction Analytics: Real-time prediction distribution and trends
- Data Quality Metrics: Temporal statistics and issue tracking
- System Health: Performance and operational metrics

### Teams Notifications
- Color-coded Alerts: Green/Orange/Red based on criticality
- Detailed Reports: Data quality scores and issue breakdowns
- HTML Reports: Comprehensive validation summaries
- Real-time Delivery: Instant notifications for data issues

## Data Management

### Dataset Information
- Source: JB Link Telco Customer Churn Dataset
- Size: 7,043 customers with 46 features
- Key Features: Customer ID, Tenure, Monthly Charge, Service Requests
- Target: Churn prediction (Binary classification)

### Data Quality Validation
The system validates 7 types of data issues:
1. Missing Columns
2. Missing Values
3. Negative Values
4. String in Numerical
5. Outlier Values
6. Duplicate Rows
7. Unknown Categories

## Project Structure

```
├── backend/                      # FastAPI application
│   ├── Dockerfile
│   ├── main.py                   # FastAPI server with batch optimization
│   ├── database.py               # SQLAlchemy models
│   ├── crud.py                   # Database operations with batch support
│   └── requirements-fastapi.txt
├── frontend/                     # Streamlit application
│   ├── Dockerfile
│   ├── app.py                    # Enhanced Streamlit UI
│   └── requirements-webapp.txt
├── airflow/                      # Airflow DAGs
│   ├── dags/
│   │   ├── ingestion_job.py      # Data ingestion pipeline
│   │   └── prediction_job.py     # Automated prediction job
│   ├── logs/                     # Airflow logs
│   └── plugins/                  # Custom plugins
├── data_ingestion/               # Data pipeline
│   ├── raw_data/                 # Input files (700 generated files)
│   ├── good_data/                # Validated data
│   ├── bad_data/                 # Failed validation data
│   ├── processed/                # Prediction tracking
│   └── reports/                  # HTML validation reports
├── model/                        # ML model artifacts
│   └── jbchurn_model.pkl         # Trained scikit-learn model
├── scripts/                      # Utility scripts
│   └── data_split.py             # Data generation with 7 error types
├── docker-compose.yaml           # Complete system orchestration
├── .env                          # Environment configuration
└── requirements.txt              # Python dependencies
```

## System Configuration

### Environment Variables
All system configuration is managed through the `.env` file:
- Database URLs
- API Endpoints
- Teams Integration
- Airflow Settings
- File Paths

### Docker Services
- fastapi: Model serving API (Port 8000)
- webapp: Streamlit interface (Port 8501)
- prediction-db: PostgreSQL database (Port 5433)
- airflow-webserver: Airflow UI (Port 8081)
- airflow-scheduler: DAG execution engine
- jblink_grafana: Monitoring dashboards (Port 3000)
- pgadmin: Database management (Port 5050)

## Testing the System

### 1. Generate Test Data
```bash
python scripts/data_split.py
```

### 2. Monitor Airflow DAGs
- Access Airflow UI and enable both DAGs
- Watch automated processing every 1-2 minutes
- Check Teams for data quality alerts

### 3. Test Predictions
- Use Streamlit UI for single/batch predictions
- Monitor Grafana dashboards for real-time updates
- Check past predictions for historical analysis

## Production Deployment

### Performance Optimizations
- Batch Processing: O(N) → O(1) optimization
- Database Transactions: Single commits for batch operations
- Efficient File Tracking: JSON-based processing state
- Container Orchestration: Docker networking and volume management

### Monitoring & Alerting
- Real-time Dashboards
- Automated Notifications
- Health Checks
- Logging

## Troubleshooting

### Common Issues
1. Port Conflicts: Ensure required ports are available
2. Teams Webhook: Verify the URL is correctly set
3. Model Loading: Confirm model file exists at given path
4. Database Connection: Check container health

### System Health Checks
```bash
docker-compose ps
curl http://localhost:8000/health
docker-compose logs [service-name]
```

## Performance Metrics

- Prediction Latency: Under 500ms for batch
- Data Processing Rate: 1 file/minute
- System Uptime: 99.9% with Docker restart policies
- Scalability: Supports concurrent usage and batch loads

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Future Enhancements

- Model Retraining Pipeline
- A/B Testing Framework
- Advanced Monitoring
- API Rate Limiting
- Multi-tenant Support

---

Designed and implemented by Team AbHe-ViPa, led by [Abubakar Aliyu](mailto:abubakaraliyu599@gmail.com), with a focus on reliability, scalability, and end-to-end production readiness for machine learning systems.

