# JB Link Customer Churn Prediction System

## Introduction

This project is a comprehensive data science production system designed for JB Link, a telecom company in California. It predicts customer churn using various customer attributes, enabling the company to proactively identify at-risk customers and implement effective retention strategies. With a focus on reducing the current high churn rate (43% of new customers leaving by the end of the quarter), this system provides valuable predictive analytics capabilities.

**Team AbHe-ViPa:**

*   Abubakar Aliyu
*   Hemanthvenkatadurgasai Surendra Babu Imandi
*   Vinay Gaddam
*   Pawan Kumar Burla
*   Sri Sainath Reddy Chilakala

## Features

*   **Single Customer Prediction:** Predict churn risk for individual customers by inputting their details.
*   **Batch Prediction:** Upload CSV files to predict churn for multiple customers simultaneously.
*   **Historical Data Viewing:** Access and filter past predictions to analyze trends.
*   **Data Quality Validation:** Ensure incoming data meets predefined quality standards.

## Getting Started

### Prerequisites

*   Python 3.8+
*   PostgreSQL
*   Docker (for Airflow)
*   Git

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/yourusername/jblink-churn-prediction.git
    cd jblink-churn-prediction
    ```
2.  **Install the required packages:**

    ```bash
    pip install -r requirements.txt
    ```
3.  **Set up the PostgreSQL database:**

    ```bash
    # Create database
    createdb jblink_db

    # Update the database connection string in database.py if needed
    # Current config: "postgresql://postgres:jblink@localhost:4190/jblink_db"
    ```
4.  **Install and configure Apache Airflow (for scheduled predictions):**

    *   Follow the [Apache Airflow Installation Guide](https://medium.com/@datathon/how-to-install-apache-airflow-with-docker-on-windows-52382e13c2e3).
    *   Copy the DAGs from the `dags/` folder to your Airflow DAGs directory.

### Running the Application

1.  **Start the FastAPI server:** 

    ```bash
    uvicorn main:ChurnApp --reload
    ```
2.  **Launch the Streamlit interface:**

    ```bash
    streamlit run app.py
    ```
3.  **Access the application:**

    *   FastAPI Docs: `http://127.0.0.1:8000/docs`
    *   Streamlit UI: `http://localhost:8501`

## Usage

### Streamlit UI

The Streamlit UI provides the following functionalities:

*   **Prediction:**
    *   Single prediction form for individual customer analysis.
    *   Batch prediction via CSV upload.
*   **Past Predictions:**
    *   View historical predictions.
    *   Filter by date range.

## API

### Endpoints

*   `POST /predict`: Make predictions for one or more customers.
*   `GET /past-predictions`: Retrieve historical predictions with optional filters.

## Data

### Dataset Information

The model is trained on the JB Link Telco Customer Churn Dataset, which includes data from 7,043 customers with the following features:

*   **Categorical Features:** Gender, Partner, Dependents, PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies, Contract, PaperlessBilling, PaymentMethod
*   **Numeric Features:** Tenure, MonthlyCharges, TotalCharges
*   **Boolean Features:** SeniorCitizen, Churn


## Project Structure
```markdown
├── airflow/                      # Airflow configuration and DAGs
│   ├── config/                   # Airflow config files
│   ├── dags/                     # DAG definition files
│   │   └── prediction_job.py     # Prediction scheduling DAG
|   |   └── data_ingestion.py     # Data ingestion scheduling
│   ├── docker-compose.yaml       # Docker compose config
│   └── .env                      # Environment variables
├── api/                          # API implementation
│   ├── __init__.py
│   ├── app.py                   
│   ├── crud.py                   # Database operations
│   ├── database.py               # Database models
│   └── main.py                   # FastAPI application
├── data_ingestion/               # Data pipeline components
│   ├── bad_data/                 # Failed validation data
│   ├── good_data/                # Validated data
│   └── raw_data/                 # Input data files
├── JB_dataset/                   # Dataset files
│   ├── processed/                # Processed data
│   └── raw/                      # Raw data files
├── model/                        # Model artifacts
│   └── jbchurn_model.pkl         # Trained model
├── notebook/                     # Jupyter notebooks
│   ├── data_issues.ipynb         # Notebook for generating data issues
│   ├── model.ipynb               # Model development
├── scripts/                      # Python scripts
|   ├── data_split.py             # Script for splitting data
└── requirements.txt              # Python dependencies 
├── README.md                     # Project documentation


## Next Steps:
## Monitoring
The Grafana dashboard will offer monitoring for:

*   Prediction distribution
*   Feature distribution
*   Data quality issues
*   Model drift detection


## Contributing

Please feel free to submit issues or pull requests to improve the project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
