# app.py
import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Base URL - use environment variable or default
API_URL = os.getenv("PREDICTION_API_URL", "http://127.0.0.1:8000")

st.set_page_config(page_title="JB Link Customer Churn Prediction App", layout="wide")

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Prediction", "Past Predictions"])


# Prediction Page
if page == "Prediction":
    st.title("JB Link Customer Churn Prediction App")

    # Single Prediction Form
    st.subheader("Single Prediction")

    customer_id = st.text_input("Customer ID", "CUST000")
    tenure_in_months = st.number_input("Tenure (months)", min_value=0, step=1)
    monthly_charge = st.number_input("Monthly Charge ($)", min_value=0.0, step=0.01)
    total_customer_svc_requests = st.number_input("Total Customer Service Requests", min_value=0, step=1)

    # Input validation
    if st.button("Predict"):
        if not customer_id or not customer_id.strip():
            st.error("Please enter a valid Customer ID")
        elif tenure_in_months < 0:
            st.error("Tenure in months must be a non-negative number")
        elif monthly_charge < 0:
            st.error("Monthly charge must be a non-negative number")
        elif total_customer_svc_requests < 0:
            st.error("Total customer service requests must be a non-negative number")
        else:
            input_data = {
                "customers": [{
                    "customer_id": customer_id.strip(),
                    "tenure_in_months": tenure_in_months,
                    "monthly_charge": monthly_charge,
                    "total_customer_svc_requests": total_customer_svc_requests
                }]
            }

            # Send request to FastAPI with loading indicator
            try:
                with st.spinner('Making prediction...'):
                    response = requests.post(f"{API_URL}/predict", json=input_data)
                
                if response.status_code == 200:
                    prediction = response.json()
                    
                    # Validate response structure
                    if "status" in prediction and prediction["status"] == "success" and "predictions" in prediction and prediction["predictions"]:
                        result = prediction["predictions"][0]["prediction"]
                        st.success(f"Prediction: **{result}**")
                        
                        # Display input features used for prediction
                        st.write("Input features used:")
                        feature_df = pd.DataFrame([{
                            "Customer ID": customer_id,
                            "Tenure (months)": tenure_in_months,
                            "Monthly Charge ($)": monthly_charge,
                            "Service Requests": total_customer_svc_requests
                        }])
                        st.dataframe(feature_df)
                    else:
                        st.error("Unexpected response structure from API")
                        st.write("API Response:", prediction)
                else:
                    st.error(f"Error making prediction: Status code {response.status_code}")
                    st.error(f"Response details: {response.text}")
            except requests.exceptions.RequestException as e:
                st.error(f"Connection error: {str(e)}")
                st.info(f"Attempting to connect to: {API_URL}")

    # Batch Prediction Form
    st.subheader("Upload CSV for Batch Prediction")

    uploaded_file = st.file_uploader("Upload a CSV file with these columns only: Customer ID, Tenure in Months, Monthly Charge, Total Customer Svc Requests")

    if uploaded_file:
        try:
            # Read the CSV file
            df = pd.read_csv(uploaded_file)
            
            # Validate CSV structure
            required_columns = ["Customer ID", "Tenure in Months", "Monthly Charge", "Total Customer Svc Requests"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"Missing required columns: {', '.join(missing_columns)}")
                st.write("Required columns:", required_columns)
                st.write("Found columns:", list(df.columns))
            else:
                # Show the original data
                st.write("Original Data:", df.head())
                
                # Validate data types and values
                validation_errors = []
                
                # Check for empty customer IDs
                if df["Customer ID"].isnull().any() or (df["Customer ID"] == "").any():
                    validation_errors.append("Customer ID contains empty values")
                
                # Check for negative values
                numeric_columns = ["Tenure in Months", "Monthly Charge", "Total Customer Svc Requests"]
                for col in numeric_columns:
                    if (df[col] < 0).any():
                        validation_errors.append(f"{col} contains negative values")
                
                if validation_errors:
                    st.error("Data validation errors:")
                    for error in validation_errors:
                        st.write(f"- {error}")
                else:
                    if st.button("Predict for Uploaded Data"):
                        # Create a list of properly formatted customer records
                        customers = []
                        
                        # Format data as dictionary
                        for _, row in df.iterrows():
                            customer = {
                                "customer_id": str(row["Customer ID"]).strip(),
                                "tenure_in_months": int(row["Tenure in Months"]),
                                "monthly_charge": float(row["Monthly Charge"]),
                                "total_customer_svc_requests": int(row["Total Customer Svc Requests"])
                            }
                            customers.append(customer)
                        
                        # Create the batch payload
                        batch_payload = {"customers": customers}
                        
                        # Send to API with loading indicator
                        try:
                            with st.spinner(f'Making predictions for {len(customers)} customers...'):
                                response = requests.post(f"{API_URL}/predict", json=batch_payload)
                            
                            if response.status_code == 200:
                                prediction_data = response.json()
                                
                                # Validate response structure
                                if "status" in prediction_data and prediction_data["status"] == "success" and "predictions" in prediction_data:
                                    predictions = prediction_data["predictions"]
                                    pred_df = pd.DataFrame(predictions)
                                    
                                    # Merge with original data to show features alongside predictions
                                    df_with_predictions = df.copy()
                                    df_with_predictions["Prediction"] = [p["prediction"] for p in predictions]
                                    df_with_predictions["Source"] = [p["source"] for p in predictions]
                                    
                                    st.success(f"Successfully processed {len(predictions)} predictions")
                                    st.write("Batch Predictions with Features:")
                                    st.dataframe(df_with_predictions)
                                else:
                                    st.error("Unexpected response structure from API")
                                    st.write("API Response:", prediction_data)
                            else:
                                st.error(f"Error making batch predictions: {response.status_code}")
                                st.error(f"Error details: {response.text}")
                        except requests.exceptions.RequestException as e:
                            st.error(f"Connection error: {str(e)}")
                            st.info(f"Attempting to connect to: {API_URL}")
        except Exception as e:
            st.error(f"Error processing CSV: {str(e)}")

# Past Predictions Page
elif page == "Past Predictions":
    st.title("View Past Predictions")

    # Date Selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date")
    with col2:
        end_date = st.date_input("End Date")
    
    # Validate date selection
    if start_date and end_date and start_date > end_date:
        st.error("Start date must be before or equal to end date")
    
    # Add prediction source filter dropdown
    prediction_source = st.selectbox(
        "Prediction Source", 
        options=["all", "webapp", "scheduled predictions"],
        index=0,
        help="Filter predictions by their source"
    )

    # Fetch Past Predictions
    if st.button("Fetch Past Predictions"):
        if start_date and end_date and start_date > end_date:
            st.error("Please correct the date range before fetching predictions")
        else:
            query_params = {}
            if start_date and end_date:
                query_params["start_date"] = start_date.strftime("%Y-%m-%d")
                query_params["end_date"] = end_date.strftime("%Y-%m-%d")
            
            # Add source to query parameters if not "all"
            if prediction_source != "all":
                query_params["source"] = prediction_source

            # Send request to API with loading indicator
            try:
                with st.spinner('Fetching past predictions...'):
                    response = requests.get(f"{API_URL}/past-predictions", params=query_params)
                
                if response.status_code == 200:
                    response_data = response.json()
                    past_predictions = response_data.get("past_predictions", [])
                    
                    if past_predictions:
                        pred_df = pd.DataFrame(past_predictions)
                        # Rename columns for better readability
                        pred_df.rename(columns={
                            "customer_id": "Customer ID",
                            "tenure_in_months": "Tenure (Months)",
                            "monthly_charge": "Monthly Charge ($)",
                            "total_customer_svc_requests": "Service Requests",
                            "prediction": "Churn Prediction",
                            "predicted_at": "Predicted At",
                            "source": "Source"
                        }, inplace=True)
                        
                        # Format datetime
                        pred_df["Predicted At"] = pd.to_datetime(pred_df["Predicted At"]).dt.strftime("%Y-%m-%d %H:%M:%S")
                        
                        st.success(f"Found {len(past_predictions)} predictions")
                        st.write("Past Predictions:")
                        st.dataframe(pred_df)
                        
                        # Show summary statistics
                        st.subheader("Summary Statistics")
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            churn_count = len(pred_df[pred_df["Churn Prediction"] == "Churn"])
                            st.metric("Churn Predictions", churn_count)
                        with col2:
                            no_churn_count = len(pred_df[pred_df["Churn Prediction"] == "No Churn"])
                            st.metric("No Churn Predictions", no_churn_count)
                        with col3:
                            if len(pred_df) > 0:
                                churn_rate = (churn_count / len(pred_df)) * 100
                                st.metric("Churn Rate (%)", f"{churn_rate:.1f}")
                    else:
                        st.warning("No past predictions found for the selected filters.")
                        st.info("Try adjusting your date range or prediction source filter.")
                else:
                    st.error(f"Error fetching past predictions: Status code {response.status_code}")
                    st.error(f"Response details: {response.text}")
            except requests.exceptions.RequestException as e:
                st.error(f"Connection error: {str(e)}")
                st.info(f"Attempting to connect to: {API_URL}")