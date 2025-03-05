# app.py
import streamlit as st
import requests
import pandas as pd

# API Base URL
API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="JB Link Customer Churn Prediction App", layout="wide")

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Prediction", "Past Predictions"])


# Prediction Page
if page == "Prediction":
    st.title("📊 JB Link Customer Churn Prediction App")

    # Single Prediction Form
    st.subheader("🔹 Single Prediction")

    customer_id = st.text_input("Customer ID", "CUST000")
    tenure_in_months = st.number_input("Tenure (months)", min_value=0, step=1)
    monthly_charge = st.number_input("Monthly Charge ($)", min_value=0.0, step=0.01)
    total_customer_svc_requests = st.number_input("Total Customer Service Requests", min_value=0, step=1)

    if st.button("Predict"):
        input_data = {
            "customers": [{
                "customer_id": customer_id,
                "tenure_in_months": tenure_in_months,
                "monthly_charge": monthly_charge,
                "total_customer_svc_requests": total_customer_svc_requests
            }]
        }

        # Send request to FastAPI
        response = requests.post(f"{API_URL}/predict", json=input_data)

        if response.status_code == 200:
            prediction = response.json()
            st.success(f"✅ Prediction: **{prediction['predictions'][0]['prediction']}**")
        else:
            st.error("❌ Error making prediction")

    # Batch Prediction Form
    st.subheader("📂 Upload CSV for Batch Prediction")

    uploaded_file = st.file_uploader("Upload a CSV file with these columns only: Customer ID, Tenure in Months, Monthly Charge, Total Customer Svc Requests")

    if uploaded_file:
        df = pd.read_csv(uploaded_file)

        # Validate if expected columns are present
        expected_columns = ["Customer ID", "Tenure in Months", "Monthly Charge", "Total Customer Svc Requests"]
        
        if not all(col in df.columns for col in expected_columns):
            st.error(f"⚠ CSV file must contain columns: {expected_columns}")
        else:
            st.write("🔍 Preview of Uploaded Data:", df.head())

            if st.button("Predict for Uploaded Data"):
                # Convert DataFrame to JSON
                data_records = df.to_dict(orient="records")
                batch_payload = {"customers": data_records}

                # Send API request
                response = requests.post(f"{API_URL}/predict", json=batch_payload)

                if response.status_code == 200:
                    predictions = response.json()["predictions"]
                    pred_df = pd.DataFrame(predictions)
                    st.write("✅ **Batch Predictions:**", pred_df)
                else:
                    st.error("❌ Error making batch predictions")

# Past Predictions Page
elif page == "Past Predictions":
    st.title("📜 View Past Predictions")

    # Date Selection
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")

    # Fetch Past Predictions
    if st.button("Fetch Past Predictions"):
        query_params = {}
        if start_date and end_date:
            query_params["start_date"] = start_date.strftime("%Y-%m-%d")
            query_params["end_date"] = end_date.strftime("%Y-%m-%d")

        # Send request to API
        response = requests.get(f"{API_URL}/past-predictions", params=query_params)

        if response.status_code == 200:
            past_predictions = response.json().get("past_predictions", [])
            if past_predictions:
                pred_df = pd.DataFrame(past_predictions)
                # Rename columns for better readability
                pred_df.rename(columns={
                    "customer_id": "Customer ID",
                    "tenure_in_months": "Tenure (Months)",
                    "monthly_charge": "Monthly Charge ($)",
                    "total_customer_svc_requests": "Service Requests",
                    "prediction": "Churn Prediction",
                    "predicted_at": "Predicted At"
                }, inplace=True)
                
                # Format datetime
                pred_df["Predicted At"] = pd.to_datetime(pred_df["Predicted At"]).dt.strftime("%Y-%m-%d %H:%M:%S")
                
                st.write("📜 **Past Predictions:**", pred_df)
            else:
                st.warning("⚠ No past predictions found for the selected filters.")
        else:
            st.error("❌ Error fetching past predictions")