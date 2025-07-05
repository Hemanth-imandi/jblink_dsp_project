FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements-webapp.txt .
RUN pip install --no-cache-dir -r requirements-webapp.txt

# Copy application code
COPY . .

# Set environment variables
ENV DOCKER_ENVIRONMENT=true

# Expose port
EXPOSE 8501

# Start the Streamlit application
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]