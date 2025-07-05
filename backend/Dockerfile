FROM python:3.9-slim

WORKDIR /app

# Install system dependencies including PostgreSQL client
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements-fastapi.txt .
RUN pip install --no-cache-dir -r requirements-fastapi.txt

# Copy application code
COPY . .

# Set environment variables
ENV DOCKER_ENVIRONMENT=true
ENV MODEL_PATH=/app/model/jbchurn_model.pkl

# Create model directory if needed
RUN mkdir -p /app/model

# Expose port
EXPOSE 8000

# Start the application
CMD ["uvicorn", "main:ChurnApp", "--host", "0.0.0.0", "--port", "8000"]