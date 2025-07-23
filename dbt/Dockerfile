FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install dbt and required packages
RUN pip install --no-cache-dir \
    dbt-core==1.6.0 \
    dbt-postgres==1.6.0 \
    pandas \
    openpyxl \
    xlrd

# Copy requirements if they exist
COPY requirements.txt* ./
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Create directories
RUN mkdir -p /app/dbt /app/profiles /app/raw /app/clean

# Set environment variables
ENV DBT_PROFILES_DIR=/app/profiles
ENV PYTHONPATH=/app

# Default command
CMD ["tail", "-f", "/dev/null"]