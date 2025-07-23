FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Dagster and additional packages
RUN pip install --no-cache-dir \
    dagster==1.5.8 \
    dagster-webserver==1.5.8 \
    dagster-postgres==0.21.8 \
    pandas \
    openpyxl \
    xlrd \
    sqlalchemy \
    psycopg2-binary

# Set working directory
WORKDIR /opt/dagster/app

# Copy Dagster project files
COPY dagster/ /opt/dagster/app/

# Set environment variables
ENV PYTHONPATH=/opt/dagster/app

# Default command (will be overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]