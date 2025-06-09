FROM apache/airflow:2.8.1-python3.10

# Switch to root to install OS-level dependencies
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements early for layer caching
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and source code
COPY dags/ /opt/airflow/dags/
COPY src/ /opt/airflow/src/

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Set working directory
WORKDIR /opt/airflow
