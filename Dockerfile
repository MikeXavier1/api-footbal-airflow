FROM apache/airflow:2.8.1-python3.10

# Switch to root to install OS-level dependencies
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for PySpark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

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
