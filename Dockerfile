FROM apache/airflow:2.8.1-python3.10

# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy your entire project structure into the image
COPY . . 

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH to include the root of your project
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"