"""
This DAG runs on-demand ETL jobs to extract and load raw dimension tables from the API-Football service into GCP.
It uses a dynamic task creation mechanism based on a YAML configuration (table_upload_modes.yml).
Tables included: 'countries', 'leagues/seasons'
"""

from __future__ import annotations

import pendulum
import os
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

try:
    from src.common.api_football_etl_processor import ETLProcessor
except ImportError as error:
    logging.error(f"Failed to import ETLProcessor. Ensure 'src' is in PYTHONPATH: {error}")
    raise

# --- Configuration Loading for the DAG ---
GCP_RAW_BUCKET = os.getenv("GCP_RAW_BUCKET")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not GCP_RAW_BUCKET:
    raise ValueError("Airflow Variable 'GCP_RAW_BUCKET' is not set.")
if GOOGLE_APPLICATION_CREDENTIALS and not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
    logging.warning(f"GOOGLE_APPLICATION_CREDENTIALS '{GOOGLE_APPLICATION_CREDENTIALS}' not found. "
                    "Ensure correct path or rely on Workload Identity.")

# Setup logging for the DAG
log = logging.getLogger(__name__)

# --- Load upload modes from YAML ---
UPLOAD_MODES: Dict[str, str] = {}
try:
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(dag_folder, '..'))
    upload_modes_path = os.path.join(project_root, 'src', 'config', 'table_upload_modes.yml')

    with open(upload_modes_path, "r") as f:
        UPLOAD_MODES = yaml.safe_load(f)
    log.info(f"Loaded upload modes from {upload_modes_path}: {UPLOAD_MODES}")
except FileNotFoundError:
    log.critical(f"CRITICAL: table_upload_modes.yml not found at {upload_modes_path}. Cannot determine upload strategies.")
    raise FileNotFoundError(f"Configuration file missing: {upload_modes_path}")
except Exception as e:
    log.critical(f"CRITICAL: Error loading table_upload_modes.yml: {e}")
    raise


def run_etl_task_callable(table_name: str, **kwargs):
    """
    Python callable for an Airflow PythonOperator.
    Initializes ETLProcessor and runs the ETL for a specific table,
    determining load strategy and partition keys based on table configuration.
    """
    log.info(f"Starting ETL task for table: {table_name}")

    # Initialize ETLProcessor *inside* the task callable for robustness
    try:
        etl_processor = ETLProcessor(
            bucket_name=GCP_RAW_BUCKET,
            credentials_path=GOOGLE_APPLICATION_CREDENTIALS
        )
    except Exception as error:
        log.critical(
            f"Failed to initialize ETLProcessor in task for {table_name}: {error}", exc_info=True
        )
        raise

    querystring_params: Dict[str, Any] = {}
    
    try:
        gcs_path = etl_processor.run_etl_for_table(
            table_name=table_name,
            stage="raw",
            querystring_params=querystring_params,
            **kwargs
        )
        log.info(f"ETL for {table_name} completed. Data loaded to: {gcs_path}")
    except Exception as e:
        log.error(f"ETL for {table_name} failed: {e}", exc_info=True)
        raise

# --- DAG Definition ---

with DAG(
    dag_id="not_dependent_tables_dag",
    start_date=pendulum.datetime(2025, 6, 5, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api-football", "etl", "on-demand"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    }
) as dag:

    # Dynamically create tasks based on UPLOAD_MODES from the YAML file
    tasks = {}
    for table_name in UPLOAD_MODES.keys():
        if table_name in ["countries", "leagues/seasons"]:
            sanitized_table_name = table_name.replace("/", "_")
            task_id = f"extract_load_{sanitized_table_name}"
            tasks[table_name] = PythonOperator(
                task_id=task_id,
                python_callable=run_etl_task_callable,
                op_kwargs={"table_name": table_name},
                doc=f"Extract and load data for the '{table_name}' dimension table from API-Football."
            )
            log.info(f"Created task: {task_id} for table: {table_name}")
