"""
This DAG runs ETL jobs to extract and load raw table data that has country as parameter from the API-Football service into GCP.
It dynamically fetches country names from BigQuery to drive the extraction.
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
from airflow.decorators import task # For cleaner XCom handling and dynamic mapping

# --- Import custom modules ---
try:
    from src.common.api_football_etl_processor import ETLProcessor
    from src.common.gcp_utils import get_from_bigquery
except ImportError as error:
    logging.error(f"Failed to import custom modules. Ensure 'src' is in PYTHONPATH: {error}")
    raise

# --- Configuration Loading for the DAG ---
# It's better to fetch these from Airflow Variables for production environments
GCP_RAW_BUCKET = os.getenv("GCP_RAW_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
# Credentials path can still come from an environment variable for local testing
# or if you manage credentials outside Airflow's native connections
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if not GCP_RAW_BUCKET:
    raise ValueError("Airflow Variable 'gcp_raw_bucket' is not set.")
if not GCP_PROJECT_ID:
    raise ValueError("Airflow Variable 'gcp_project_id' is not set.")
if GOOGLE_APPLICATION_CREDENTIALS and not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
    logging.warning(f"GOOGLE_APPLICATION_CREDENTIALS '{GOOGLE_APPLICATION_CREDENTIALS}' not found. "
                    "Ensure correct path or rely on Workload Identity.")

# Setup logging for the DAG
log = logging.getLogger(__name__)

# --- Load upload modes from YAML (only for 'teams' in this DAG context) ---
TEAMS_UPLOAD_MODE: Optional[str] = None
try:
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(dag_folder, '..'))
    upload_modes_path = os.path.join(project_root, 'src', 'config', 'table_upload_modes.yml')

    with open(upload_modes_path, "r") as f:
        upload_modes_all = yaml.safe_load(f)
        TEAMS_UPLOAD_MODE = upload_modes_all.get("teams")
    
    if not TEAMS_UPLOAD_MODE:
        log.critical("CRITICAL: 'teams' table not found in table_upload_modes.yml. Cannot determine upload strategy.")
        raise ValueError("'teams' entry missing in table_upload_modes.yml")

    log.info(f"Loaded 'teams' upload mode from {upload_modes_path}: {TEAMS_UPLOAD_MODE}")

except FileNotFoundError:
    log.critical(f"CRITICAL: table_upload_modes.yml not found at {upload_modes_path}. Cannot configure 'teams' ETL.")
    raise FileNotFoundError(f"Configuration file missing: {upload_modes_path}")
except Exception as e:
    log.critical(f"CRITICAL: Error loading table_upload_modes.yml for 'teams': {e}")
    raise


# --- Python Callable for ETL Task ---
def run_etl_task_callable(
        table_name: str,
        querystring_params: Optional[Dict[str, Any]] = None,
        **kwargs
):
    """
    Python callable for an Airflow PythonOperator.
    Initializes ETLProcessor and runs the ETL for a specific table,
    determining load strategy and partition keys based on table configuration.
    """
    log.info(f"Starting ETL task for table: {table_name} with params: {querystring_params}")

    if querystring_params is None:
        querystring_params = {}

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
    dag_id="country_dependency_tables",
    start_date=pendulum.datetime(2025, 6, 5, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["api-football", "etl", "teams"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    }
) as dag:

    # Task to fetch countries from BigQuery
    @task(task_id="fetch_countries_from_bq")
    def _fetch_countries_task():
        """
        Fetches a list of countries from the BigQuery dim_countries table
        using the dedicated helper function in gcp_utils.
        """
        countries = get_from_bigquery(
            project_id=GCP_PROJECT_ID,
            dataset_id="api_football", 
            table_name="dim_countries",
            column_name="country_name"
        )
        return countries

    @task
    def extract_load_teams(country: str):
        run_etl_task_callable(
            table_name="teams",
            querystring_params={"country": country}
        )
    
    @task
    def extract_load_leagues(country: str):
        run_etl_task_callable(
            table_name="leagues",
            querystring_params={"country": country}
        )
    
    countries = _fetch_countries_task()

    teams = extract_load_teams.expand(country=countries)
    leagues = extract_load_leagues.expand(country=countries)

    countries >> [teams, leagues]