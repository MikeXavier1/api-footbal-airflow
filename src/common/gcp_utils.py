import os
import json
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from typing import Literal, Dict, Any, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

env_path: str = "src/config/.env"
load_dotenv(dotenv_path=env_path) # This is now correctly uncommented

def get_spark_session(
        app_name="BigQueryParamExtractor",
        project_id=None
    ):
    # ... (your get_spark_session function remains the same) ...
    if not project_id:
        logger.error("Project ID must be provided to get_spark_session.")
        raise ValueError("Project ID is required.")

    try:
        spark_bigquery_connector_package = ",".join([
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
            "javax.inject:javax.inject:1"
        ])

        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", spark_bigquery_connector_package) \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.cloud.google.project.id", project_id) \
            .config("parentProject", project_id) \
            .getOrCreate()
        
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error initializing SparkSession: {e}")
        raise

def extract_bigquery_params(
        spark: SparkSession,
        project_id: str,
        dataset_id: str,
        table_name: str,
        query: str = None,
        output_column: str = None,
        output_format: str = "list"
    ):
    # ... (your extract_bigquery_params function remains the same) ...
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    logger.info(f"Attempting to read data from BigQuery table: {full_table_id}")

    try:
        if query:
            df = spark.read.format("bigquery") \
                .option("project", project_id) \
                .option("dataset", dataset_id) \
                .option("table", table_name) \
                .option("query", query) \
                .load()
            logger.info(f"Executed custom query: {query}")
        else:
            df = spark.read.format("bigquery") \
                .option("table", full_table_id) \
                .load()
            logger.info(f"Loaded data from table: {full_table_id}")

        if df.isEmpty():
            logger.warning("No data found from BigQuery query.")
            return [] if output_format in ["list", "json_list"] else spark.createDataFrame([], df.schema)

        if output_format == "dataframe":
            logger.info("Returning Spark DataFrame.")
            return df
        
        elif output_format == "list":
            if not output_column:
                raise ValueError("output_column must be specified for 'list' output_format.")
            
            params_list = [row[output_column] for row in df.select(col(output_column)).distinct().collect()]
            logger.info(f"Extracted {len(params_list)} parameters in list format.")
            return params_list

        elif output_format == "json_list":
            params_json_list = [row.asDict() for row in df.collect()]
            logger.info(f"Extracted {len(params_json_list)} parameters in JSON list format.")
            return json.dumps(params_json_list)

        else:
            raise ValueError(f"Unsupported output_format: {output_format}")

    except Exception as e:
        logger.error(f"Error extracting BigQuery parameters: {e}")
        raise

def get_from_bigquery(
    project_id: str,
    dataset_id: str,
    table_name: str,
    column_name: str
) -> List[str]:
    """
    Fetches a list of unique field from a specified BigQuery table.
    Manages SparkSession creation and shutdown internally.
    """
    spark = None
    try:
        spark = get_spark_session(project_id=project_id)
        
        query = f"""
        SELECT DISTINCT {column_name} 
        FROM `{project_id}.{dataset_id}.{table_name}` 
        WHERE {column_name} IS NOT NULL
        """
        
        unique_fields = extract_bigquery_params(
            spark=spark,
            project_id=project_id,
            dataset_id=dataset_id,
            table_name=table_name,
            query=query,
            output_column=column_name,
            output_format="list"
        )
        logger.info(f"Successfully fetched {len(unique_fields)} itens from {table_name}.")
        return unique_fields
    except Exception as e:
        logger.error(f"Failed to fetch country names from BigQuery: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped for country extraction.")

# Manual Test

if __name__ == "__main__":
    # --- START OF DEBUGGING CODE ---
    # Check if .env file exists at the expected path
    absolute_env_path = os.path.abspath(env_path)
    if not os.path.exists(absolute_env_path):
        logger.error(f"ERROR: .env file not found at: {absolute_env_path}")
        logger.error("Please ensure your .env file is in src/config/ relative to your execution directory.")
    else:
        logger.info(f".env file found at: {absolute_env_path}")

    # Check if GOOGLE_APPLICATION_CREDENTIALS was loaded and if the path is valid
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if GOOGLE_APPLICATION_CREDENTIALS:
        logger.info(f"GOOGLE_APPLICATION_CREDENTIALS environment variable is set to: {GOOGLE_APPLICATION_CREDENTIALS}")
        if not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
            logger.error(f"ERROR: The credential file specified in GOOGLE_APPLICATION_CREDENTIALS does NOT exist at: {GOOGLE_APPLICATION_CREDENTIALS}")
            logger.error("Please verify the path to your service account key in your .env file.")
        elif not os.access(GOOGLE_APPLICATION_CREDENTIALS, os.R_OK):
            logger.error(f"ERROR: No read permissions for credential file at: {GOOGLE_APPLICATION_CREDENTIALS}")
            logger.error("Please check file permissions (e.g., `chmod 400 <key_file>`).")
        else:
            logger.info("Credential file path is valid and readable.")
    else:
        logger.error("ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is NOT set.")
        logger.error("This means load_dotenv() failed or the variable is missing from your .env file.")
    # --- END OF DEBUGGING CODE ---

    GCP_PROJECT_ID = "api-football-461418"
    BQ_DATASET_ID = "api_football" 
    BQ_TABLE_NAME = "dim_countries"
    
    PARAM_QUERY = f"""
    SELECT 
        country_name
    FROM 
        `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_NAME}`
    """
    PARAM_OUTPUT_COLUMN = "country_name"
    
    spark = None
    try:
        spark = get_spark_session(project_id=GCP_PROJECT_ID) 

        country_name = extract_bigquery_params(
            spark,
            project_id=GCP_PROJECT_ID,
            dataset_id=BQ_DATASET_ID,
            table_name=BQ_TABLE_NAME,
            query=PARAM_QUERY,
            output_column=PARAM_OUTPUT_COLUMN,
            output_format="list"
        )
        logger.info(f"Extracted country names: {country_name}")
    
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped.")