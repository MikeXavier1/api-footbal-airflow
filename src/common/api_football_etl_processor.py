import logging
import os
import yaml
from typing import Literal, Dict, Any, Optional
from datetime import datetime, timedelta, timezone

try:
    from src.common.api_football_transformer import APIFootballTransformer
    from src.common.api_football_loader import APIFootballLoader
except ImportError as error:
    logging.error(f"Failed to import modules. Ensure src is in PYTHONPATH or adjust paths: {error}")
    try:
        from api_football_transformer import APIFootballTransformer
        from api_football_loader import APIFootballLoader
        logging.warning("Using direct imports. Ensure module paths are correct for deployment.")
    except ImportError as error_fallback:
        logging.critical(f"Critical: Cannot import core ETL modules. Exiting. Error: {error_fallback}")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))

# Load .env
try:
    from dotenv import load_dotenv
    dotenv_path_full = os.path.join(project_root, 'src', 'config', '.env')
    if os.path.exists(dotenv_path_full):
        load_dotenv(dotenv_path=dotenv_path_full)
        logger.info("Environment variables loaded from .env for local testing.")
    else:
        logger.warning(f".env file not found at {dotenv_path_full}. Relying on system environment variables.")
except ImportError:
    logger.warning("python-dotenv not installed. Cannot load .env file for local testing.")
except Exception as e:
    logger.warning(f"Failed to load .env file: {e}")

# Load config variables
GCP_RAW_BUCKET = os.getenv("GCP_RAW_BUCKET")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Load table upload strategies
UPLOAD_MODES: Dict[str, str] = {}
try:
    upload_modes_path = os.path.join(project_root, 'src', 'config', 'table_upload_modes.yml')
    with open(upload_modes_path, "r") as file:
        UPLOAD_MODES = yaml.safe_load(file)
    logger.info(f"Loaded upload modes from {upload_modes_path}: {UPLOAD_MODES}")
except FileNotFoundError:
    logger.critical(f"Missing config file: {upload_modes_path}")
    raise
except Exception as error:
    logger.critical(f"Error loading table_upload_modes.yml: {error}")
    raise

class ETLProcessor:
    """
    Orchestrates the ETL process for API Football tables,
    using configuration from environment and YAML files.
    """

    def __init__(
            self,
            bucket_name: str,
            credentials_path: str
        ):
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path
        self._validate_config()

    def _validate_config(self):
        if not self.bucket_name:
            logger.critical("Missing GCP_RAW_BUCKET.")
            raise ValueError("GCP_RAW_BUCKET is not configured.")
        if not self.credentials_path or not os.path.exists(self.credentials_path):
            logger.critical(f"Invalid GOOGLE_APPLICATION_CREDENTIALS: {self.credentials_path}")
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not configured or path is invalid.")
        if not UPLOAD_MODES:
            logger.critical("UPLOAD_MODES is empty or not loaded.")
            raise ValueError("UPLOAD_MODES is missing.")
        logger.info("ETLProcessor configuration validated.")

    def _perform_core_etl_steps(
        self,
        table_name: str,
        stage: Literal["raw", "processed"],
        load_strategy: Literal["overwrite", "append"],
        querystring_params: Dict[str, Any],
        **context: Any
    ) -> Optional[str]:
        logger.info(f"Executing core ETL for table: '{table_name}' (Strategy: '{load_strategy}') with params: {querystring_params}")

        if APIFootballTransformer is None:
            raise RuntimeError("APIFootballTransformer is not imported.")

        try:
            transformer = APIFootballTransformer(table_name=table_name, **querystring_params)
            transformed_df = transformer.transform()
            logger.info(f"Transformed {len(transformed_df)} records for table '{table_name}'.")
        except Exception as error:
            logger.critical(f"Transformation error for '{table_name}': {error}", exc_info=True)
            raise

        except Exception as error:
            logger.critical(f"Error during Extraction or Transformation for table '{table_name}': {error}", exc_info=True)
            raise

        if transformed_df.empty:
            logger.warning(f"No data returned from transformer for '{table_name}'. Skipping GCS load.")
            return None

        if APIFootballLoader is None:
            logger.critical("APIFootballLoader class not available. Cannot perform ETL.")
            raise RuntimeError("APIFootballLoader not imported.")

        try:
            logger.info(f"Step 2: Initializing APIFootballLoader for bucket '{self.bucket_name}'.")
            loader = APIFootballLoader(
                bucket_name=self.bucket_name,
                credentials_path=self.credentials_path
            )
        except Exception as error:
            logger.critical(f"Failed to initialize APIFootballLoader: {error}", exc_info=True)
            raise

        # 3. Load the transformed DataFrame to GCS
        try:
            logger.info(f"Step 3: Loading transformed data for '{table_name}' to GCS with strategy '{load_strategy}'.")
            uploaded_path = loader.load_dataframe(
                df=transformed_df,
                table_name=table_name,
                stage=stage,
            )
            logger.info(f"Data successfully loaded for '{table_name}'. GCS path: {uploaded_path}")

            # Push the GCS path to XComs, if running in Airflow
            if 'ti' in context:
                context['ti'].xcom_push(key=f'{table_name}_gcs_path', value=uploaded_path)
                logger.info(f"Pushed GCS path to XCom with key '{table_name}_gcs_path'.")

            return uploaded_path

        except Exception as e:
            logger.critical(f"An error occurred during data loading to GCS for '{table_name}': {e}", exc_info=True)
            raise

    def run_etl_for_table(
        self,
        table_name: str,
        stage: Literal["raw", "processed"],
        querystring_params: Dict[str, Any],
        **context: Any
    ) -> Optional[str]:
        """
        Executes an ETL process for a given table, determining the load strategy
        from the UPLOAD_MODES configuration.
        """
        load_strategy = UPLOAD_MODES.get(table_name)
        if not load_strategy:
            logger.critical(f"Load strategy for table '{table_name}' not found in UPLOAD_MODES. Skipping ETL.")
            raise ValueError(f"Load strategy not configured for table: {table_name}")

        if load_strategy not in ["overwrite", "append"]:
            logger.critical(f"Invalid load strategy '{load_strategy}' configured for table '{table_name}'. Must be 'overwrite' or 'append'.")
            raise ValueError(f"Invalid load strategy configured for table: {table_name}")

        #if load_strategy == "append" and (not partition_keys or not isinstance(partition_keys, dict)):
        #    logger.error(f"Append strategy chosen for '{table_name}', but partition_keys are missing or invalid.")
        #    raise ValueError(f"Partition keys are mandatory for 'append' load strategy for table: {table_name}")

        logger.info(f"Determined load strategy for '{table_name}': {load_strategy}")
        return self._perform_core_etl_steps(
            table_name=table_name,
            stage=stage,
            load_strategy=load_strategy,
            querystring_params=querystring_params,
            **context
        )

# --- Manual Test ---
if __name__ == "__main__":
    logger.info("\n--- Running manual ETL tests using ETLProcessor class ---")

    # Ensure config vars are loaded for local testing
    GCP_RAW_BUCKET_TEST = os.getenv("GCP_RAW_BUCKET")
    GOOGLE_APPLICATION_CREDENTIALS_TEST = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not (GCP_RAW_BUCKET_TEST and GOOGLE_APPLICATION_CREDENTIALS_TEST and os.path.exists(str(GOOGLE_APPLICATION_CREDENTIALS_TEST) if GOOGLE_APPLICATION_CREDENTIALS_TEST else '')):
        logger.error("GCP_RAW_BUCKET or GOOGLE_APPLICATION_CREDENTIALS not set for local testing. Skipping tests.")
        exit(1) # Exit if configuration is missing for tests

    # Initialize the ETL Processor instance
    try:
        etl_processor = ETLProcessor(
            bucket_name=GCP_RAW_BUCKET_TEST,
            credentials_path=GOOGLE_APPLICATION_CREDENTIALS_TEST
        )
    except Exception as e:
        logger.critical(f"Failed to initialize ETLProcessor: {e}. Exiting tests.")
        exit(1)

    # --- Test 1: ETL for 'countries' (should use 'overwrite' from YAML) ---
    logger.info("\n--- Test 1: Running ETL for 'countries' (mode from YAML) ---")
    try:
        table_name_countries = "countries"
        stage_raw = "raw"
        querystring_countries = {} # No specific search for full countries list

        uploaded_path_countries = etl_processor.run_etl_for_table(
            table_name=table_name_countries,
            stage=stage_raw,
            querystring_params=querystring_countries
        )
        logger.info(f"Manual '{table_name_countries}' ETL finished. Uploaded to: {uploaded_path_countries}")

    except Exception as e:
        logger.critical(f"Manual '{table_name_countries}' ETL test failed: {e}", exc_info=True)


    