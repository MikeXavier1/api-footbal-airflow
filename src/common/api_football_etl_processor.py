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

# Load config
GCP_RAW_BUCKET = os.getenv("GCP_RAW_BUCKET")
GCP_CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")

UPLOAD_MODES: Dict[str, str] = {}
try:
    upload_modes_path = os.path.join(project_root, 'src', 'config', 'table_upload_modes.yml')
    with open(upload_modes_path, "r") as file:
        UPLOAD_MODES = yaml.safe_load(file)
    logger.info(f"Loaded upload modes from {upload_modes_path}: {UPLOAD_MODES}")
except FileNotFoundError:
    logger.critical(f"CRITICAL: table_upload_modes.yml not found at {upload_modes_path}. Cannot determine upload strategies.")
    raise FileNotFoundError(f"Configuration file missing: {upload_modes_path}")
except Exception as error:
    logger.critical(f"CRITICAL: Error loading table_upload_modes.yml: {error}")
    raise

class ETLProcessor:
    """
    A class to encapsulate and manage various ETL processes for API Football data.
    It reads load strategies from table_upload_modes.yml.
    """

    def __init__(
            self,
            bucket_name: str,
            credentials_path: str
        ):
        """
        Initializes the ETLProcessor with Google Cloud Storage configuration.

        Args:
            bucket_name (str): The name of the GCS bucket for raw data.
            credentials_path (str): The path to the GCP service account key file.
        """
        self.bucket_name = bucket_name
        self.credentials_path = credentials_path
        self._validate_config()

    def _validate_config(self):
        """Validates that necessary configuration parameters are set."""
        if not self.bucket_name:
            logger.critical("GCP_RAW_BUCKET (bucket_name) is not configured in ETLProcessor.")
            raise ValueError("GCP_RAW_BUCKET is not configured.")
        if not self.credentials_path or not os.path.exists(self.credentials_path):
            logger.critical(f"GCP_CREDENTIALS_PATH (credentials_path) is not set or file not found at '{self.credentials_path}'.")
            raise ValueError("GCP_CREDENTIALS_PATH is not configured or invalid.")
        if not UPLOAD_MODES:
            logger.critical("UPLOAD_MODES is empty. table_upload_modes.yml might be empty or unreadable.")
            raise ValueError("UPLOAD_MODES configuration is missing or empty.")
        logger.info("ETLProcessor initialized and configuration validated.")

    def _perform_core_etl_steps(
        self,
        table_name: str,
        stage: Literal["raw", "processed"],
        load_strategy: Literal["overwrite", "append"],
        querystring_params: Dict[str, Any],
        # partition_keys: Optional[Dict[str, Any]] = None,
        **context: Any
    ) -> Optional[str]:
        """
        Contains the core ETL logic (Extract, Transform, Load) that is shared
        between different loading strategies.
        """
        logger.info(f"Executing core ETL for table: '{table_name}' (Strategy: '{load_strategy}') with params: {querystring_params}")

        # 1. Initialize the Transformer (which internally calls the Extractor)
        if APIFootballTransformer is None:
            logger.critical("APIFootballTransformer class not available. Cannot perform ETL.")
            raise RuntimeError("APIFootballTransformer not imported.")

        try:
            logger.info(f"Step 1: Initializing APIFootballTransformer for table '{table_name}'.")
            transformer = APIFootballTransformer(
                table_name=table_name,
                **querystring_params
            )
            transformed_df = transformer.transform()
            logger.info(f"Transformer completed. Records found: {len(transformed_df) if not transformed_df.empty else 0}.")

        except Exception as error:
            logger.critical(f"Error during Extraction or Transformation for table '{table_name}': {error}", exc_info=True)
            raise

        if transformed_df.empty:
            logger.warning(f"No data returned from transformer for '{table_name}'. Skipping GCS load.")
            return None

        # 2. Initialize the Loader
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
                # partition_keys=partition_keys
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
        # partition_keys is optional, only needed for 'append' strategy
        # partition_keys: Optional[Dict[str, Any]] = None,
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
    GCP_CREDENTIALS_PATH_TEST = os.getenv("GCP_CREDENTIALS_PATH")

    if not (GCP_RAW_BUCKET_TEST and GCP_CREDENTIALS_PATH_TEST and os.path.exists(str(GCP_CREDENTIALS_PATH_TEST) if GCP_CREDENTIALS_PATH_TEST else '')):
        logger.error("GCP_RAW_BUCKET or GCP_CREDENTIALS_PATH not set for local testing. Skipping tests.")
        exit(1) # Exit if configuration is missing for tests

    # Initialize the ETL Processor instance
    try:
        etl_processor = ETLProcessor(
            bucket_name=GCP_RAW_BUCKET_TEST,
            credentials_path=GCP_CREDENTIALS_PATH_TEST
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


    