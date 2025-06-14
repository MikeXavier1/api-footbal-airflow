import os
import pandas as pd
import logging
import yaml
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
from typing import Literal

# Load configs
load_dotenv(dotenv_path="src/config/.env")
GCP_RAW_BUCKET = os.getenv("GCP_RAW_BUCKET")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Load upload mode config
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'table_upload_modes.yml')
with open(config_path, "r") as f:
    UPLOAD_MODES = yaml.safe_load(f)

# Load GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.debug("Upload modes loaded: %s", UPLOAD_MODES)

class APIFootballLoader:
    """
    Upload transformed data into Google Cloud Storage
    """

    def __init__(
            self,
            bucket_name: str,
            credentials_path: str
    ):
        """
        Initializes the GCS client and bucket.

        Args:
            bucket_name (str): The name of the GCS bucket to load data into.
            credentials_path (str): The path to the GCP service account JSON key file.
        """
        if not bucket_name:
            logger.error("GCS bucket name is not provided.")
            raise ValueError("GCS bucket name is required for APIFootballLoader.")
        if not credentials_path or not os.path.exists(credentials_path):
            logger.error("GCP credentials file not found at: %s", credentials_path)
            raise ValueError(f"GCP credentials file not found at: {credentials_path}")

        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            # Ensure project_id is available from credentials or explicitly set if needed
            self.storage_client = storage.Client(
                credentials=self.credentials,
                project=self.credentials.project_id
            )
            self.bucket = self.storage_client.bucket(bucket_name)
            logger.info("APIFootballLoader initialized for bucket '%s'.", bucket_name)
        except Exception as e:
            logger.error("Failed to initialize GCS client: %s", e)
            raise

    def load_dataframe(
            self,
            table_name: str,
            df: pd.DataFrame,
            stage: Literal["raw"]
    ):
        """
        Loads a Pandas DataFrame into a Parquet file in GCS.

        The file path in GCS will be structured as:
        <stage>/<table_name>_<timestamp>.parquet

        Args:
            df (pd.DataFrame): The DataFrame to be loaded.
            table_name (str): The name of the table (e.g., "countries", "teams").
            stage (Literal["raw"]): The stage of the data (e.g., "raw").

        Returns:
            str: The full path of the uploaded file in GCS.

        Raises:
            Exception: If the upload operation fails.
        """

        if df.empty:
            logger.warning("Attempted to load an empty DataFrame for table" \
            " '%s' to GCS. No file will be uploaded.", table_name)
            return "No file uploaded (empty DataFrame)"
        
        mode = UPLOAD_MODES.get(table_name, "append")
        if mode not in ["overwrite", "append"]:
            logger.warning("Invalid upload mode '%s' for table '%s'. Defaulting to 'append'.",
                mode, table_name
            )
            mode = "append"
        logger.info("Upload mode for table '%s': %s", table_name, mode)
        
        now_timestamp = datetime.now(timezone(timedelta(hours=-3))).strftime('%Y%m%d_%H%M%S')

        file_path = f"{stage}/{table_name}_{now_timestamp}.parquet"

        temp_file_name = f"{table_name}_{now_timestamp}.parquet"
        temp_path = os.path.join('/tmp', temp_file_name)

        os.makedirs(os.path.dirname(temp_path), exist_ok=True)

        try:
            if mode == "overwrite":
                prefix = f"{stage}/{table_name}_"
                blobs_to_delete = list(self.bucket.list_blobs(prefix=prefix))
                if blobs_to_delete:
                    logger.info("Deleting %d existing files in GCS for overwrite mode...",
                        len(blobs_to_delete)
                    )
                    for blob in blobs_to_delete:
                        blob.delete()
                        logger.debug("Deleted blob: %s", blob.name)
                else:
                    logger.info("No existing files found for overwrite.")

            logger.info("Saving DataFrame to temporary local path: %s", temp_path)
            df.to_parquet(temp_path, index=False)
            logger.info("Successfully saved DataFrame to local Parquet.")

            blob = self.bucket.blob(file_path)
            logger.info("Uploading '%s' data to GCS path: gs://%s/%s",
                        table_name,
                        self.bucket.name,
                        file_path
            )
            blob.upload_from_filename(temp_path)
            logger.info("Successfully uploaded %s data to GCS bucket: gs://%s/%s",
                        table_name,
                        self.bucket.name,
                        file_path
            )

            return f"gs://{self.bucket.name}/{file_path}"
        
        # Re-raise the exception after logging
        except Exception as error:
            logger.error("Failed to upload data for table '%s' to GCS: %s", table_name, error)
            raise 

        finally:
            # Clean up the temporary local file
            if os.path.exists(temp_path):
                os.remove(temp_path)
                logger.debug("Removed temporary local file: %s", temp_path)

# --- Test Section ---
if __name__ == "__main__":           
    from api_football_transformer import APIFootballTransformer

    if not (GCP_RAW_BUCKET and GOOGLE_APPLICATION_CREDENTIALS):
        logger.error("GCP_RAW_BUCKET or GOOGLE_APPLICATION_CREDENTIALS are not set. "
                     "Please check your src/config/.env file.")
        exit(1)

    print("\n--- Testing APIFootballLoader ---")
    try:
        # 1. Initialize the Extractor and Transformer
        test_table_name = "teams"
        test_querystring = {"country": "Brazil"} # Example: Brazil

        logger.info("Step 1: Extracting and Transforming data...")
        transformer = APIFootballTransformer(table_name=test_table_name, **test_querystring)
        transformed_df = transformer.transform()

        if transformed_df.empty:
            logger.warning("No data returned from transformer for %s. Skipping load test.", test_table_name)
        else:
            print(f"\nTransformed DataFrame for {test_table_name} (Head):")
            print(transformed_df.head())
            print(f"DataFrame Shape: {transformed_df.shape}")

            # 2. Initialize the Loader
            logger.info("Step 2: Initializing APIFootballLoader...")
            loader = APIFootballLoader(
                bucket_name=GCP_RAW_BUCKET,
                credentials_path=GOOGLE_APPLICATION_CREDENTIALS
            )

            # 3. Load the transformed DataFrame
            logger.info("Step 3: Loading transformed data to GCS...")
            uploaded_path = loader.load_dataframe(
                df=transformed_df,
                table_name=test_table_name,
                stage="raw" # Or "processed", depending on your design
            )
            logger.info("Data successfully loaded. GCS path: %s", uploaded_path)
            logger.info(f"\nSuccessfully uploaded data for {test_table_name} to {uploaded_path}")

    except Exception as e:
        logger.critical("An error occurred during the full ETL test: %s", e, exc_info=True)
