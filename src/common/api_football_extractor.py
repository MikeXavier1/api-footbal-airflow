import requests
import os
import logging
from dotenv import load_dotenv
from typing import Literal, Dict, Any, List

# Load configs
load_dotenv(dotenv_path="src/config/.env")
API_KEY = os.getenv("API_FOOTBALL_KEY")
API_HOST = os.getenv("API_FOOTBALL_HOST")
API_URL = os.getenv("URL_PATH")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIFootballExtractor:
    """
    Extract class for fetch API Football data
    """
    _ALLOWED_QUERY_PARAMS: Dict[str, List[str]] = {
        "countries": ["name", "code", "search"],
    }

    def __init__(self):
        if not all ([API_KEY, API_HOST, API_URL]):
            logger.error("API credentials or URL not loaded. Plese check environment setup.")
            raise ValueError("API credentials or URL are missing.")
        self.headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }
        logger.info("APIFootballExtractor initialized.")

    def _validate_querystring_params(self, table_name: str, querystring: Dict[str, Any]):
        """
        Validates the provided querystring parameters against the allowed ones for the given table.

        Args:
            table_name (str): The name of the table/endpoint.
            querystring (Dict): The dictionary of querystring parameters to validate.

        Raises:
            ValueError: If an invalid or unexpected parameter is found.
        """
        allowed_params = self._ALLOWED_QUERY_PARAMS.get(table_name)

        if allowed_params is None:
            raise ValueError(
                f"Table '{table_name}' is not supported for query parameter validation."
            )

        for param_key in querystring.keys():
            if param_key not in allowed_params:
                error_msg = (
                    f"Invalid querystring parameter '{param_key}' for table '{table_name}'. "
                    f"Allowed parameters are: {', '.join(allowed_params)}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
        logger.debug("Querystring parameters validated successfully for table '%s'.", table_name)



    def extract_table(
            self,
            table_name: Literal["countries"],
            **querystring: Dict
        ) -> Dict[str, Any]:
        
        """
        Fetch data from a specific table with optional queristrin parameters.

        Args:
            table_name (Literal): The table name that will be extracted.
            **querystring (Dict): Optional parameters for the URL querystring.
        
        Returns:
            Dict: The JSON object returned by the API.
        Raises:
            requests.exceptions.RequestException: If there's an HTTP request error.
        """

        # Perform validation BEFORE making the request
        self._validate_querystring_params(table_name, querystring)

        url = API_URL + table_name

        logging.info("Starting fetching data from table %s...", table_name)

        try:
            response = requests.get(url, headers=self.headers, params=querystring)

            logging.info("Full URL requested: %s", response.url)
            response.raise_for_status()
            logging.info("Fetched data complete for %s!", table_name)

            return response.json()
        
        except requests.exceptions.RequestException as error:
            logger.error("Error during API request for %s: %s", table_name, error)
            raise

        except ValueError as error:
            logger.error("Failed to decode JSON response for %s: %s", table_name, error)
            raise

if __name__ == "__main__":
    extractor = APIFootballExtractor()
    try:
        TEST_TABLE_NAME = "countries"
        QUERYSTRING = {"search":"bra"}
        DATA = extractor.extract_table(TEST_TABLE_NAME, **QUERYSTRING)
        print("\n--- API Response (first 2 items in 'response' key) ---")
        print(DATA.get("response", [])[:2])

        print("\n--- Example without querystring ---")
        countries_all_data = extractor.extract_table(table_name="countries")
        print(countries_all_data.get("response", [])[:2])

    except Exception as e:
        logger.error("An error occurred during extraction test: %s", e)