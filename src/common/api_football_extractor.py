import requests
import os
import logging
from dotenv import load_dotenv
from typing import Literal, Dict, Any, List, Optional

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIFootballExtractor:
    """
    Extract class for fetch API Football data
    """
    _ALLOWED_QUERY_PARAMS: Dict[str, List[str]] = {
        "countries": ["name", "code", "search"],
        "teams": ["id", "name", "league", "season", "country", "search", "code", "venue"]
    }

    def __init__(
        self,
        env_path: str = "src/config/.env"
    ):
        # Load configs
        load_dotenv(dotenv_path=env_path)

        self.api_key = os.getenv("API_FOOTBALL_KEY")
        self.api_host = os.getenv("API_FOOTBALL_HOST")
        self.api_url = os.getenv("URL_PATH")

        if not all ([self.api_key, self.api_host, self.api_url]):
            logger.error("API credentials or URL not loaded. Please check environment setup.")
            raise ValueError("API credentials or URL are missing.")
        
        self.session = requests.Session()
        self.session.headers.update({
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.api_host
        })

        logger.info("APIFootballExtractor initialized.")

    def _validate_querystring_params(
        self,
        table_name: str,
        querystring: Dict[str, Any]
    ) -> None:
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
            table_name: Literal["countries", "teams"],
            **querystring: Any
        ) -> Dict[str, Any]:
        
        """
        Fetch data from a specific table with optional queristring parameters.

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

        url = f"{self.api_url}{table_name}"

        logging.info("Starting fetching data from table %s...", table_name)

        try:
            response = self.session.get(url, params=querystring)

            logging.info("Full URL requested: %s", response.url)
            response.raise_for_status()
            logging.info("Data fetched sucessfully for %s!", table_name)

            return response.json()
        
        except requests.exceptions.RequestException as error:
            logger.error("Error during API request for %s: %s", table_name, error)
            raise

        except ValueError as error:
            logger.error("Failed to decode JSON response for %s: %s", table_name, error)
            raise

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    try:
        extractor = APIFootballExtractor()
        TEST_TABLE_NAME = "teams"
        QUERYSTRING = {"country":"Brazil"}

        print("\n--- API Response (first 2 items in 'response' key) ---")
        DATA = extractor.extract_table(TEST_TABLE_NAME, **QUERYSTRING)
        print(DATA.get("response", [])[:2])

        print("\n--- Example without querystring ---")
        countries_all_data = extractor.extract_table(table_name="countries")
        print(countries_all_data.get("response", [])[:2])

    except Exception as e:
        logger.error("An error occurred during extraction test: %s", e)