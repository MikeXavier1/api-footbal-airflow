import requests
import os
import logging
from dotenv import load_dotenv
from typing import Literal, Dict, Any

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

    def __init__(self):
        if not all ([API_KEY, API_HOST, API_URL]):
            logger.error("API credentials or URL not loaded. Plese check environment setup.")
            raise ValueError("API credentials or URL are missing.")
        self.headers = {
            "x-rapidapi-key": API_KEY,
            "x-rapidapi-host": API_HOST
        }
        logger.info("APIFootballExtractor initialized.")


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