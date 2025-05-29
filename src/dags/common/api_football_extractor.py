import requests
import os
import pandas as pd
import logging
from dotenv import load_dotenv
from typing import Literal, Dict

# Load configs
load_dotenv(dotenv_path="src/config/.env")
API_KEY = os.getenv("API_FOOTBALL_KEY")
API_HOST = os.getenv("API_FOOTBALL_HOST")
API_URL = os.getenv("URL_PATH")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_table(
        table_name: Literal["countries"],
        **querystring: Dict
    ) -> Dict:

    """
    Fetch data from a specific table with optional queristrin parameters.

    ## Args:
        table_name (Literal): The table name that will be extracted.
        **querystring (Dict): Optional parameters for the URL querystring.
    """

    url = API_URL + table_name

    logging.info("Starting fetching data from table %s...", table_name)

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST
    }

    response = requests.get(url, headers=headers, params=querystring)
    logger.info("Full URL requested: %s", response.url)
    logging.info("Fetched data complete for %s!", table_name)

    return response.json()

## Manual test only

if __name__ == "__main__":
    TEST_TABLE_NAME = "countries"
    QUERYSTRING = {"search":"bra"}
    DATA = extract_table(TEST_TABLE_NAME, **QUERYSTRING)
    print(DATA)