import pandas as pd
import logging
from typing import Any, Callable, Dict, Literal, TypeVar
from src.common.api_football_extractor import APIFootballExtractor

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type hinting
T = TypeVar('T', bound='APIFootballTransformer')

class APIFootballTransformer:
    """
    Transform fetched data from API Football by table name.
    """

    # TO VERIFY:
    # _TRANSFORM_METHODS: Dict[str, Callable[[T], pd.DataFrame]] = {}

    def __init__(
            self,
            table_name:Literal["countries"],
            **querystring: Dict[str, Any]
        ):
        """
        Initialize transformer with raw data fetched from API.
        """
        self.table_name = table_name
        self.querystring = querystring

        extractor = APIFootballExtractor()
        self.raw_data = extractor.extract_table(self.table_name, **self.querystring)
        self.extracted_data = self.raw_data.get('response', [])
        logger.info(
            "APIFootballTransformer initialized., Data records found: %d",
            len(self.extracted_data) if isinstance(self.extracted_data, list) else 0
        )

    def _convert_to_dataframe(
            self,
            data:Any
        ) -> pd.DataFrame:
        """
        Utility method to convert a list of dicts or a single dict into a pandas DataFrame.

        Args:
            data (Any): The data to convert, expected to be a list of dictionaries or a single dictionary.

        Returns:
            pd.DataFrame: The converted pandas DataFrame.
        """

        if isinstance(data, list) and all(isinstance(i, dict) for i in data):
            logger.debug("Converting list of dictionaries to DataFrame.")
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            logger.debug("Converting single dictionary to DataFrame.")
            return pd.DataFrame([data])
        else:
            logger.warning("Data is not a list of dictionaries or a single dictionary for" \
            " DataFrame conversion. Type: %s", type(data))
            return pd.DataFrame()

    def transform_countries(self) -> pd.DataFrame:
        """
        Transformation logic for 'countries' table data.
        """

        logger.info("Starting transformation for 'countries' table.")

        if not self.extracted_data:
            logger.warning("No data available for 'countries' transformation." \
            "Returning empty DataFrame.")

        df = self._convert_to_dataframe(self.extracted_data)

        if df.empty:
            logger.warning("DataFrame is empty after initial conversion for 'countries'.")
            return df

        df = df.rename(columns={
            'name': 'country_name',
            'code': 'country_code',
            'flag': 'country_flag_url'
        })

        df['country_name'] = df['country_name'].astype(str).str.strip()
        df['country_code'] = df['country_code'].astype(str).str.upper().str.strip()
        df['country_flag_url'] = df['country_flag_url'].astype(str).str.strip()

        logger.info("Finished transforming 'countries' data. DataFrame shape: %s", df.shape)

        return df
    
    def transform(self) -> pd.DataFrame:
        """
        Dispatches the raw data to the appropriate transformation method
        based on the table_name specified during initialization.

        Returns:
            pd.DataFrame: The transformed DataFrame.

        Raises:
            ValueError: If no specific transformation method is found for the given table_name.
        """
        # A more dynamic way to map methods using getattr
        method_name = f"transform_{self.table_name}"
        transformer_method = getattr(self, method_name, None)

        if transformer_method is None or not callable(transformer_method):
            logger.error("No specific transformation method found for table: '%s'. "
                         "Expected method: '%s'", self.table_name, method_name)
            raise ValueError(
                f"No specific transformation method found for table: '{self.table_name}'. "
                f"Please implement `transform_{self.table_name}`."
            )

        logger.info("Dispatching to transformation method: '%s'", method_name)
        return transformer_method()

# --- Test ---

if __name__ == "__main__":
    print("\n--- Testing Countries Transformation ---")
    try:
        countries_transformer = APIFootballTransformer(table_name="countries", search="bra")
        transformed_countries_df = countries_transformer.transform()
        print("\nTransformed Countries DataFrame Head:")
        print(transformed_countries_df.head())
        print("\nTransformed Countries DataFrame Info:")
        transformed_countries_df.info()
    except Exception as e:
        logger.error("Error during countries transformation test: %s", e)