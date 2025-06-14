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

    def __init__(
            self,
            table_name:Literal["countries", "teams", "leagues", "leagues/season"],
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
    
    @staticmethod
    def clean_float_value(value):
        try:
            value = float(str(value).strip())
            if np.isfinite(value):
                return int(value)
        except:
            pass
        return pd.NA

    def transform_countries(self) -> pd.DataFrame:
        """
        Transformation logic for 'countries' table data.
        """

        logger.info("Starting transformation for 'countries' table.")

        if not self.extracted_data:
            logger.warning("No data available for 'countries' transformation." \
            "Returning empty DataFrame.")
            return pd.DataFrame()

        df = self._convert_to_dataframe(self.extracted_data)

        if df.empty:
            logger.warning("DataFrame is empty after initial conversion for 'countries'.")
            return df

        df = df.rename(columns={
            'name': 'country_name',
            'code': 'country_code',
            'flag': 'country_flag_url'
        }).copy()

        df['country_name'] = df['country_name'].astype(str).str.strip()
        df['country_code'] = df['country_code'].astype(str).str.upper().str.strip()
        df['country_flag_url'] = df['country_flag_url'].astype(str).str.strip()

        logger.info("Finished transforming 'countries' data. DataFrame shape: %s", df.shape)

        return df

    def transform_teams(self) -> pd.DataFrame:
        """
        Transformation logic for 'teams' table data.
        """

        logger.info("Starting transformation for 'teams' table.")

        if not self.extracted_data:
            logger.warning("No data available for 'teams' transformation." \
            "Returning empty DataFrame.")
            return pd.DataFrame()

        teams = self.extracted_data

        if len(teams) == 0:
            logger.warning("DataFrame is empty after initial conversion for 'teams'.")
            return pd.DataFrame()

        records = []

        for team in teams:
            team_id = team["team"]["id"]
            venue_id = team["venue"]["id"]
            team_name = team["team"]["name"]
            team_founded = team["team"]["founded"]
            is_national = team["team"]["national"]
            team_logo = team["team"]["logo"]

            record = {
                'team_id': team_id,
                'venue_id': venue_id,
                'team_name': team_name,
                'team_founded': team_founded,
                'is_national': is_national,
                'team_logo': team_logo,
            }

            records.append(record)

        df = self._convert_to_dataframe(records)

        df['team_id'] = df['team_id'].astype(str).str.strip()
        df['venue_id'] = df['venue_id'].astype(str).str.strip().apply(self.clean_float_value).astype('Int64')
        df['team_name'] = df['team_name'].astype(str).str.strip()
        df['team_founded'] = df['team_founded'].astype(str).str.strip().apply(self.clean_float_value).astype('Int64')
        df['is_national'] = df['is_national'].astype(str).str.strip()
        df['team_logo'] = df['team_logo'].astype(str).str.strip()

        return df

    def transform_leagues(self) -> pd.DataFrame:
        """
        Transformation logic for 'leagues' table data.
        """

        logger.info("Starting transformation for 'leagues' table.")

        if not self.extracted_data:
            logger.warning("No data available for 'leagues' transformation." \
            "Returning empty DataFrame.")
            return pd.DataFrame()

        leagues = self.extracted_data

        if len(leagues) == 0:
            logger.warning("DataFrame is empty after initial conversion for 'leagues'.")
            return pd.DataFrame()

        records = []

        for league in leagues:
            league_id = league["league"]["id"]
            league_name = league["league"]["name"]
            league_type = league["league"]["type"]
            league_logo = league["league"]["logo"]
            country_code = league["country"]["code"]

            record = {
                'league_id': league_id,
                'league_name': league_name,
                'league_type': league_type,
                'league_logo': league_logo,
                'country_code': country_code
            }

            records.append(record)

        df = self._convert_to_dataframe(records)

        df['league_id'] = df['league_id'].astype(str).str.strip()
        df['league_name'] = df['league_name'].astype(str).str.strip()
        df['league_type'] = df['league_type'].astype(str).str.strip()
        df['league_logo'] = df['league_logo'].astype(str).str.strip()
        df['country_code'] = df['country_code'].astype(str).str.strip()

        return df

    def transform_leagues_seasons(self) -> pd.DataFrame:
        """
        Transformation logic for 'leagues' table data.
        """

        logger.info("Starting transformation for 'leagues/seasons' table.")

        if not self.extracted_data:
            logger.warning("No data available for 'leagues/seasons' transformation." \
            "Returning empty DataFrame.")
            return pd.DataFrame()

        seasons = self.extracted_data

        records = [{'season': s} for s in seasons]

        if len(records) == 0:
            logger.warning("DataFrame is empty after initial conversion for 'leagues/season'.")
            return pd.DataFrame()

        df = self._convert_to_dataframe(records)
        
        df['season'] = df['season'].astype('Int64')

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
        method_name = f"transform_{self.table_name.replace('/', '_')}"
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
    print("\n--- Testing seasons Transformation ---")
    
    try:
        teams_transformer = APIFootballTransformer(table_name="leagues/seasons",)
        transformed_teams_df = teams_transformer.transform()

        print("\nTransformed seasons DataFrame Head:")
        print(transformed_teams_df.head())

        print("\nTransformed seasons DataFrame Info:")
        transformed_teams_df.info()

    except Exception as error:
        logger.error("Error during seasons transformation test: %s", error)