import pandas as pd
import logging
from api_football_extractor import APIFootballExtractor

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_data(table_name, **querystring):
    extractor = APIFootballExtractor()
    data = extractor.extract_table(table_name=table_name, **querystring)
    response_data = data.get('response')
    df = pd.DataFrame(response_data)

    df = df.rename(columns={
        'name': 'country_name',
        'code': 'country_code',
        'flag': 'country_flag_url'
    })

    df['country_name'] = df['country_name'].astype(str).str.strip()
    df['country_code'] = df['country_code'].astype(str).str.upper().str.strip()
    df['country_flag_url'] = df['country_flag_url'].astype(str).str.strip()

    return df

# Manual Test

if __name__ == "__main__":
    TABLE_NAME_TEST = 'countries'
    QUERYSTRING = {"search":"bra"}
    df = transform_data(table_name=TABLE_NAME_TEST, **QUERYSTRING)
    print(f"First rows:\n {df.head()}")