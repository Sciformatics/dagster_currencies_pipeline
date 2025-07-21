# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                       IMPORT MODULES
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from dagster import asset, asset_check, AssetCheckResult
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from constants import POSTGRES_PATH  # Set your own Postgres instance connection in your constants file
import pandas as pd
import psycopg2

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                       CONSTANTS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ENGINE = create_engine(POSTGRES_PATH)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                       ASSETS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@asset(
    description="The raw extract from the CoinCap Rates API."

)
def all_currencies():

    response = requests.get('https://api.coincap.io/v2/rates').json()
    response_df = pd.json_normalize(response['data'])
    response_df['api_call_at'] = datetime.now()
    response_df.to_sql('all_currencies', ENGINE, if_exists='append', index=False)

@asset(
    deps=["all_currencies"],
    description="A subset of the all_currencies table showing only crypto currencies"
)
def crypto_currencies():

    query = '''
        
        SELECT * FROM all_currencies
        WHERE TYPE ILIKE 'CRYPTO';
    
    '''
    crypto_df = pd.read_sql(query, ENGINE)
    crypto_df.to_sql('crypto_currencies', ENGINE, if_exists='replace', index=False)

@asset(
    deps=["all_currencies"],
    description="A subset of the all_currencies table showing only fiat currencies"
)
def fiat_currencies():

    query = '''
    
        SELECT * FROM all_currencies
        WHERE TYPE ILIKE 'FIAT';
    
    '''
    fiat_df = pd.read_sql(query, ENGINE)
    fiat_df.to_sql('fiat_currencies', ENGINE, if_exists='replace', index=False)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                       ASSET TESTS
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Checks for nulls in ID column

@asset_check(asset=all_currencies)
def test_for_nulls_in_currencies_df():
    query = '''
        SELECT * FROM all_currencies
        WHERE ID IS NOT NULL AND SYMBOL IS NOT NULL;'''
    check_for_nulls_df = pd.read_sql(query, ENGINE)
    number_of_nulls = check_for_nulls_df['id'].isna().sum()

    return AssetCheckResult(
        passed=bool(number_of_nulls == 0),
    )