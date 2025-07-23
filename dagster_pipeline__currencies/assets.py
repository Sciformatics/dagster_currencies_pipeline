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
    description="The raw extract from the CoinLore crypto rates API."

)
def stg_source__crypto_rates():

    retries = 0
    while retries < 3:
        try:
            response = requests.get('https://api.coinlore.net/api/tickers/?start=0&limit=100').json()
            response_df = pd.json_normalize(response['data'])
            print(response_df.head())
            response_df['api_call_at'] = datetime.now(pytz.timezone("Etc/GMT"))
            response_df.to_sql('stg_source__crypto_rates', ENGINE, if_exists='append', index=False)
            print(f'{datetime.now(pytz.timezone("Etc/GMT"))}: Loaded to database successfully.')
            retries+=3
        except Exception as e:
            print(f'Error: {e}')
            retries +=1

@asset(
    description="Exchange rates - GBP base"
)
def stg_source__exchange_rate_from_gbp():
    
    try:
        response = requests.get('https://api.frankfurter.dev/v1/latest?base=GBP').json()
        response_df = pd.json_normalize(response)
        response_df.to_csv('stg_source__exchange_rate_from_gbp', ENGINE, if_exists='append', index=False)
    except Exception as e:
        print(f'Error: {e}')

@asset(
    description="Exchange rates - GBP base"
)
def stg_source__exchange_rate_from_usd():
    
    try:
        response = requests.get('https://api.frankfurter.dev/v1/latest?base=USD').json()
        response_df = pd.json_normalize(response)
        response_df.to_csv('stg_source__exchange_rate_from_usd', ENGINE, if_exists='append', index=False)
    except Exception as e:
        print(f'Error: {e}')

####### COMPLETE

@asset(
    deps=["stg_source__exchange_rate_from_gbp"],
    description="convert crypto "
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