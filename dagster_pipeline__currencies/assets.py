
from dagster import asset
from constants import POSTGRES_PATH
import requests, psycopg2
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd


engine = create_engine(POSTGRES_PATH)

@asset(
    description="The raw extract from the CoinCap Rates API."
)
def all_currencies() -> None:

    response = requests.get('https://api.coincap.io/v2/rates').json()
    response_df = pd.json_normalize(response['data'])
    response_df['api_call_at'] = datetime.now()
    response_df.to_sql('all_currencies', engine, if_exists='append', index=False)

@asset(
    deps=["all_currencies"],
    description="A subset of the all_currencies table showing only crypto currencies"
)
def crypto_currencies():

    query = '''

        SELECT * FROM ALL_CURRENCIES
        WHERE TYPE ILIKE 'CRYPTO';
    
    '''
    crypto_df = pd.read_sql(query, engine)
    crypto_df.to_sql('crypto_currencies', engine, index=False)


@asset(
    deps=["all_currencies"],
    description="A subset of the all_currencies table showing only fiat currencies"
)
def fiat_currencies():

    query = '''

        SELECT * FROM ALL_CURRENCIES
        WHERE TYPE ILIKE 'FIAT';
    
    '''
    crypto_df = pd.read_sql(query, engine)
    crypto_df.to_sql('fiat_currencies', engine, index=False)
