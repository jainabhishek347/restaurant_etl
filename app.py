import sys
import logging
import pandas as pd

import psycopg2
import numpy as np
import psycopg2.extras as extras

from etl_scripts import extract, load_db, load_basket, table_script
from utils.db_utils import get_db_connection

LOGGER = logging.getLogger()

FIELDNAMES =['timestamp','store','customer_name',
             'basket_items','total_price','cash_or_card','card_number']

FILENAME = 'chesterfield.csv'

DATA = pd.read_csv(FILENAME, names = FIELDNAMES)

def csv_to_df():
    LOGGER.info('Creating Products DataFrame!')
    products_df = extract.create_products_df(DATA)

    LOGGER.info('Creating Customer DataFrame!')
    customer_df = extract.unique_customers_table(DATA)

    LOGGER.info('Creating Store DataFrame!')
    store_df = pd.DataFrame(DATA['store'].unique(), columns=['store'])

    return products_df, customer_df, store_df

def handler(event, context):
    conn, cursor = get_db_connection()

    LOGGER.info('Creating tables in database if tables not exists!')
    tables = table_script.db_create_tables(conn, cursor)

    LOGGER.info('Creating and Fetching Data from DataFrames!')
    products_df, customer_df, store_df = csv_to_df()
    basket_df = load_basket.create_basket_df(conn, cursor, DATA)

    LOGGER.info('Inserting Data into Database..')
    load_basket.execute_values(conn, basket_df, 'basket_df')
    load_db.execute_values(conn, customer_df, 'customer_df')
    load_db.execute_values(conn, products_df, 'products_df')
    load_db.execute_values(conn, store_df, 'store_df')

    context = {
        'status' : 200,
        'message' : 'Success',
    }
    return context


# handler('' , '')

