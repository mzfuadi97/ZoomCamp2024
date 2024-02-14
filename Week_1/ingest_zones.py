#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    # Download the csv file
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the csv to create a dataframe
    df = pd.read_csv(csv_name)

    # Create a database with table table_name
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Ingest data into the table
    t_start = time()
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()

    print('Total time taken: %.3f seconds' %(t_end - t_start))

if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the CSV file')

    args = parser.parse_args()

    main(args)