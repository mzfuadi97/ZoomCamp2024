import os
import argparse
import gzip
from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    output_folder = 'ny_taxi_postgres_data'
    csv_name = 'output.csv.gz'

    # Download the gzipped CSV file
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read a compressed file
    with gzip.open(csv_name, 'rt') as file:
        df_iter = pd.read_csv(file, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        # Create a database with table table_name
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        # Ingest data into the table
        df.to_sql(name=table_name, con=engine, if_exists='append')

        while True:
            try:
                t_start = time()

                df = next(df_iter)

                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))
            except StopIteration:
                print('Data ingestion completed')
                break


if __name__ == '__main__':
    user = "postgres"
    password = "admin"
    host = "localhost"
    port = "5433"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_urls = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

