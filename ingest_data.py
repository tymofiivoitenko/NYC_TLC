import argparse
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
    file_url = params.file_url

    # TODO: download file from some storage, instead of keeping it in resources

    # Prepare sql engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    # Define table
    # TODO - Define table via flyway or something
    df = pd.read_csv(file_url)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql('yellow_taxi_data', con=engine, if_exists='replace')

    # Extract and load data
    df_iter = pd.read_csv(file_url, iterator=True, chunksize=100000)
    while True:
        time_start = time()

        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(table_name, con=engine, if_exists='append')

        print("Inserted bunch of data, took: %.2f seconds" % (time() - time_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    parser.add_argument('--user')
    parser.add_argument('--password')
    parser.add_argument('--host')
    parser.add_argument('--port')
    parser.add_argument('--db')
    parser.add_argument('--table_name')
    parser.add_argument('--file_url')

    args = parser.parse_args()
    main(args)
