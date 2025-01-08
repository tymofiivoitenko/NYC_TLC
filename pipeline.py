import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from time import time

# Prepare sql engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

# Define table (TODO - should be done via flyway or something)
df = pd.read_csv('yellow_tripdata_2021-01.csv')
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
df.head(n=0).to_sql('yellow_taxi_data', con=engine, if_exists='replace')

# Extract and load data
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
while True:
    time_start = time()

    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql('yellow_taxi_data', con=engine, if_exists='append')

    print("Inserted bunch of data, time taken:", time() - time_start)
