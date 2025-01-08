# New Your Taxi Data Engineering project

Project to build ETL from csv files to SQL




#### Important CLI commands:

docker build -t taxi_ingest:v001 .

docker run -it \                              
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

docker run -it taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=host.docker.internal \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --file_url='yellow_tripdata_2021-01.csv'

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --file_url='resources/yellow_tripdata_2021-01.csv'
