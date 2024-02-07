docker run -it \
      -e POSTGRES_USER=root \
      -e POSTGRES_PASSWORD=root \ 
      -e POSTGRES_DB=ny_taxi \ 
      -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgressql/data \
      -p 5432:5432 \
postgres:13

# network docker

docker network create pg-network


docker run -it \
   -e POSTGRES_USER=root \
   -e POSTGRES_PASSWORD=root \
   -e POSTGRES_DB=ny_taxi \
   -v /home/mzfuadi/zoomcamp_dataengineering2024/ZoomCamp2024/Week_1/ny_taxi_postgres_data:/var/lib/postgressql/data \
   -p 5432:5432 \
   --network=pg-network \
   --name pg-database \
   postgres:13


docker run -it \
  -e PGADMIN_DEFAULT_EMAIL=admin@admin.com \
  -e PGADMIN_DEFAULT_PASSWORD=root \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4 

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

python3 ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_data --url=${URL}

URL="http://172.24.128.1:8000/ny_taxi_postgres_data/yellow_tripdata_2021-01.parquet"

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
docker run -it \
    --network=pg-network \
  taxi_ingest:v100 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_data \
    --url=${URL}


--- Ingest data using script ---
python3 ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

    --- Run the built docker file ---
docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pg-database \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_trips \
        --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

--- Ingest zones data using script ---
python3 ingest_zones.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zones \
    --url="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"