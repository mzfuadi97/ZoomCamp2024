CREATE OR REPLACE EXTERNAL TABLE `secret-meridian-414302.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoomcamp-project-mzfuadi97/nyc_taxi_data/lpep_pickup_date=*']
);

SELECT * FROM `secret-meridian-414302.ny_taxi.external_green_tripdata` limit 5;

-- Create Non Partitioned Table from external table
CREATE OR REPLACE TABLE secret-meridian-414302.ny_taxi.external_green_tripdata_non_partitioned as 
SELECT * FROM `secret-meridian-414302.ny_taxi.external_green_tripdata`;

-- Create Partitioned Table from external table
CREATE OR REPLACE TABLE secret-meridian-414302.ny_taxi.external_green_tripdata_partitioned
PARTITION BY
  DATE(lpep_pickup_datetime) as 
SELECT * FROM `secret-meridian-414302.ny_taxi.external_green_tripdata`;

-- Create Partitioned + Clustered from External Table
CREATE OR REPLACE TABLE secret-meridian-414302.ny_taxi.external_green_partitoned_clustered
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY 
  PUlocationID as 
SELECT * FROM `secret-meridian-414302.ny_taxi.external_green_tripdata`;

SELECT DISTINCT PULocationID
FROM `secret-meridian-414302.ny_taxi.external_green_partitoned_clustered`
WHERE 1=1
AND lpep_pickup_datetime >= '2022-06-01'
AND lpep_pickup_datetime <= '2022-06-30';