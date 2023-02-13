-- Create a external table in BQ using the fhv 2019 data (do not partition or cluster this table).
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://trips_data_lake_dtc-de-376314/data/fhv/fhv_tripdata_2019-*.parquet']
);

--Q1: What is the count for fhv vehicle records for year 2019?
SELECT COUNT(1)
FROM trips_data_all.external_fhv_tripdata
;
-- 43244696

--Q2: Create a non-partition table in BQ using the fhv 2019 data.
-- Write a query to count the distinct number of affiliated_base_number 
-- for the entire dataset on both the tables.
CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_non_partitoned AS
SELECT * FROM trips_data_all.external_fhv_tripdata;
-- error: Error while reading table: trips_data_all.external_fhv_tripdata, 
-- error message: Parquet column 'DOlocationID' has type INT64 which does not match the target cpp_type DOUBLE. File: gs://trips_data_lake_dtc-de-376314/data/fhv/fhv_tripdata_2019-05.parquet
-- Q: Why even the bug occur? What happen really? 
-- Inuition: schema data type doesn't meet the parquet(which is in gcs) data type
-- Solution?: fix the data type issue in data pipeline
-- use astype and the Int64 dtype (accept nan value)
-- Q: What is external table in bigquery? how is it work?
SELECT DISTINCT(Affiliated_base_number)
FROM trips_data_all.external_fhv_tripdata
;
-- expected: 0b vs 317.94 MB

SELECT DISTINCT(Affiliated_base_number)
FROM trips_data_all.fhv_tripdata_non_partitoned
;
-- expected: 317.94 MB vs  317.94 MB

--Q3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset
SELECT COUNT(*)
FROM trips_data_all.fhv_tripdata_non_partitoned
WHERE PUlocationID IS NULL 
    AND DOlocationID IS NULL 
;

-- 717748

-- Q4: 
-- Partition by pickup_datetime Cluster on affiliated_base_number but why?

--Q5: Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct 
-- affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata_partitoned
PARTITION BY
  DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT * FROM trips_data_all.external_fhv_tripdata;

SELECT DISTINCT(Affiliated_base_number)
FROM trips_data_all.fhv_tripdata_non_partitoned
WHERE 
    DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
;

SELECT DISTINCT(Affiliated_base_number)
FROM trips_data_all.fhv_tripdata_partitoned
WHERE 
    DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
;

-- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

-- Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be 
-- validated by running a count query on the External Table to check if any errors occur.