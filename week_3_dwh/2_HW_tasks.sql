-- for: https://docs.google.com/forms/d/e/1FAIpQLSeFHAi51wndRZAL3v570zoSVQR52X_VFmh2ImAzLatP6A4DWA/closedform
-- for: /Users/user/github.com/hnkovr/dezoomcamp/week_3_dwh/homework.md

-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `dez-2023.trips_data_all.rides_fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dez-2023/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- Question 1: What is count for fhv vehicle records for year 2019?
SELECT count(*) FROM `dez-2023.trips_data_all.rides_fhv_tripdata`;


-- Create nonpartitioned table
--CREATE OR REPLACE TABLE `dez-2023.trips_data_all.rides_fhv_tripdata__nonpartitioned`
--AS SELECT * FROM `dez-2023.trips_data_all.rides_fhv_tripdata`;
CREATE OR REPLACE TABLE `dez-2023.trips_data_all.rides_fhv_tripdata__nonpartitioned`
AS
SELECT
       dispatching_base_num,
       CAST(pickup_datetime AS DATETIME) AS pickup_datetime,
       CAST(dropOff_datetime AS DATETIME) AS dropOff_datetime,
       CAST(PULocationID AS INTEGER) AS PULocationID,
       CAST(DOLocationID AS INTEGER) AS DOLocationID,
       CAST(SR_Flag AS FLOAT) AS SR_Flag,
       CAST(Affiliated_base_number AS STRING) AS Affiliated_base_number
 FROM `dez-2023.trips_data_all.rides_fhv_tripdata`;


-- Question 2: What is the estimated amount of data that will be read when you execute your query on the External Table and the Materialized Table?
SELECT * FROM `dez-2023.trips_data_all.rides_fhv_tripdata`;
SELECT * FROM `dez-2023.trips_data_all.rides_fhv_tripdata__nonpartitioned`;


--Question 3:
--How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(*) FROM `dez-2023.trips_data_all.rides_fhv_tripdata__nonpartitioned` --__nonpartitioned`
where PUlocationID is null and DOlocationID is null;


-- Question 4:
--What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
CREATE OR REPLACE TABLE `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `dez-2023.trips_data_all.rides_fhv_tripdata__nonpartitioned`
);


--Question 5:
--Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
--Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM  `dez-2023.trips_data_all.rides_fhv_tripdata__nonpartitioned`
 WHERE CAST(dropoff_datetime AS TIMESTAMP) BETWEEN CAST('2019-01-01' AS TIMESTAMP) AND CAST('2019-03-31' AS TIMESTAMP);
--> This query will process 647.87 MB when run.
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned`
 WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31';
--> This query will process 385.71 MB when run.
