
SELECT count(*) FROM `dez-2023.trips_data_all.rides_fhv_tripdata`
where tpep_pickup_datetime   >= cast('2019-01-01' as timestamp)
  and tpep_dropoff_datetime < cast('2021-01-01' as timestamp)
-- Not found: Dataset dez-2023:trips_data_all was not found in location US


SELECT count(*) FROM `dez-2023.dezoomcamp.rides_fhv_tripdata`
where tpep_pickup_datetime   >= cast('2019-01-01' as timestamp)
  and tpep_dropoff_datetime < cast('2021-01-01' as timestamp)
-- Cannot read and write in different locations: source: europe-west6, destination: US


SELECT count(*) FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned`
where pickup_datetime   >= cast('2019-01-01' as datetime)
  and pickup_datetime < cast('2021-01-01' as datetime)
-- 43244696



SELECT count(*) FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned`
where pickup_datetime   >= cast('2019-01-01' as datetime)
  and pickup_datetime < cast('2021-01-01' as datetime)
-- 43244696


SELECT count(*) cnt,
        EXTRACT(MONTH FROM pickup_datetime)  as month_
FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned`
where pickup_datetime   >= cast('2019-01-01' as datetime)
  and pickup_datetime < cast('2020-01-01' as datetime)
group by 2
order by 1 desc


-- Create external table
CREATE OR REPLACE EXTERNAL TABLE `dez-2023.trips_data_all.taxi_zone_lookup`
OPTIONS (
  format = 'parquet',  --!
  uris = ['gs://dtc_data_lake_dez-2023/data/taxi_zone_lookup.csv']
);


WITH dim_zones AS (
    select
        locationid,
        borough,
        zone,
        replace(service_zone,'Boro','Green') as service_zone
    FROM `dez-2023.trips_data_all.taxi_zone_lookup`
)
SELECT COUNT(*)
FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned` AS fact_fhv_data
JOIN dim_zones ON dim_zones.locationid = fact_fhv_data.PULocationID
--DOLocationID
WHERE borough != 'Unknown';
--

--WITH dim_zones AS (
--    select
--        locationid,
--        borough,
--        zone,
--        replace(service_zone,'Boro','Green') as service_zone
--    FROM `dez-2023.trips_data_all.taxi_zone_lookup`
--)
--SELECT COUNT(*)
--FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned` AS fact_fhv_data
--JOIN dim_zones ON dim_zones.locationid = fact_fhv_data.DOLocationID
--WHERE borough != 'Unknown';



WITH dim_zones AS (
    select
        locationid,
        borough,
        zone,
        replace(service_zone,'Boro','Green') as service_zone
    FROM `dez-2023.trips_data_all.taxi_zone_lookup`
)
, fact_fhv_data AS (
    SELECT *
    FROM `dez-2023.trips_data_all.rides_fhv_tripdata__partitioned` AS fact_fhv_data
    JOIN dim_zones ON dim_zones.locationid = fact_fhv_data.PULocationID
    --DOLocationID
    WHERE borough != 'Unknown'
)
SELECT count(*) cnt,
       EXTRACT(MONTH FROM pickup_datetime) as month_
FROM fact_fhv_data
-- WHERE pickup_datetime >= cast('2019-01-01' as datetime)
--   AND pickup_datetime < cast('2021-01-01' as datetime)
GROUP BY 2
ORDER BY 1 DESC;
/*
20511716
1
2
380031
12
3
377929
10
4
365797
11
5
356780
8
6
336274
9
7
315232
7
8
304258
6
9
285470
5
10
278253
4
11
216311
3
12
208410
2
*/

