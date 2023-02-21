
-- CREATE OR REPLACE TABLE `dez-2023.trips_data_all.rides_fhv_tripdata_nonpartitioned`
--(
--    dispatching_base_num	STRING,
--    pickup_datetime	    DATETIME,
--    dropOff_datetime	    DATETIME,
--    PUlocationID	        STRING,
--    DOlocationID	        STRING,
--    SR_Flag	                STRING,
--    Affiliated_base_number	STRING
--) AS
--SELECT
--       dispatching_base_num, 
       CAST(pickup_datetime AS DATETIME) AS XXX, dropOff_datetime AS DATETIME) AS XXX,
--       CAST(PULocationID AS STRING) AS PULocationID,
--       CAST(DOLocationID AS STRING) AS DOLocationID,
--       CAST(SR_Flag AS STRING) AS SR_Flag,
--       CAST(Affiliated_base_number AS STRING) AS Affiliated_base_number
-- FROM `dez-2023.trips_data_all.rides_fhv_tripdata`;

