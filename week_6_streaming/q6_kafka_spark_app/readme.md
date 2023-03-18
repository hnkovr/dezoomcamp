# Question 6:
Implement a streaming application, 
* for finding out popularity of PUlocationID across green and fhv trip datasets.

* Use the datasets fhv_tripdata_2019-01.csv.gz and green_tripdata_2019-01.csv.gz

## Code should include following

* Producer 
  * that reads csv files 
  * and publish rides in corresponding kafka topics (such as rides_green, rides_fhv)
* Pyspark-streaming-application
  * that reads two kafka topics 
  * and writes both of them in topic rides_all
  * and apply aggregations to find most popular pickup location.
