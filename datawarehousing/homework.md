# Yellow Taxis Datawarehousing

## Setup
1. Download parquet files from 2024-01 up to 2024-06 from [Ny Taxis Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
2. Upload them to GCP bucket (ny-yellow-taxis)
3. Create external tables in BigQuery
    1. Grant access as BigQuery Admin in IAM
    2. Create the dataset nytaxis
    ```sql
    CREATE SCHEMA `ny-taxis-datawarehousing.nytaxi`
    OPTIONS (
    location = 'US' -- ou a região correta do seu bucket
    );
    ```
    3. Create external table
    ```sql
    -- Creating external table referring to gcs path
    CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
    OPTIONS (
    format = 'PARQUET',
    uris = ['gs://ny-yellow-taxis/yellow_tripdata_2024-*.parquet']
    );
    ```
    3. Create a (regular/materialized) table
    ```sql
    CREATE OR REPLACE TABLE ny-taxis-datawarehousing.nytaxi.yellow_tripdata_non_partitioned AS
    SELECT * FROM ny-taxis-datawarehousing.nytaxi.external_yellow_tripdata;
    ```

# Questions
## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- <mark>20,332,093</mark>
- 85,431,289

```sql
SELECT COUNT(*) AS CNT
FROM ny-taxis-datawarehousing.nytaxi.yellow_tripdata_non_partitioned
```

## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- <mark>0 MB for the External Table and 155.12 MB for the Materialized Table</mark>
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```sql
SELECT COUNT(DISTINCT(PULocationID))
FROM ny-taxis-datawarehousing.nytaxi.yellow_tripdata_non_partitioned
-- This query will process 155.12 MB when run.
```

```sql
SELECT COUNT(DISTINCT(PULocationID))
FROM ny-taxis-datawarehousing.nytaxi.external_yellow_tripdata
-- This query will process 0 B when run.
```

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- <mark>BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.</mark>
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- <mark>8,333</mark>
```sql
SELECT COUNT(*) AS CNT
FROM ny-taxis-datawarehousing.nytaxi.yellow_tripdata_non_partitioned
WHERE fare_amount = 0
```

## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
- <mark>Partition by tpep_dropoff_datetime and Cluster on VendorID</mark>
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- <mark>310.24 MB for non-partitioned table and 26.84 MB for the partitioned table</mark>
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

```sql
CREATE OR REPLACE TABLE ny-taxis-datawarehousing.nytaxi.yellow_tripdata_partitioned
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM ny-taxis-datawarehousing.nytaxi.yellow_tripdata_non_partitioned
```
```sql
-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
SELECT COUNT(DISTINCT(VendorID))
FROM `nytaxi.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_dropoff_datetime) >= DATE(2024,3,1) AND DATE(tpep_dropoff_datetime) <= DATE(2024, 3, 15)
-- This query will process 310.24 MB when run.
```
```sql
-- Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
SELECT COUNT(DISTINCT(VendorID))
FROM `nytaxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_dropoff_datetime) >= DATE(2024,3,1) AND DATE(tpep_dropoff_datetime) <= DATE(2024, 3, 15)
-- This query will process 26.84 MB when run.
```
## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- <mark>GCP Bucket</mark>
- Big Table

## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- <mark>False</mark>

## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

<mark> 0 bytes. Because it's metadata that's already calculated</mark>
```sql
SELECT count(*)
FROM `nytaxi.yellow_tripdata_non_partitioned`
-- This query will process 0 B when run.
```

## Solution

Solution: https://www.youtube.com/watch?v=wpLmImIUlPg