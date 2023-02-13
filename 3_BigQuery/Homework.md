## Week 3 Homework

## Question 1:

>Command:

```
SELECT COUNT(*) FROM `speedy-code-376706.dt_zc_week3.fhv_tripdata_2019`
```
>Answer:
`43,244,696`

## Question 2:
>Command:
```
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dt_zc_week3.fhv_tripdata_2019`
```
```
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dt_zc_week3.fhv_tripdata_2019_bq`
```
```
SELECT affiliated_base_number, COUNT(*)
FROM (
  SELECT DISTINCT affiliated_base_number
  FROM `dt_zc_week3.fhv_tripdata_2019`
)
GROUP BY affiliated_base_number
```
>Answer:
`0 MB for the External Table and 317.94MB for the BQ Table`

# Question 3:

>Command:
```
SELECT COUNT(*)
FROM `dt_zc_week3.fhv_tripdata_2019`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```
>Answer:
`717,748`

## Question 4:
>Command:
```
CREATE OR REPLACE TABLE
  `dt_zc_week3.fhv_tripdata_2019_partitioned_clustered`
PARTITION BY
DATE_TRUNC(pickup_datetime,DAY)
CLUSTER BY
  affiliated_base_number AS (
  SELECT
    *
  FROM
    `dt_zc_week3.fhv_tripdata_2019`)
```

>Answer:
`Partition by pickup_datetime Cluster on affiliated_base_number`

## Question 5:
```
SELECT DISTINCT affiliated_base_number
FROM `dt_zc_week3.fhv_tripdata_2019_bq`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
```
```
SELECT DISTINCT affiliated_base_number
FROM `dt_zc_week3.fhv_tripdata_2019_partitioned`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
```
```
SELECT DISTINCT affiliated_base_number
FROM `dt_zc_week3.fhv_tripdata_2019_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
```

>Answer:
`647.87 MB for non-partitioned table and 23.06 MB for the partitioned table`

## Question 6:
>Answer:
`GCP Bucket`

## Question 7:
>Answer:
`False`

## Question 8:

[ETL Script here](https://github.com/jprq87/Datatalks-Zoomcamp-2023/blob/main/3_BigQuery/etl_parquet.py)

>Count confirmation queries:
```
SELECT COUNT(*) AS CSV
FROM `dt_zc_week3.fhv_tripdata_2019`
UNION ALL
SELECT COUNT(*) AS PARQUET
FROM `dt_zc_week3.fhv_tripdata_2019_parquet`;
```
>Answer:
`43,244,696`