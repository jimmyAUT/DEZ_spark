# The goal of this project is to use pyspark to create DataFrame of NYC Yellow Taxi data 2024-10 and taxi_zone_lookup and execute simple SQL queries.

===

Q1. spark.version?  
**Ans: 3.4.4**

---

Q2: What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

**Ans: 25MB**  

```bash
cd data/repartition && ls -lh
```

output:  

```
total 97M
-rw-r--r-- 1 vice vice   0 Mar  7 17:23 _SUCCESS
-rw-r--r-- 1 vice vice 25M Mar  7 17:23 part-00000-0d81658f-cd65-4a96-a4f4-844b913dc0ec-c000.snappy.parquet
-rw-r--r-- 1 vice vice 25M Mar  7 17:23 part-00001-0d81658f-cd65-4a96-a4f4-844b913dc0ec-c000.snappy.parquet
-rw-r--r-- 1 vice vice 25M Mar  7 17:23 part-00002-0d81658f-cd65-4a96-a4f4-844b913dc0ec-c000.snappy.parquet
-rw-r--r-- 1 vice vice 25M Mar  7 17:23 part-00003-0d81658f-cd65-4a96-a4f4-844b913dc0ec-c000.snappy.parquet
```

---

Q3: How many taxi trips were there on the 15th of October?  
**Asn: 116396(125,567)**  

```python 
spark.sql("""
    SELECT count(1) AS trips_count 
    FROM yellow_tripdata_2024_10 
    WHERE DATE(tpep_pickup_datetime) = '2024-10-15'
""").show()
```

---

Q4: What is the length of the longest trip in the dataset in hours?  
**Ans: 162**

```python
spark.sql("""
    SELECT 
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS hours
    FROM yellow_tripdata_2024_10
    ORDER BY hours DESC
    LIMIT 10;
""").show() 
```

---

Q5: Spark’s User Interface which shows the application's dashboard runs on which local port?  
**Ans: 4040**

---

Q6: Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?  
**Ans: Governor's Island/Ellis Island/Liberty Island** 

```python
spark.sql("""
    SELECT zones.Zone , count(1) as trips_count 
    FROM yellow_tripdata_2024_10
    INNER JOIN zones 
    ON zones.LocationID = yellow_tripdata_2024_10.PULocationID
    GROUP BY zones.Zone
    ORDER BY trips_count;
""").show()  
```