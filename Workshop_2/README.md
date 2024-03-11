## Workshop 2 : Stream Processing with RisingWave

# Homework Answers Workshop 2 

## Questions 1. Highest average trip time
Midtown Center, University Heights/Morris Heights
select * FROM trip_anomalies WHERE (max_trip_time_minutes BETWEEN 10 AND 20)  ORDER BY avg_trip_time_minutes DESC;
```

## Question 2. Number of trips
10 row
```ssh
select count(*) FROM trip_anomalies WHERE  pickup_zone Like "%Midtown Center%" ;
```

## Question 3. Top 3 busiest zones
LaGuardia Airport, Lincoln Square East, JFK Airport
```ssh
from pyspark.sql import functions as F
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter("pickup_date = '2019-10-15'") \
    .count()
```