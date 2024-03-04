## Week 5 Batch Processing

# Homework Answers Week 5

## Questions 1. Spark version 
3.3.2
```ssh
spark.version
```

## Question 2. FHV October 2019 partition size
6 MB
```ssh
avg_size = rdd.map(lambda x: len(x[1])).reduce(lambda x, y: x + y) / (num_files * 1024 * 1024)
print("The average size is equal to {} MB".format(int(avg_size)))
```

## Question 3. Count records on 15th of October
62,610 rows
```ssh
from pyspark.sql import functions as F
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .filter("pickup_date = '2019-10-15'") \
    .count()
```

## Question 4. The longest trip
631,152.50 Hours

```ssh
from pyspark.sql.functions import col, max, round, to_date
df \
    .withColumn('duration', ((col('dropoff_datetime').cast('long') - col('pickup_datetime').cast('long')) / 60)/60) \
    .withColumn('pickup_date', to_date(col('pickup_datetime'))) \
    .groupBy('pickup_date') \
        .max('duration') \
    .withColumn('max_duration_rounded', round(col('max(duration)'), 2)) \
    .orderBy('max_duration_rounded', ascending=False) \
    .limit(5) \
    .show()
```

## Question 5. Spark UI port
4040

## Question 6. Least frequent pickup location zone
Jamaica Bay

```ssh
spark.sql("""
SELECT
    CONCAT(pul.Zone) AS pu_loc,
    COUNT(1)
FROM 
    fhv_2019_10 fhv INNER JOIN zones pul ON fhv.PULocationID = pul.LocationID
                      
GROUP BY 
    1
ORDER BY
    2 ASC
LIMIT 1;
""").show()
```