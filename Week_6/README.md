## Week 6 : Stream Processing

# Homework Answers Week 6 

## Questions 1. Redpanda version
v22.3.5
```ssh
docker exec -it redpanda-1 rpk version
```

## Question 2. Creating a topic
TOPIC       STATUS
test-topic  OK
```ssh
docker exec -it redpanda-1 rpk topic create test-topic
```

## Question 3. Connecting to the Kafka server
True
```ssh
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print(producer.bootstrap_connected())
```

## Question 4. Sending data to the stream
Both took approximately the same amount of time
```ssh
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
```

## Question 5. Time to send data
Script execution completed in 76.45 seconds.
```ssh
python green_producer.py
```

## Question 6. Parsing the data
DataFrame[lpep_pickup_datetime: string, lpep_dropoff_datetime: string, PULocationID: int, DOLocationID: int, passenger_count: double, trip_distance: double, tip_amount: double]
```ssh

python parsing_data.py
```

## Question 7. Most popular destination 
East Harlem South 
```ssh
python join_data.py
```