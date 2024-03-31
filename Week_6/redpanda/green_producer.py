from kafka import KafkaProducer
import pandas as pd
import time

# Record the start time
start_time = time.time()

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Define the topic name
topic = 'green-trips'

# Define the data types for each column
dtype_dict = {
    'lpep_pickup_datetime': 'str',
    'lpep_dropoff_datetime': 'str',
    'PULocationID': 'int',
    'DOLocationID': 'int',
    'passenger_count': 'float',
    'trip_distance': 'float',
    'tip_amount': 'float'
}

# Define the columns you want to keep
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

# Read the CSV file into a DataFrame, specifying the dtype parameter
df_green = pd.read_csv('dataset/green_tripdata_2019-10.csv', usecols=columns_to_keep, dtype=dtype_dict, na_values=[''])
df_zones = pd.read_csv('dataset/taxi_zone_lookup.csv', dtype={'LocationID':'int'})

# Fill missing values in integer columns with zeros
df_green['passenger_count'] = df_green['passenger_count'].fillna(0).astype(int)

# Join the green trips data with the taxi zones lookup data
df_joined = df_green.join(df_zones, on='DOLocationID', how='inner')
df_joined.dropna(subset=['Zone'], inplace=True)
# Select the columns to include in the final DataFrame
columns_join = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'Zone'
]

df_joined = df_joined[columns_join]
df_joined.dropna(subset=['Zone'], inplace=True)
# Iterate over each row of the DataFrame
for row in df_joined.itertuples(index=False):
    # Extract the row values as a dictionary
    row_dict = {col: getattr(row, col) for col in columns_join}
    
    # Convert the dictionary to JSON and encode as bytes
    message = pd.Series(row_dict).to_json().encode('utf-8')
    
    # Send the message to the Kafka topic
    producer.send(topic, value=message)
    
# Close the producer
producer.close()

# Record the end time
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

# Print the execution time
print(f"Script execution completed in {execution_time:.2f} seconds.")