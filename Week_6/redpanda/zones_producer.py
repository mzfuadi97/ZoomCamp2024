from kafka import KafkaProducer
import pandas as pd
import time 

# Record the start time
start_time = time.time()

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Define the topic name
topic = 'zones'

# Define the data types for each column
dtype_dict = {
    'LocationID': 'str',
    'Borough': 'str',
    'Zone': 'str'
}

# Define the columns you want to keep
columns_to_keep = [
    'LocationID',
    'Borough',
    'Zone'
]

# Read the CSV file into a DataFrame, specifying the dtype parameter
df_zones = pd.read_csv('dataset/taxi_zone_lookup.csv', usecols=columns_to_keep, dtype=dtype_dict, na_values=[''])

# Iterate over each row of the DataFrame
for row in df_zones.itertuples(index=False):
    # Extract the row values as a dictionary
    row_dict = {col: getattr(row, col) for col in row._fields}
    
    # Filter the dictionary to keep only the specified columns
    row_dict_filtered = {key: value for key, value in row_dict.items() if key in columns_to_alias}
    
    # Convert the dictionary to JSON and encode as bytes
    message = pd.Series(row_dict_filtered).to_json().encode('utf-8')
    
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