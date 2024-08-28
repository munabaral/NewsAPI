import csv
import json
import os
import subprocess
from kafka import KafkaConsumer

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'george_midterm_news',                    # Kafka topic to consume from
    bootstrap_servers='localhost:9092',       # Kafka broker address
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize message value as JSON
)

# Define the CSV file path
csv_file_path = 'news_articles.csv'

# Check if the CSV file already exists
file_exists = os.path.isfile(csv_file_path)

# Create and write the CSV file
with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
    # Customize the CSV writer to use '\001' as the delimiter and '\n' as the line terminator
    writer = csv.writer(csv_file, delimiter='\001', lineterminator='\n')

    fieldnames = ['author', 'title', 'source_name', 'publishedAt']

    # Write the header only if the file doesn't already exist
    if not file_exists:
        print("Creating new CSV file.")
        writer.writerow(fieldnames)

    else:
        print("CSV file already exists.")

    # Initialize a variable to track the last offset
    last_offset = None


    # Consume messages from the Kafka topic indefinitely
    for message in consumer:
        article = message.value
        print(f"Received message: {article}")

        # Write article data to CSV file
        writer.writerow([
            article.get('author'),
            article.get('title'),
            article.get('source_name'),
            article.get('publishedAt')
        ])

        # Update the last offset seen
        last_offset = message.offset

        # Get the current offsets for all partitions assigned to the consumer
        partitions = consumer.assignment()
        end_offsets = consumer.end_offsets(partitions)

        # Check if the current message is the last one in all partitions
        is_last_message = all(last_offset >= end_offsets[partition] - 1 for partition in partitions)

        # If it's the last message, copy the CSV file to HDFS
        if is_last_message:
            print("Copying CSV file to Hadoop.")

            # Create directory in Hadoop if it doesn't exist
            hdfs_dir = '/midterm_project'
            mkdir_command = f'hadoop fs -mkdir -p {hdfs_dir}'
            subprocess.run(mkdir_command, shell=True, check=True)

            # Copy CSV file to Hadoop
            hdfs_path = f'{hdfs_dir}/news_articles.csv'
            copy_command = f'hadoop fs -copyFromLocal -f {csv_file_path} {hdfs_path}'
            subprocess.run(copy_command, shell=True, check=True)

            print(f"CSV file copied to Hadoop: {hdfs_path}")
            print("Waiting for next cycle from producer.")
