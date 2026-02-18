from google.cloud import pubsub_v1
import glob
import json
import os
import csv
import time

# Find and set the service account credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Project config
project_id = "" # removed from security reasons
topic_name = "sensor-data"

# Create publisher and get the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}.")

# Convert CSV values to proper types
def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

# Read the CSV file and publish each record
with open('Labels.csv', 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)

    for row in csv_reader:
        try:
            # Build the record from CSV columns
            record = {
                'time':         to_int(row.get('time')),
                'profile_name': row.get('profileName'),
                'temperature':  to_float(row.get('temperature')),
                'humidity':     to_float(row.get('humidity')),
                'pressure':     to_float(row.get('pressure'))
            }

            # Send as raw message
            record_value = json.dumps(record).encode('utf-8')
            future = publisher.publish(topic_path, record_value, stage="raw")
            future.result()
            print(f"Published: {record['profile_name']} at {record['time']}")

            # Slow it down to simulate streaming
            time.sleep(0.1)

        except Exception as e:
            print(f"Failed to publish the message: {e}")
