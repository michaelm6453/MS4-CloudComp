from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists
import glob
import json
import os

# Find and set the service account credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Project config
project_id = ""
topic_name = "sensor-data"
subscription_id = "convert-reading-sub"

# Create subscriber and publisher clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = publisher.topic_path(project_id, topic_name)

# Create the subscription filtered to only filtered messages
try:
    subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path, "filter": 'attributes.stage = "filtered"'})
    print(f"Subscription created: {subscription_path}")
except AlreadyExists:
    print(f"Subscription already exists: {subscription_path}")

print(f"ConvertReading: listening for messages on {subscription_path}..\n")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Parse the incoming message
        record = json.loads(message.data.decode('utf-8'))

        # Copy the record so we don't modify the original
        converted = record.copy()

        # Convert Celsius to Fahrenheit
        if converted.get('temperature') is not None:
            celsius = float(converted['temperature'])
            converted['temperature'] = round(celsius * 1.8 + 32, 2)

        # Convert kPa to psi
        if converted.get('pressure') is not None:
            kpa = float(converted['pressure'])
            converted['pressure'] = round(kpa / 6.895, 4)

        # Republish as processed with the final stage
        record_value = json.dumps(converted).encode('utf-8')
        publisher.publish(topic_path, record_value, stage="processed")
        print(f"Converted: {converted.get('profile_name')} at {converted.get('time')} "
              f"| {converted.get('temperature')} F | {converted.get('pressure')} psi")

    except Exception as e:
        print(f"Error processing message: {e}")

    finally:
        # Ack the message
        message.ack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
