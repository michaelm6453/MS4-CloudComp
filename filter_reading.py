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
subscription_id = "filter-reading-sub"

# Create subscriber and publisher clients
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = publisher.topic_path(project_id, topic_name)

# Create the subscription filtered to only raw messages
try:
    subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path, "filter": 'attributes.stage = "raw"'})
    print(f"Subscription created: {subscription_path}")
except AlreadyExists:
    print(f"Subscription already exists: {subscription_path}")

print(f"FilterReading: listening for messages on {subscription_path}..\n")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Parse the incoming message
        record = json.loads(message.data.decode('utf-8'))

        # Check if all required fields exist
        if (record.get('temperature') is not None and
                record.get('humidity') is not None and
                record.get('pressure') is not None):
            # Republish as filtered with the next stage
            record_value = json.dumps(record).encode('utf-8')
            publisher.publish(topic_path, record_value, stage="filtered")
            print(f"Forwarded (complete): {record.get('profile_name')} at {record.get('time')}")
        else:
            # Drop incomplete records
            print(f"Dropped (incomplete): {record.get('profile_name')} at {record.get('time')}")
    
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
