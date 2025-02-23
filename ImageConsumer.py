from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import os 
import base64

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "dynamic-nomad-448416-h3"
topic_name = "ImageReader"   # change it for your topic name if needed
subscription_id = "ImageReader-sub"   # change it for your subscription name if needed

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id, topic_name)

print(f"Listening for messages on {subscription_path}..\n")

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Decode the base64 image (to check if it's a valid image)
        image_bytes = base64.b64decode(message.data)

        # Get the image name from the ordering key
        image_name = message.attributes.get('image_key', 'Unknown')

        # Print the name of the image
        print(f"Consumed image: {image_name}")
        
        # Acknowledge the message after processing
        message.ack()
    except Exception as e:
        print(f"Failed to process message: {e}")
        # NACK the message to reprocess later
        message.nack()

with subscriber:
    # The call back function will be called for each message received from the topic 
    # through the subscription.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
