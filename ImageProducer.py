from google.cloud import pubsub_v1  # Install with: pip install google-cloud-pubsub
import glob
import json
import os
import base64

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]  

# Set the project_id with your project ID
project_id = "dynamic-nomad-448416-h3"
topic_name = "ImageReader"   # Change it for your topic name if needed

# Create a publisher and get the topic path for the publisher
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages with ordering keys to {topic_path}.")

# Path to the images folder
images_path = "Dataset_Occluded_Pedestrian/"

# Get all files starting with 'A' in the images path
files = glob.glob(os.path.join(images_path, "A*.*"))

print(f"Publishing images to {topic_name}...")
for image_path in files:
    with open(image_path, "rb") as image_file:
        # Read the image and serialize it to base64
        value = base64.b64encode(image_file.read()).decode('utf-8')

        # Fix padding
        value += "=" * ((4 - len(value) % 4) % 4)

    key = os.path.basename(image_path)  # Extract the image name as the key

    # Create the JSON object with ID and Image keys
    message = {
        "ID": key,
        "Image": value
    }

    # Convert the message to JSON format and encode it
    json_message = json.dumps(message).encode('utf-8')

    try:
        # Publish the JSON-encoded message
        future = publisher.publish(topic_path, json_message, ordering_key=key)
        future.result()  # Ensure the publishing is completed
        print(f"Published the key: {key}")
    except Exception as e:
        print(f"Failed to publish {key}. Error: {e}")

print("All images are published.")
