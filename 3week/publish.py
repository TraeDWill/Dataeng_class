
from google.cloud import pubsub_v1
import json

project_id = "Data-Eng-Williams"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

# Opens the file in read mode
with open('bcsample.json', 'r') as file:
    data = json.load(file);

#Itterates through each of the 2 vehicles
for line in data:
    stuffs = data[line]
    #Itterates through the list of values and stuffs, stops at 50 items. Enumerate cycles through stuffs, which is the data in each vehicle
    for i, stuff in enumerate(stuffs):
        if i > 50:
            break
        #Stores Json data drectly on a file, while encode properly formats it
        vehicle = json.dumps(stuff).encode("utf-8")
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, vehicle)
        print(future.result())

print(f"Published messages to {topic_path}.")
