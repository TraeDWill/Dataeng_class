from google.cloud import pubsub_v1

def receive_messages(project_id, subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    def callback(message):
        # Discard the message
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    print(f"Listening for messages on {subscription_path}...")
    try:
        # Keep the main thread alive
        while True:
            pass
    except KeyboardInterrupt:
        print("Received KeyboardInterrupt, stopping...")
        subscriber.close()

if __name__ == "__main__":
    project_id = "Data-Eng-Williams"  # Replace with your Google Cloud project ID
    subscription_name = "my-sub"  # Replace with your subscription name

    receive_messages(project_id, subscription_name)