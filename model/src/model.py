import pika
import json
import pickle
import numpy as np

# Load the serialized model from file
model_path = "myfile.pkl"  # Adjust path if necessary
with open(model_path, "rb") as file:
    model = pickle.load(file)

print(f"Model loaded: {model}")

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

# Declare queues
channel.queue_declare(queue="features_queue")
channel.queue_declare(queue="predictions_queue")

def callback(ch, method, properties, body):
    # Parse the incoming message
    message = json.loads(body)
    message_id = message["id"]
    feature_vector = np.array(message["body"]).reshape(1, -1)  # Ensure feature vector is 2D

    # Make predictions using the model
    prediction = model.predict(feature_vector)[0]  # Get the first value (scalar)

    # Create a message for the predictions queue
    message_prediction = {
        "id": message_id,
        "body": prediction
    }

    # Send the prediction to the predictions queue
    channel.basic_publish(
        exchange="",
        routing_key="predictions_queue",
        body=json.dumps(message_prediction)
    )

    print(f"Processed and sent prediction: {message_prediction}")

# Consume messages from the features queue
channel.basic_consume(queue="features_queue", on_message_callback=callback, auto_ack=True)

print("Waiting for messages...")
channel.start_consuming()
