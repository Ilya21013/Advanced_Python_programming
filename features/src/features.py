import os
import time
import pika
from sklearn.datasets import load_diabetes
import random
import json
from datetime import datetime

# Get RabbitMQ host from environment variable, default to 'localhost'
rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')

# Retry mechanism to connect to RabbitMQ
while True:
    try:
        print(f"Attempting to connect to RabbitMQ at {rabbitmq_host}...")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()
        print(f"Successfully connected to RabbitMQ at {rabbitmq_host}")
        break
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Connection to RabbitMQ failed: {e}. Retrying in 5 seconds...")
        time.sleep(5)

# Declare queues
channel.queue_declare(queue='features')
channel.queue_declare(queue='y_true')

# Load the diabetes dataset
diabetes = load_diabetes()
X = diabetes.data.tolist()
y = diabetes.target.tolist()

# Publish messages to RabbitMQ
while True:
    try:
        random_row = random.randint(0, len(X) - 1)
        selected_features = X[random_row]
        selected_label = y[random_row]

        message_id = datetime.timestamp(datetime.now())
        message_features = {"id": message_id, "body": selected_features}
        message_y_true = {"id": message_id, "body": selected_label}

        # Publish features and y_true messages to RabbitMQ
        channel.basic_publish(exchange='', routing_key='features', body=json.dumps(message_features))
        channel.basic_publish(exchange='', routing_key='y_true', body=json.dumps(message_y_true))

        print(f"Sent features: {message_features}")
        print(f"Sent y_true: {message_y_true}")
        time.sleep(10)  # Adjust message publishing interval as needed
    except Exception as e:
        print(f"Error during publishing: {e}. Retrying in 5 seconds...")
        time.sleep(5)
