import pika
import json
import pandas as pd
import os

# File path for metric_log.csv in the logs directory
LOG_DIR = "../../logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "metric_log.csv")

# Initialize CSV file with headers if not exists
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w") as file:
        file.write("id,y_true,y_pred,absolute_error\n")

# RabbitMQ setup
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queues
channel.queue_declare(queue='features')
channel.queue_declare(queue='y_true')

# Store incoming messages in a buffer
buffer = {"y_true": {}, "y_pred": {}}


def calculate_absolute_error(y_true, y_pred):
    """Calculate the absolute error."""
    return abs(float(y_true) - float(y_pred))


def log_to_csv(entry):
    """Log the calculated values to CSV."""
    df = pd.DataFrame([entry])
    df.to_csv(LOG_FILE, mode='a', header=False, index=False)


def process_message(queue_name, body):
    """Process the incoming message and handle buffering."""
    global buffer

    message = json.loads(body)
    message_id = message["id"]
    message_body = message["body"]

    # Ensure the body is a single value (convert if it's a list)
    if isinstance(message_body, list):
        message_body = message_body[0]

    buffer[queue_name][message_id] = message_body

    # If both y_true and y_pred are available for the ID, calculate error and log
    if message_id in buffer["y_true"] and message_id in buffer["y_pred"]:
        y_true = buffer["y_true"].pop(message_id)
        y_pred = buffer["y_pred"].pop(message_id)

        absolute_error = calculate_absolute_error(y_true, y_pred)

        # Log to CSV
        log_entry = {
            "id": message_id,
            "y_true": y_true,
            "y_pred": y_pred,
            "absolute_error": absolute_error,
        }
        log_to_csv(log_entry)
        print(f"Logged to CSV: {log_entry}")


def callback_features(ch, method, properties, body):
    """Callback function for features queue."""
    process_message("y_pred", body)


def callback_y_true(ch, method, properties, body):
    """Callback function for y_true queue."""
    process_message("y_true", body)


# Consume messages
channel.basic_consume(queue='features', on_message_callback=callback_features, auto_ack=True)
channel.basic_consume(queue='y_true', on_message_callback=callback_y_true, auto_ack=True)

print(" [*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
