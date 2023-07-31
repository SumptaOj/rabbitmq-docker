import pika
from datetime import datetime, time
import json

# Global counter for edits per minute
global_edits_per_minute = {}
german_edits_per_minute = {}

# Callback function when a message is received
def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    global global_edits_per_minute
    global german_edits_per_minute

    try:
        # Convert the incoming message (body) to a JSON object
        message = json.loads(body)

        # Extract the timestamp and server name from the JSON object
        timestamp_str = message.get('timestamp')
        server_name = message.get('server_name')

        # if not timestamp_str or not server_name:
        #     print("Missing required fields in the message.")
        #     return

        # Convert the timestamp string to a timestamp object
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            # If the timestamp is in a different format ("%Y-%m-%d %H:%M:%S"), adjust the format
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

        # Increment the global edits counter for this minute
        global_edits_per_minute[timestamp] = global_edits_per_minute.get(timestamp, 0) + 1

        # Check if the edit is for the German Wikipedia
        if "de.wikipedia.org" in server_name:
            # Increment the German Wikipedia edits counter for this minute

            german_edits_per_minute[timestamp] = german_edits_per_minute.get(timestamp, 0) + 1

    except Exception as e:
        print(f"Error occurred while processing message: {str(e)}")


# Establishing connection to RabbitMQ server

def connect(callback):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    # Declaring the exchange and queue 
    channel.exchange_declare(exchange='wikipedia_edits', exchange_type='direct')
    channel.queue_declare(queue='wikipedia_queue')

    # Bind the queue to the exchange with the appropriate routing key 
    channel.queue_bind(exchange='wikipedia_edits', queue='wikipedia_queue', routing_key='wikipedia_key')

    # Set up the callback function to be triggered when a message is received
    channel.basic_consume(queue='wikipedia_queue', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit, press CTRL+C')

    # Start consuming messages from the queue
    channel.start_consuming()

    connection.close()
    
    print(f"Connection closed")

attempts = 0
success = False
while attempts < 3 and not success:
    try:
        connect(callback)

    except pika.exceptions.AMQPConnectionError:
        print(f"Error connecting to RabbitMQ server.")
        attempts += 1
        if attempts < 3:
            print(f"Retrying in 5 seconds...")
            time.sleep(5)
        else:
            print(f"Max attempts reached. Exiting...")
            success = True

    except KeyboardInterrupt:
        print(f"Consumer interrupted.")
        success = True

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        attempts += 1
        if attempts < 3:
            print(f"Retrying in 5 seconds...")
            time.sleep(5)
        else:
            print(f"Max attempts reached. Exiting...")
            success = True

    finally:
        # After consuming all messages, print the aggregated edits per minute data
        print("\nAggregated Global Edits Per Minute:")
        for timestamp, edits_count in global_edits_per_minute.items():
            print(f"{timestamp}: {edits_count}")

        print("\nAggregated German Wikipedia Edits Per Minute:")
        for timestamp, edits_count in german_edits_per_minute.items():
            print(f"{timestamp}: {edits_count}")

        
