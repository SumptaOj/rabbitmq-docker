import random
import time
import pika
import csv
import json

# Creating queues, exchanges, and bindings:
def emit():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    # Declaring the exchanges (direct type)
    channel.exchange_declare(exchange='wikipedia_edits', exchange_type='direct')

    # Declaring the queues
    channel.queue_declare(queue='wikipedia_queue')

    # Binding the queues to the exchange with their respective routing keys
    channel.queue_bind(exchange='wikipedia_edits', queue='wikipedia_queue', routing_key='wikipedia_key')


    # Read the data from the CSV file into a list of dictionaries
    with open('de_challenge_sample_data.csv', 'r') as file:
        # Use the CSV reader to handle the file
        csv_reader = csv.reader(file)

        # Skip the header line that has  the column names
        next(csv_reader)

        # Process each row in the CSV file
        for row in csv_reader:
            # Convert the row data into a dictionary using the header as keys
            data = dict(zip(["","$schema", "id", "type", "namespace", "title", "comment", "timestamp", "user",
                                "bot", "minor", "patrolled", "server_url", "server_name", "server_script_path",
                                "wiki", "parsedcomment", "meta_domain", "meta_uri", "meta_request_id",
                                "meta_stream", "meta_topic", "meta_dt", "meta_partition", "meta_offset", "meta_id",
                                "length_old", "length_new", "revision_old", "revision_new"], row))

            time.sleep(random.random())

            # Create a dictionary containing the required fields
            message_data = {
                "timestamp": data['meta_dt'],
                "server_name": data["server_name"]
            }

            # Convert the dictionary to a JSON string
            message_body = json.dumps(message_data)

            # Send the message to the exchange with the appropriate routing key
            source = data["server_name"]  # Access the server_name field for the source
            try:
                if "de.wikipedia.org" in source:
                    channel.basic_publish(exchange='wikipedia_edits', routing_key='wikipedia_key', body=message_body)
                    print(f"Sending message for German Wikipedia")
                else:
                    channel.basic_publish(exchange='wikipedia_edits', routing_key='wikipedia_key', body=message_body)
                    print(f"Sending message for Global Wikipedia")

           
            except Exception as e:
                print(f"Error occurred while sending message for source '{source}': {str(e)}")

    connection.close()


attempts = 0
success = False
while attempts < 3 and not success:
    try:
        emit()

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
        print(f"Producer interrupted.")
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
        print(f"Connection closed")
