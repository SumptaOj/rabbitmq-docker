import pika
import time
from datetime import datetime
import json
import pandas as pd


class WikipediaEditsProcessor:
    def __init__(self):
        self.global_edits_per_minute = {}
        self.german_edits_per_minute = {}
        self.connection = None

    def process_message(self, ch, method, properties, body):
        try:
            # Convert the incoming message (body) to a JSON object
            message = json.loads(body)

            # Extract the timestamp and server name from the JSON object
            timestamp_str = message.get('timestamp')
            server_name = message.get('server_name')
            edit_id = message.get("edit_id")

            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").replace(second=0)

            # Check if the edit is for the German Wikipedia
            if "de.wikipedia.org" in server_name:
                # Increment the German Wikipedia edits counter for this minute
                self.german_edits_per_minute[timestamp] = self.german_edits_per_minute.get(timestamp, 0) + 1
                print(f"Printing German edit for {timestamp}: 1")
            else:
                # Increment the global edits counter for this minute
                self.global_edits_per_minute[timestamp] = self.global_edits_per_minute.get(timestamp, 0) + 1
                print(f"Printing Global edit for {timestamp}")

        except Exception as e:
            print(f"Error occurred while processing message: {str(e)}")

    def start_consuming(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = self.connection.channel()

            # Declaring the exchange and queue (make sure to match the exchange and queue names with the producer)
            channel.exchange_declare(exchange='wikipedia_edits', exchange_type='direct')
            channel.queue_declare(queue='global_wikipedia_queue')

            # Bind the queue to the exchange with the appropriate routing key (make sure to match with the producer)
            channel.queue_bind(exchange='wikipedia_edits', queue='global_wikipedia_queue',
                               routing_key='global_wikipedia_key')

            # Set up the callback function to be triggered when a message is received
            channel.basic_consume(queue='global_wikipedia_queue', on_message_callback=self.process_message, auto_ack=True)

            print('Waiting for messages. To exit, press CTRL+C')

            # Start consuming messages from the queue
            channel.start_consuming()

        except Exception as e:
            print(f"Error occurred: {str(e)}")

        finally:
            # After consuming all messages, print the aggregated edits per minute data
            self.print_aggregated_data()
            self.connection.close()
            print(f"Connection closed")

    def print_aggregated_data(self):
        print("\nAggregated Global Edits Per Minute:")
        for timestamp, edits_count in self.global_edits_per_minute.items():
            print(f"{timestamp}: {edits_count}")

        print("\nAggregated German Wikipedia Edits Per Minute:")
        for timestamp, edits_count in self.german_edits_per_minute.items():
            print(f"{timestamp}: {edits_count}")


if __name__ == "__main__":
    wikipedia_processor = WikipediaEditsProcessor()
    wikipedia_processor.start_consuming()
