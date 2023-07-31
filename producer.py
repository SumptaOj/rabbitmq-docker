import random
import time
import pika
import csv
import json


class WikipediaEditsProducer:
    def __init__(self, csv_file_path, rabbitmq_host='localhost'):
        self.csv_file_path = csv_file_path
        self.rabbitmq_host = rabbitmq_host

    def setup_rabbitmq(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
        self.channel = self.connection.channel()

        # Declaring the exchanges (direct type)
        self.channel.exchange_declare(exchange='wikipedia_edits', exchange_type='direct')

        # Declaring the queues
        self.channel.queue_declare(queue='global_wikipedia_queue')
        self.channel.queue_declare(queue='german_wikipedia_queue')

        # Binding the queues to the exchange with their respective routing keys
        self.channel.queue_bind(exchange='wikipedia_edits', queue='global_wikipedia_queue', routing_key='global_wikipedia_key')
        self.channel.queue_bind(exchange='wikipedia_edits', queue='german_wikipedia_queue', routing_key='german_wikipedia_key')

    def process_csv_data(self):
        try:
            with open(self.csv_file_path, 'r') as file:
                csv_reader = csv.reader(file)
                next(csv_reader)  # Skip the header line

                for row in csv_reader:
                    data = self.parse_row_to_dict(row)

                    time.sleep(random.random())

                    message_data = {
                        "timestamp": data['meta_dt'],
                        "server_name": data["server_name"]
                    }

                    message_body = json.dumps(message_data)
                    self.send_message(data["server_name"], message_body)

        except Exception as e:
            print(f"Error occurred: {str(e)}")

    def parse_row_to_dict(self, row):
        header = [
            "$schema", "id", "type", "namespace", "title", "comment", "timestamp", "user",
            "bot", "minor", "patrolled", "server_url", "server_name", "server_script_path",
            "wiki", "parsedcomment", "meta_domain", "meta_uri", "meta_request_id",
            "meta_stream", "meta_topic", "meta_dt", "meta_partition", "meta_offset", "meta_id",
            "length_old", "length_new", "revision_old", "revision_new"
        ]
        return dict(zip(header, row))

    def send_message(self, source, message_body):
        try:
            routing_key = 'german_wikipedia_key' if "de.wikipedia.org" in source else 'global_wikipedia_key'
            self.channel.basic_publish(exchange='wikipedia_edits', routing_key=routing_key, body=message_body)
            print(f"Sending message for {'German Wikipedia' if routing_key == 'german_wikipedia_key' else 'Global Wikipedia'}")
        except Exception as e:
            print(f"Error occurred while sending message for source '{source}': {str(e)}")

    def close_connection(self):
        self.connection.close()
        print(f"Connection closed")


if __name__ == "__main__":
    csv_file_path = '/path/to/producer.py/de_challenge_sample_data.csv'
    producer = WikipediaEditsProducer(csv_file_path)

    try:
        producer.setup_rabbitmq()
        producer.process_csv_data()
    finally:
        producer.close_connection()
