# Setting up RabbitMQ with Docker Compose

This repository contains a RabbitMQ prototype setup with Docker Compose, consisting of a producer and a consumer for testing and demonstrating possible scenario.

The goal of this project is to create a system that collects and processes Wikipedia edit events, calculates the global number of edits per minute, and also tracks the number of edits for the German Wikipedia per minute. The system uses RabbitMQ as the message broker to handle the message passing between the producer and consumer.

## Setup

1. Clone the repository to your local machine.

2. Install Docker Desktop and make sure it's running.

3. Open the project folder in Visual Studio Code (optional but recommended).

## Project Structure

The repository has the following files:

- `docker-compose.yml`: The Docker Compose configuration file that defines the RabbitMQ server, producer, and consumer services.

- `Dockerfile_producer` and `Dockerfile_consumer`: Dockerfiles for building the Docker images for the producer and consumer scripts.

- `producer.py`: The Python script acting as the producer, emitting sample data to the RabbitMQ queue.

- `consumer.py`: The Python script acting as the consumer, reading data from the RabbitMQ queue and performing aggregations.

## How to Run

1. Open the terminal in Visual Studio Code or any terminal/command prompt in the project folder.

2. Run the following command to build the Docker images and start the containers:

docker-compose up --build

This command will start the RabbitMQ server and the producer and consumer containers, and you'll see the logs in the terminal.

## Note

Replace /path/to/producer.py in producer.py with the actual path to the respective Python script.

To stop the containers, press 'Ctrl+C' in the terminal where the Docker Compose is running.

To remove the Docker containers and images created by Docker Compose, run the following command:

docker-compose down