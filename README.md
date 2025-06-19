Overview
This service consumes weather data from a Kafka topic and stores it in an Aiven PostgreSQL database. It's part of a larger weather data pipeline that collects and processes weather information from multiple cities.

Prerequisites
System Requirements
Python 3.7+

Apache Kafka (running locally or via Aiven)

Aiven PostgreSQL database

Python Dependencies
Install required packages:

bash
pip install kafka-python psycopg2-binary python-dotenv
Configuration
Create a .env file in the project root with these variables:

ini
# Kafka Configuration
KAFKA_BROKER=your.kafka.broker:9092
KAFKA_TOPIC=weather_data

# Aiven PostgreSQL Configuration
AIVEN_DB_HOST=your-postgres-host
AIVEN_DB_PORT=5432
AIVEN_DB_NAME=defaultdb
AIVEN_DB_USER=avnadmin
AIVEN_DB_PASSWORD=your-password
Database Schema
The consumer automatically creates this table if it doesn't exist:

sql
CREATE TABLE IF NOT EXISTS weather_data (
    city VARCHAR(100),
    temperature FLOAT,
    humidity INT,
    pressure INT,
    wind_speed FLOAT,
    weather_description VARCHAR(100),
    processed_at TIMESTAMP,
    PRIMARY KEY (city, processed_at)
)
Running the Consumer
Start the consumer service:

bash
python consumer.py
Consumer Behavior
Connects to Kafka and starts consuming from the earliest available message

Processes each message and stores weather data in PostgreSQL

Automatically commits offsets

Prints success/error messages to console

Message Format Expectation
The consumer expects messages in this JSON format:

json
{
    "name": "City Name",
    "main": {
        "temp": 22.5,
        "humidity": 65,
        "pressure": 1012
    },
    "wind": {
        "speed": 3.1
    },
    "weather": [
        {
            "description": "clear sky"
        }
    ],
    "processed_at": "2023-05-01T12:00:00Z"
}
Error Handling
The consumer handles:

Database connection errors

Invalid message formats

Duplicate primary key violations

Network connectivity issues

Errors are logged to console but don't stop the consumer.

Monitoring
Check the database for stored data:

sql
SELECT * FROM weather_data ORDER BY processed_at DESC LIMIT 10;
