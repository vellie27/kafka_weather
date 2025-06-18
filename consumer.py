from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv

# Load configuration
load_dotenv()

class Config:
    KAFKA_BROKER = os.getenv('KAFKA_BROKER')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
    AIVEN_DB_HOST = os.getenv('AIVEN_DB_HOST')
    AIVEN_DB_PORT = os.getenv('AIVEN_DB_PORT')
    AIVEN_DB_NAME = os.getenv('AIVEN_DB_NAME')
    AIVEN_DB_USER = os.getenv('AIVEN_DB_USER')
    AIVEN_DB_PASSWORD = os.getenv('AIVEN_DB_PASSWORD')

def create_db_connection():
    """Create connection to Aiven PostgreSQL"""
    return psycopg2.connect(
        host=Config.AIVEN_DB_HOST,
        port=Config.AIVEN_DB_PORT,
        database=Config.AIVEN_DB_NAME,
        user=Config.AIVEN_DB_USER,
        password=Config.AIVEN_DB_PASSWORD
    )

def create_weather_table(conn):
    """Create table if not exists"""
    with conn.cursor() as cur:
        cur.execute("""
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
        """)
        conn.commit()

def store_weather_data(conn, data):
    """Store weather data in Aiven PostgreSQL"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO weather_data (
                city, temperature, humidity, pressure,
                wind_speed, weather_description, processed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data['name'],
            data['main']['temp'],
            data['main']['humidity'],
            data['main']['pressure'],
            data['wind']['speed'],
            data['weather'][0]['description'],
            data['processed_at']
        ))
        conn.commit()

def consume_weather_data():
    """Main consumer function"""
    conn = create_db_connection()
    create_weather_table(conn)
    
    consumer = KafkaConsumer(
        Config.KAFKA_TOPIC,
        bootstrap_servers=[Config.KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    for message in consumer:
        try:
            store_weather_data(conn, message.value)
            print(f"Stored data for {message.value['name']}")
        except Exception as e:
            print(f"Error storing data: {e}")

if __name__ == "__main__":
    consume_weather_data()