import requests
from kafka import KafkaProducer
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load configuration
load_dotenv()

class Config:
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    KAFKA_BROKER = os.getenv('KAFKA_BROKER')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

def fetch_weather_data(city_name):
    """Fetch weather data from OpenWeather API"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city_name,
        'appid': Config.OPENWEATHER_API_KEY,
        'units': 'metric'
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def create_kafka_producer():
    """Create Kafka producer with reliability settings"""
    return KafkaProducer(
        bootstrap_servers=[Config.KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

def produce_weather_data():
    """Main producer loop"""
    cities = ['London', 'New York', 'Tokyo', 'Paris', 'Berlin']
    producer = create_kafka_producer()
    
    while True:
        for city in cities:
            data = fetch_weather_data(city)
            if data:
                data['processed_at'] = datetime.utcnow().isoformat()
                producer.send(Config.KAFKA_TOPIC, value=data)
                print(f"Sent data for {city} to Kafka")
            time.sleep(10)
        time.sleep(60)

if __name__ == "__main__":
    produce_weather_data()