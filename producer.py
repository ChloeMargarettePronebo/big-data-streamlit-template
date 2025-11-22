import argparse
import time
import requests
from kafka import KafkaProducer
import json
import random

def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast?latitude=14.5995&longitude=120.9842&current_weather=true"
    res = requests.get(url, timeout=5)
    data = res.json()
    weather = data["current_weather"]

    return {
        "temperature": weather["temperature"],
        "windspeed": weather["windspeed"],
        "winddirection": weather["winddirection"],
        "timestamp": time.time()
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--rate", type=float, default=1.0)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"✅ Connected to Kafka at {args.bootstrap_servers}")
    print(f"🌦 Sending WEATHER data... Rate: {args.rate} msg/sec")

    while True:
        try:
            weather_data = fetch_weather()
            producer.send(args.topic, weather_data)
            print("📤 Sent:", weather_data)
        except Exception as e:
            print("⚠️ API error:", e)

        time.sleep(1 / args.rate)
