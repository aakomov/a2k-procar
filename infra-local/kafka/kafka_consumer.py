#!/usr/bin/env python
"""Kafka consumer for a2k-procar — reads car data, calls FastAPI /predict, writes results back."""

import json
import requests
import argparse
from kafka import KafkaConsumer, KafkaProducer

API_URL = "http://localhost:8000/predict"  # URL твоего FastAPI сервиса


def main():
    parser = argparse.ArgumentParser(description="Kafka consumer for a2k-procar predictions")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--input_topic", default="input_car_data", help="Topic to consume from")
    parser.add_argument("--output_topic", default="predictions", help="Topic to publish predictions")
    parser.add_argument("--group_id", default="a2k-procar-consumer", help="Kafka consumer group ID")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.input_topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest"
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"🔌 Connected to Kafka at {args.bootstrap}")
    print(f"🚀 Listening to topic '{args.input_topic}' and sending predictions to '{args.output_topic}'")

    for msg in consumer:
        car_data = msg.value
        try:
            response = requests.post(API_URL, json=car_data, timeout=5)
            if response.status_code == 200:
                prediction = response.json().get("prediction", None)
                if prediction is not None:
                    result = {"car_data": car_data, "predicted_price": prediction}
                    producer.send(args.output_topic, value=result)
                    print(f"✅ Predicted: {prediction} for {car_data}")
                else:
                    print(f"⚠️ No 'prediction' field in API response: {response.text}")
            else:
                print(f"❌ API error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"💥 Request failed: {e}")


if __name__ == "__main__":
    main()