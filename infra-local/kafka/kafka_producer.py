#!/usr/bin/env python
"""Kafka producer for a2k-procar car price prediction"""

import json
import random
import argparse
import datetime
from kafka import KafkaProducer

FUEL_TYPES = ["Petrol", "Diesel", "CNG"]
SELLER_TYPES = ["Dealer", "Individual"]
TRANSMISSIONS = ["Manual", "Automatic"]


def generate_car_data():
    """Генерация случайных данных автомобиля"""
    year = random.randint(2005, 2022)
    present_price = round(random.uniform(2.0, 20.0), 2)
    kms_driven = random.randint(10_000, 200_000)
    owner = random.randint(0, 2)
    car_age = datetime.datetime.now().year - year
    kms_per_year = round(kms_driven / max(car_age, 1), 1)

    return {
        "Year": year,
        "Present_Price": present_price,
        "Kms_Driven": kms_driven,
        "Owner": owner,
        "Car_Age": car_age,
        "Kms_Per_Year": kms_per_year,
        "Fuel_Type": random.choice(FUEL_TYPES),
        "Seller_Type": random.choice(SELLER_TYPES),
        "Transmission": random.choice(TRANSMISSIONS),
    }


def main():
    parser = argparse.ArgumentParser(description="Kafka producer for car price data")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="input_car_data", help="Kafka topic name")
    parser.add_argument("--n", type=int, default=10, help="Number of messages to send")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Sending {args.n} car data messages to topic '{args.topic}'...")
    for i in range(args.n):
        data = generate_car_data()
        producer.send(args.topic, value=data)
        print(f"[{i+1}] Sent: {data}")

    producer.flush()
    producer.close()
    print("Done — all messages sent successfully.")


if __name__ == "__main__":
    main()