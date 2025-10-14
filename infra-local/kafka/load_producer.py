#!/usr/bin/env python
"""Kafka producer for load testing"""

import json
import random
import time
import threading
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
    car_age = 2024 - year
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

def produce_messages(producer, topic, messages_per_second, duration_seconds):
    """Генерация сообщений с заданной интенсивностью"""
    start_time = time.time()
    message_count = 0
    
    while time.time() - start_time < duration_seconds:
        batch_start = time.time()
        
        for _ in range(messages_per_second):
            data = generate_car_data()
            producer.send(topic, value=data)
            message_count += 1
        
        # Регулируем скорость
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
    
    return message_count

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    print("Starting load test...")
    print("Этапы нагрузки:")
    print("1. Низкая нагрузка: 10 сообщений/сек - 30 сек")
    print("2. Средняя нагрузка: 50 сообщений/сек - 30 сек") 
    print("3. Высокая нагрузка: 200 сообщений/сек - 60 сек")
    print("4. Пиковая нагрузка: 500 сообщений/сек - 30 сек")
    
    # Этап 1: Низкая нагрузка
    print("\n Этап 1: Низкая нагрузка (10/сек)")
    count1 = produce_messages(producer, "input_car_data", 10, 30)
    print(f"Отправлено: {count1} сообщений")
    
    # Этап 2: Средняя нагрузка
    print("\n Этап 2: Средняя нагрузка (50/сек)")
    count2 = produce_messages(producer, "input_car_data", 50, 30)
    print(f"Отправлено: {count2} сообщений")
    
    # Этап 3: Высокая нагрузка
    print("\n Этап 3: Высокая нагрузка (200/сек)")
    count3 = produce_messages(producer, "input_car_data", 200, 60)
    print(f"Отправлено: {count3} сообщений")
    
    # Этап 4: Пиковая нагрузка
    print("\n Этап 4: Пиковая нагрузка (500/сек)")
    count4 = produce_messages(producer, "input_car_data", 500, 30)
    print(f"Отправлено: {count4} сообщений")
    
    producer.flush()
    producer.close()
    
    total = count1 + count2 + count3 + count4
    print(f"\n Нагрузочное тестирование завершено!")
    print(f"Всего отправлено сообщений: {total}")

if __name__ == "__main__":
    main()