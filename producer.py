from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

customers = [
    {"name": "Alice", "age": 22, "city": "New York"},
    {"name": "Bob", "age": 30, "city": "London"},
    {"name": "Charlie", "age": 19, "city": "Paris"},
    {"name": "David", "age": 27, "city": "Berlin"},
    {"name": "David1", "age": 24, "city": "Berlin"},
    {"name": "David2", "age": 28, "city": "Berlin"},
    {"name": "David3", "age": 24, "city": "Berlin"}
]

while True:
    customer = random.choice(customers)
    print(f"Sending: {customer}")
    producer.send("customer-input", customer)
    time.sleep(2)
