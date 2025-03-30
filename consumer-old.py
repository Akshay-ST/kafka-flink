from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'old-folks-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for msg in consumer:
    print(f'Old Folks: {msg.value}')