import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "github_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="demo-printer",
    auto_offset_reset="earliest"
)

print("listening on topic github_events …")
for msg in consumer:
    print(msg.value)
