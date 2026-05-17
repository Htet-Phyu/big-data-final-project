

# 🎯 Big picture: fetching real-time data → Convert to JSON → Send to Kafka every 1 second

import json
import requests
from kafka import KafkaProducer

TOPIC = "wiki-events"
KAFKA_SERVER = "kafka:9092"
URL = "https://stream.wikimedia.org/v2/stream/recentchange"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Connecting to Wikimedia stream...")

headers = {
    "Accept": "text/event-stream",
    "User-Agent": "MIU-Big-Data-Student-Project/1.0 (educational project)"
}

response = requests.get(URL, stream=True, headers=headers)

for line in response.iter_lines(decode_unicode=True):
    if line:
        # print("RAW:", line)

        if line.startswith("data:"):
            try:
                data = line.replace("data:", "").strip()
                event = json.loads(data)

                simplified_event = {
                    "wiki": event.get("wiki"),
                    "title": event.get("title"),
                    "user": event.get("user"),
                    "type": event.get("type"),
                    "bot": event.get("bot"),
                    "timestamp": str(event.get("timestamp"))
                }

                producer.send(TOPIC, simplified_event)
                producer.flush()

                print("Sent:", simplified_event)

            except Exception as e:
                print("Error:", e)