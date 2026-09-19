import random
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

# Each location has a realistic "normal" power range (min_kw, max_kw)
LOCATION_PROFILES = {
    "Room-101":     (0.5, 2.0),
    "Room-102":     (0.5, 2.0),
    "Lab-2":        (2.0, 6.0),
    "Library":      (1.5, 4.0),
    "Server-Room":  (8.0, 15.0),
}

ANOMALY_PROBABILITY = 0.05
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "energy-readings"

def generate_reading():
    """Creates one fake smart-meter reading as a dictionary."""
    location = random.choice(list(LOCATION_PROFILES.keys()))
    min_kw, max_kw = LOCATION_PROFILES[location]

    is_anomaly = random.random() < ANOMALY_PROBABILITY

    if is_anomaly:
        power_kw = round(max_kw * random.uniform(2.0, 4.0), 2)
    else:
        power_kw = round(random.uniform(min_kw, max_kw), 2)

    return {
        "location": location,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "power_kw": power_kw,
        "is_anomaly_injected": is_anomaly
    }

def create_producer():
    """Creates and returns a Kafka producer connected to our broker."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

if __name__ == "__main__":
    producer = create_producer()
    reading = generate_reading()

    producer.send(KAFKA_TOPIC, value=reading)
    producer.flush()  # ensures the message is actually sent before the script exits

    print("Sent to Kafka:")
    print(json.dumps(reading, indent=2))
