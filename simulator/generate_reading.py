import random
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

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
SEND_INTERVAL_SECONDS = 3

def generate_reading():
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
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

if __name__ == "__main__":
    producer = create_producer()
    print(f"Starting simulator - sending a reading every {SEND_INTERVAL_SECONDS} seconds.")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            reading = generate_reading()
            producer.send(KAFKA_TOPIC, value=reading)
            producer.flush()

            flag = " <-- ANOMALY" if reading["is_anomaly_injected"] else ""
            print(f"Sent: {reading['location']:12s} {reading['power_kw']:6.2f} kW{flag}")

            time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
        producer.close()
