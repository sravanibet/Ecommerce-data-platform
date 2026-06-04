import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "orders"

PRODUCT_CATEGORIES = [
    "electronics",
    "fashion",
    "beauty",
    "home",
    "groceries",
]

STATUSES = ["placed", "confirmed", "shipped", "delivered"]


def create_order_event(order_id: int) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "order_id": order_id,
        "customer_id": random.randint(1000, 1010),
        "product_id": random.randint(2000, 2010),
        "category": random.choice(PRODUCT_CATEGORIES),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10.0, 250.0), 2),
        "status": random.choice(STATUSES),
        "event_time": now.isoformat(),
        "event_id": str(uuid.uuid4()),
    }


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def main() -> None:
    producer = make_producer()
    print(f"Producing events to Kafka topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")

    for order_id in range(1, 41):
        event = create_order_event(order_id)
        producer.send(TOPIC, event)
        print(f"Sent event {order_id}: {event}")
        time.sleep(1)

    producer.flush()
    producer.close()
    print("Finished producing order events.")


if __name__ == "__main__":
    main()
