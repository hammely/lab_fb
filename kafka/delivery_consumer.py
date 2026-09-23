import json
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "order-events"
GROUP_ID = "delivery-group"


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )
        except NoBrokersAvailable:
            print(f"[delivery] Kafka not available at {KAFKA_BOOTSTRAP}, retry in 3s...", flush=True)
            time.sleep(3)


def main():
    consumer = create_consumer()
    print(f"[delivery] consumer started, group={GROUP_ID}, topic={TOPIC}", flush=True)

    for msg in consumer:
        order = msg.value
        order_id = order.get("order_id")
        product = order.get("product")

        print(
            f"[delivery] preparing shipment for order #{order_id} "
            f"({product}), partition={msg.partition}, offset={msg.offset}",
            flush=True,
        )
        time.sleep(0.5)
        print(f"[delivery] order #{order_id} READY FOR SHIPMENT", flush=True)


if __name__ == "__main__":
    main()