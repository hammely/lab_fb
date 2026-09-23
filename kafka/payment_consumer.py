import json
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "order-events"
GROUP_ID = "payment-group"


def create_consumer():
    """Ждём, пока Kafka станет доступна, потом создаём consumer."""
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
            print(f"[payment] Kafka not available at {KAFKA_BOOTSTRAP}, retry in 3s...", flush=True)
            time.sleep(3)


def main():
    consumer = create_consumer()
    print(f"[payment] consumer started, group={GROUP_ID}, topic={TOPIC}", flush=True)

    for msg in consumer:
        order = msg.value
        order_id = order.get("order_id")
        amount = order.get("amount")

        print(
            f"[payment] processing payment for order #{order_id}, "
            f"amount={amount}, partition={msg.partition}, offset={msg.offset}",
            flush=True,
        )
        time.sleep(0.5)  # имитация работы
        print(f"[payment] order #{order_id} PAID", flush=True)


if __name__ == "__main__":
    main()