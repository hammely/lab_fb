import os
import time

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
QUEUE = "order-notifications"


def connect():
    """Ждём RabbitMQ и подключаемся."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=10,
    )
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print(f"[worker] RabbitMQ not available at {RABBIT_HOST}, retry in 3s...", flush=True)
            time.sleep(3)


def main():
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        message = body.decode("utf-8")
        print(f"[worker] notification received: {message}", flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE, on_message_callback=callback)
    print(f"[worker] waiting for notifications on queue '{QUEUE}'...", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()