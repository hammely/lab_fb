import json
import os
import time

import mysql.connector
import pika
from flask import Flask, jsonify, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "orders_db")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app_password")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = "order-events"

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBIT_QUEUE = "order-notifications"


_producer = None


def get_kafka_producer():
    """Возвращает singleton KafkaProducer, ждёт, пока Kafka поднимется."""
    global _producer
    if _producer is not None:
        return _producer

    while True:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                key_serializer=lambda k: str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
            print(f"[api] connected to Kafka at {KAFKA_BOOTSTRAP}", flush=True)
            return _producer
        except NoBrokersAvailable:
            print(f"[api] Kafka not available, retry in 3s...", flush=True)
            time.sleep(3)


def get_mysql_connection():
    """Открывает новое соединение с MySQL (с ретраями)."""
    while True:
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DB,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
            )
            return conn
        except mysql.connector.Error as e:
            print(f"[api] MySQL not available ({e}), retry in 3s...", flush=True)
            time.sleep(3)


def get_rabbit_channel():
    """Открывает соединение с RabbitMQ и объявляет очередь (durable)."""
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
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
            print(f"[api] connected to RabbitMQ at {RABBIT_HOST}", flush=True)
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print(f"[api] RabbitMQ not available, retry in 3s...", flush=True)
            time.sleep(3)


@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json(silent=True)

    # 1. Валидация
    if not data:
        return jsonify({"error": "JSON body is required"}), 400

    customer = (data.get("customer") or "").strip()
    product = (data.get("product") or "").strip()
    amount_raw = data.get("amount")

    if not customer or not product or amount_raw is None:
        return jsonify({"error": "customer, product and amount are required"}), 400

    try:
        amount = float(amount_raw)
        if amount <= 0:
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"error": "amount must be a positive number"}), 400

    # 2. Запись в MySQL
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders (customer, product, amount) VALUES (%s, %s, %s)",
            (customer, product, amount),
        )
        conn.commit()
        order_id = cursor.lastrowid
        cursor.close()
        conn.close()
    except mysql.connector.Error as e:
        print(f"[api] MySQL insert failed: {e}", flush=True)
        return jsonify({"error": "database error"}), 500

    print(f"[api] order #{order_id} saved to MySQL", flush=True)

    # 3. Событие в Kafka
    event = {
        "order_id": order_id,
        "customer": customer,
        "product": product,
        "amount": amount,
        "status": "created",
    }
    try:
        producer = get_kafka_producer()
        producer.send(KAFKA_TOPIC, key=order_id, value=event)
        producer.flush()
        print(f"[api] order #{order_id} published to Kafka topic '{KAFKA_TOPIC}'", flush=True)
    except Exception as e:
        # Не роняем HTTP-ответ: заказ уже в БД, событие можно переотправить позже
        print(f"[api] Kafka send failed for order #{order_id}: {e}", flush=True)

    # 4. Уведомление в RabbitMQ
    notification = f"Новый заказ #{order_id}: {customer} — {product} на {amount}"
    try:
        connection, channel = get_rabbit_channel()
        channel.basic_publish(
            exchange="",
            routing_key=RABBIT_QUEUE,
            body=notification.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        connection.close()
        print(f"[api] notification for order #{order_id} sent to RabbitMQ", flush=True)
    except Exception as e:
        print(f"[api] RabbitMQ send failed for order #{order_id}: {e}", flush=True)

    # 5. Ответ
    return jsonify({"order_id": order_id, "status": "created"}), 201


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)