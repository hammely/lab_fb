"""
Flask-приложение: принимает заказы с формы.

На данном этапе — скелет:
- валидирует входные данные
- возвращает фиктивный order_id

На этапе интеграции (в ветке lisa) сюда добавим:
- запись в MySQL
- отправку в Kafka
- отправку в RabbitMQ
"""
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"error": "JSON body is required"}), 400

    customer = data.get("customer")
    product = data.get("product")
    amount = data.get("amount")

    if not customer or not product or amount is None:
        return jsonify({"error": "customer, product and amount are required"}), 400

    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({"error": "amount must be a number"}), 400

    # TODO (интеграция): запись в MySQL, отправка в Kafka и RabbitMQ
    order_id = 1  # заглушка

    print(f"[api] new order from '{customer}': {product} on {amount}", flush=True)

    return jsonify({"order_id": order_id, "status": "created"}), 201

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
