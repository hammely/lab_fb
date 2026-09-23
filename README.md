## Архитектура

```
[Браузер] → [nginx] → [Flask API] → [MySQL]
                            ├──→ [Kafka: order-events] → [payment-group]
                            │                          → [delivery-group]
                            └──→ [RabbitMQ: order-notifications] → [notification-worker]
```

## Команда и роли

| Участник | Ветка | Зона ответственности |
|---|---|---|
| Лиза | `lisa` | Инфраструктура: `docker-compose.yml`, `kafka/`, `rabbit/`, `mysql/` |
| Юля | `yulia` | Приложение: `flask-app/`, `html/`, `nginx/` |

Основная ветка — `master`. Работа идёт в личных ветках.

### Таблица MySQL `orders`

Схема (создаётся в `mysql/init.sql`):

```sql
CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer VARCHAR(255) NOT NULL,
  product VARCHAR(255) NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Kafka

**Топик:** `order-events`  
**Партиции:** 3  
**Ключ сообщения:** `order_id` (строка)  
**Формат значения (JSON):**

```json
{
  "order_id": 1,
  "customer": "Иван Иванов",
  "product": "Ноутбук",
  "amount": 999.99,
  "status": "created"
}
```

**Consumer groups:**

| Скрипт | Group ID | Что делает |
|---|---|---|
| `kafka/payment_consumer.py` | `payment-group` | Имитирует обработку оплаты |
| `kafka/delivery_consumer.py` | `delivery-group` | Имитирует подготовку доставки |

Каждый консьюмер — в своей группе, читает один и тот же топик независимо.

### RabbitMQ

**Очередь:** `order-notifications`  
**Формат сообщения (plain text):**

```
Новый заказ #1: Иван Иванов — Ноутбук на 999.99
```

### Имена сервисов в docker-compose

| Сервис | Роль | Порт на хосте |
|---|---|---|
| `mysql` | СУБД | `3306` |
| `kafka` | брокер | `29092` |
| `kafka-init` | одноразовый, создаёт топик | — |
| `rabbitmq` | брокер + UI | `5672`, `15672` |
| `kafka-ui` | веб-UI Kafka | `8080` |
| `api` | Flask | `5000` (внутренний) |
| `nginx` | статика + reverse proxy | `80` |
| `consumer-payment` | Kafka consumer | — |
| `consumer-shipping` | Kafka consumer | — |
| `notification-worker` | RabbitMQ consumer | — |

## Как запустить

```bash
docker-compose up --build
```

Затем открыть:
- форма заказа: `http://localhost`
- Kafka UI: `http://localhost:8080`
- RabbitMQ Management: `http://localhost:15672` (guest / guest)
- phpMyAdmin: `http://localhost:8081/`

## Структура репозитория

```
lab_fb/
├── docker-compose.yml
├── README.md
├── .gitignore
├── flask-app/          # Flask
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── html/               # Форма
│   └── index.html
├── nginx/              # Reverse proxy
│   └── nginx.conf
├── kafka/              # Kafka consumers
│   ├── payment_consumer.py
│   ├── delivery_consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── rabbit/             # RabbitMQ worker
│   ├── worker.py
│   ├── Dockerfile
│   └── requirements.txt
└── mysql/              # Инициализация БД
    └── init.sql
```