## Архитектура

```
[Браузер] → [nginx] → [Flask API] → [MySQL]
                            ├──→ [Kafka: order-events] → [payment-consumer]
                            │                          → [delivery-consumer]
                            └──→ [RabbitMQ: order-notifications] → [notification-worker]
```

## Команда и роли

| Участник | Ветка | Зона ответственности |
|---|---|---|
| Лиза | `lisa` | Инфраструктура: `docker-compose.yml`, `kafka/`, `rabbit/`, `mysql/` |
| Юля | `yulia` | Приложение: `flask-app/`, `html/`, `nginx/` |

Основная ветка — `master`. Работа идёт в личных ветках.

## Контракты

### 1. HTTP-запрос от браузера к Flask

**Метод:** `POST /api/orders`  
**Content-Type:** `application/json`

**Тело запроса:**
```json
{
  "customer": "Иван Иванов",
  "product": "Ноутбук",
  "amount": 999.99
}
```

**Успешный ответ:** `201 Created`
```json
{
  "order_id": 1,
  "status": "created"
}
```

**Ответ при ошибке:** `400 Bad Request`
```json
{
  "error": "customer, product and amount are required"
}
```

### 2. Таблица MySQL `orders`

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

Параметры подключения (внутри docker-сети):

| Параметр | Значение |
|---|---|
| host | `mysql` |
| port | `3306` |
| database | `orders_db` |
| user | `app` |
| password | `app_password` |
| root password | `root_password` |

### 3. Kafka

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

Каждый консьюмер — в **своей** группе, читает один и тот же топик независимо.

**Bootstrap servers внутри docker-сети:** `kafka:9092`  
**Bootstrap servers с хоста (для отладки):** `localhost:29092`

### 4. RabbitMQ

**Очередь:** `order-notifications`  
**Формат сообщения (plain text):**

```
Новый заказ #1: Иван Иванов — Ноутбук на 999.99
```

**Параметры подключения (внутри docker-сети):**

| Параметр | Значение |
|---|---|
| host | `rabbitmq` |
| port (AMQP) | `5672` |
| user | `guest` |
| password | `guest` |
| queue | `order-notifications` |

**Management UI:** `http://localhost:15672` (guest / guest)

### 5. Имена сервисов в docker-compose

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

### 6. Порядок запуска

```
mysql (healthy) ─┐
kafka (healthy) ─┼─→ kafka-init (создать топик) ─→ consumers
rabbitmq (healthy) ┘                            ─→ notification-worker
                                                ─→ api ─→ nginx
```

Через `depends_on` + `condition: service_healthy`.

## Как запустить

```bash
docker-compose up --build
```

Затем открыть:
- форма заказа: `http://localhost`
- Kafka UI: `http://localhost:8080`
- RabbitMQ Management: `http://localhost:15672` (guest / guest)

## Структура репозитория

```
lab_fb/
├── docker-compose.yml
├── README.md
├── .gitignore
├── flask-app/          # Flask (Юля)
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── html/               # Форма (Юля)
│   └── index.html
├── nginx/              # Reverse proxy (Юля)
│   └── nginx.conf
├── kafka/              # Kafka consumers (Лиза)
│   ├── payment_consumer.py
│   ├── delivery_consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── rabbit/             # RabbitMQ worker (Лиза)
│   ├── worker.py
│   ├── Dockerfile
│   └── requirements.txt
└── mysql/              # Инициализация БД (Лиза)
    └── init.sql
```

## Распределение работы

- **Юля (`yulia`):** `html/`, `flask-app/`, `nginx/`
- **Лиза (`lisa`):** `docker-compose.yml`, `mysql/`, `kafka/`, `rabbit/`
- **Интеграция Flask → MySQL + Kafka + RabbitMQ:** выполняется в ветке `lisa` после того, как часть Юли попала в `master`.

## План

1. README с контрактами → `master` ✅
2. Часть Юли (`yulia`): форма, Flask-скелет, nginx → PR в `master`
3. Часть Лизы (`lisa`): mysql, kafka, rabbit, docker-compose → PR в `master`
4. Интеграция Flask с реальными сервисами (`lisa`) → PR в `master`
5. Проверка end-to-end, скриншоты
6. Отчёт (`REPORT.md` в `master`)