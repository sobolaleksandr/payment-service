# Payment Service — асинхронный процессинг платежей

Микросервис принимает запрос на оплату, сразу отвечает `202 Accepted`, обрабатывает платёж в фоне через эмулятор платёжного шлюза и сообщает результат на `webhook_url` клиента.

Стек: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL 17, RabbitMQ 4.3 через FastStream, Alembic, Docker Compose.

## Быстрый старт

```bash
docker compose up --build -d
docker compose ps
```

| Что | Адрес |
|---|---|
| API и Swagger UI | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 (логин и пароль `payments` / `payments`) |
| Демо-приёмник вебхуков | http://localhost:8080/webhook (история полученных вебхуков) |

API-ключ по умолчанию — `dev-api-key-change-me`. Его, как и порты и учётные данные, можно переопределить в `.env` (образец — `.env.example`).

## Примеры

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key-change-me" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://webhook-receiver:8080/webhook"
      }'
```

```http
HTTP/1.1 202 Accepted

{"payment_id": "6d17af6f-c8b4-4dff-89fb-4213549e306b", "status": "pending", "created_at": "2026-11-10T08:04:26.063375Z"}
```

`webhook-receiver` — демо-сервис из этого же compose, поэтому адрес указан по имени контейнера.

### Повтор запроса с тем же `Idempotency-Key`

Тот же запрос вернёт тот же платёж (с текущим статусом) и заголовок `Idempotent-Replayed: true`. Новый платёж не создаётся, событие повторно не публикуется.

Если ключ тот же, а тело другое, сервис ответит `422`:

```json
{"detail": "Idempotency key 'order-42' was already used with a different request body"}
```

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/6d17af6f-c8b4-4dff-89fb-4213549e306b \
  -H "X-API-Key: dev-api-key-change-me"
```

```json
{
  "payment_id": "6d17af6f-c8b4-4dff-89fb-4213549e306b",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ #42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-receiver:8080/webhook",
  "created_at": "2026-11-10T08:04:26.063375Z",
  "processed_at": "2026-11-10T08:04:29.512004Z"
}
```

Сумма передаётся строкой, чтобы не терять точность на `float`. На вход принимается и число, и строка; больше двух знаков после запятой — ошибка валидации.

### Вебхук

`POST` на `webhook_url` с заголовком `X-Payment-Id`:

```json
{
  "event": "payment.succeeded",
  "payment": { "...": "те же поля, что в GET /api/v1/payments/{id}" }
}
```

Для отклонённого платежа `event` будет `payment.failed`. Успешной доставкой считается любой ответ `2xx`.

### Retry и DLQ на демо-приёмнике

Приёмник умеет отвечать `500` первые N раз: `http://webhook-receiver:8080/webhook?fail_times=N`.

- `fail_times=2` — первые две попытки падают, вебхук доставляется с третьей (через 2 с, затем через 4 с).
- `fail_times=10` — все три попытки падают, сообщение уходит в `payments.new.dlq`.

```bash
docker compose logs -f consumer             # попытки и задержки
curl http://localhost:8080/webhook          # что и когда дошло до приёмника
```

Сообщения в DLQ видны в RabbitMQ UI: **Queues → payments.new.dlq → Get messages**. Номер попытки лежит в заголовке `x-attempt`, текст последней ошибки — в `x-error`.

## Архитектура

```mermaid
flowchart LR
    client([Клиент]) -->|"POST /api/v1/payments<br/>X-API-Key, Idempotency-Key"| api[api]
    api -->|"одна транзакция:<br/>INSERT payments + INSERT outbox"| pg[(PostgreSQL)]
    relay[outbox-relay] -->|"SELECT … FOR UPDATE SKIP LOCKED"| pg
    relay -->|"publish: confirm + mandatory"| ex{{payments}}
    ex -->|payments.new| q[[payments.new]]
    q --> consumer[consumer]
    consumer -->|"UPDATE status"| pg
    consumer -->|"POST webhook"| hook([webhook_url])
    consumer -->|"ошибка, попытка < 3"| rex{{payments.retry}}
    rex --> rq[["payments.new.retry.Nms<br/>(TTL)"]]
    rq -->|"dead-letter по TTL"| ex
    consumer -->|"ошибка, попытка = 3"| dlx{{payments.dlx}}
    q -.->|"reject: невалидное сообщение"| dlx
    dlx --> dlq[[payments.new.dlq]]
```

| Сервис | Роль |
|---|---|
| `api` | FastAPI: создание и чтение платежей. С RabbitMQ не работает вообще: принимает платежи, даже когда брокер недоступен. |
| `outbox-relay` | Переносит события из таблицы `outbox` в RabbitMQ. |
| `consumer` | Единственный обработчик `payments.new`: шлюз → статус в БД → вебхук → retry/DLQ. |
| `migrate` | Одноразовый `alembic upgrade head`; `api`, `outbox-relay` и `consumer` стартуют после него. |
| `webhook-receiver` | Демо-приёмник вебхуков для проверки, в боевой схеме не нужен. |

### Путь платежа

1. **API.** В одной транзакции выполняется `INSERT INTO payments … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *` и, если строка вставилась, `INSERT INTO outbox`. Ответ — `202` со статусом `pending`.
2. **Outbox relay.** Каждые 0,5 с relay берёт пачку неопубликованных событий (`SELECT … WHERE published_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED`). Он публикует их в exchange `payments` с publisher confirms, `mandatory=True` и `delivery_mode=persistent`, а успешно подтверждённые помечает `published_at`. Неподтверждённые остаются в outbox до следующего прохода.
3. **Consumer.** Получает `{event_id, payment_id, occurred_at}`, читает платёж из БД. Если статус `pending`, вызывает шлюз (2–5 с, 90% успех) и сохраняет результат через `UPDATE … WHERE status = 'pending'`. Затем отправляет вебхук и подтверждает (ack) сообщение.

### Топология RabbitMQ

| Объект | Тип | Аргументы | Назначение |
|---|---|---|---|
| `payments` | direct exchange | durable | основной обменник событий |
| `payments.new` | queue, binding `payments.new` | `x-dead-letter-exchange=payments.dlx` | очередь consumer'а |
| `payments.retry` | direct exchange | durable | маршрутизация в очереди задержки |
| `payments.new.retry.2000ms`, `…4000ms` | queue | `x-message-ttl`, `x-dead-letter-exchange=payments` | отложенный повтор: по истечении TTL сообщение возвращается в `payments.new` |
| `payments.dlx` | direct exchange | durable | dead-letter обменник |
| `payments.new.dlq` | queue, binding `payments.new` | — | окончательно необработанные сообщения |

Топологию объявляет consumer при старте, relay — только свой exchange. Очереди и exchange'и durable, сообщения persistent.

## Гарантии доставки и идемпотентность

### Outbox

Запись платежа и события атомарна: событие не может потеряться между `COMMIT` и публикацией, и не может появиться событие без платежа. Relay публикует с подтверждением брокера и только после этого ставит `published_at`.

`mandatory=True` вместе с `on_return_raises` означает, что событие, которое брокеру некуда положить (например, consumer ещё ни разу не стартовал и очереди нет), не пропадёт молча. Relay получит `NO_ROUTE`, оставит событие в outbox и повторит его с экспоненциальной паузой (до 10 с).

`FOR UPDATE SKIP LOCKED` позволяет запускать несколько relay: каждый берёт свои строки, ожидания нет. Опрос идёт по частичному индексу `ix_outbox_unpublished … WHERE published_at IS NULL`, так что размер истории на скорость не влияет.

### At-least-once и идемпотентный consumer

Сквозная гарантия — at-least-once. Дубли возможны в двух местах:

- relay упал после подтверждения брокера, но до `COMMIT` с `published_at` — событие уйдёт повторно;
- consumer упал после вебхука, но до ack — RabbitMQ доставит сообщение снова.

Оба случая безопасны. Шлюз вызывается только для `pending`, а статус меняется условным `UPDATE … WHERE status = 'pending' RETURNING`. Если два экземпляра всё-таки обработали один платёж параллельно, побеждает первый `UPDATE`, второй берёт уже сохранённый статус. Повторно уходит только вебхук, поэтому получателю стоит дедуплицировать по `payment_id` (он же в `X-Payment-Id`). С реальным шлюзом в него передавался бы `payment_id` как ключ идемпотентности, и тогда повторное списание было бы исключено и на стороне шлюза.

### Idempotency-Key

- Заголовок обязателен (без него — `422`), длина от 1 до 255 символов.
- Уникальность гарантирует `UNIQUE (idempotency_key)` в БД, а не проверка в коде. `ON CONFLICT DO NOTHING` корректно отрабатывает конкурентные запросы: второй ждёт коммита первого и получает его платёж. Это покрыто тестом на 10 одновременных запросов.
- Вместе с ключом хранится SHA-256 нормализованного тела. Повтор с тем же телом возвращает исходный платёж и `Idempotent-Replayed: true`, с другим телом — `422` (как в IETF draft `Idempotency-Key`). Тела с `"amount": 1500.5` и `"1500.50"` считаются одинаковыми.

### Retry и DLQ

- Отказ шлюза (10%) — это бизнес-результат, а не ошибка: платёж получает статус `failed`, клиенту уходит вебхук `payment.failed`, повторов нет.
- Технические ошибки (вебхук ответил не `2xx` или не ответил, БД недоступна) повторяются. Всего три попытки; задержки перед второй и третьей — 2 и 4 с (`RETRY_BASE_DELAY * 2^(n-1)`).
- Повтор делается через брокер, а не через `sleep` в обработчике. Consumer публикует копию сообщения с `x-attempt = n + 1` в очередь задержки и подтверждает оригинал. Пока сообщение ждёт, consumer его не держит: слот prefetch свободен, падение процесса ничего не теряет. Повтор не вызывает шлюз ещё раз, потому что статус уже финальный.
- Для каждой задержки своя очередь с фиксированным TTL. В одной очереди с per-message TTL сообщения истекают только из головы очереди, и короткая задержка ждала бы длинную. Имя очереди содержит задержку: если поменять `RETRY_BASE_DELAY`, появятся новые очереди, а не ошибка `PRECONDITION_FAILED` при повторном объявлении.
- В `payments.new.dlq` сообщения попадают тремя путями:
  - исчерпаны попытки (`x-attempt`, `x-error`);
  - постоянная ошибка, например платежа с таким id нет: сразу, без повторов;
  - сообщение не проходит валидацию: FastStream делает `reject`, и RabbitMQ сам кладёт его в DLQ через `x-dead-letter-exchange` (заголовок `x-death`).
- Сообщение в DLQ не означает потерянный платёж: его статус в БД финальный, и клиент может узнать его через `GET`. Не доставлено только уведомление. После устранения причины сообщения можно вернуть в `payments.new` (shovel или Move messages в UI).

### Аутентификация

Статический ключ в `X-API-Key` проверяется для всех эндпоинтов `/api/v1/*` сравнением за постоянное время (`secrets.compare_digest`). Без ключа или с неверным ключом ответ — `401`. Открыт только `/health`: это liveness-проба для оркестратора, данных она не отдаёт.

## Конфигурация

Каждый процесс читает только свои переменные окружения (классы в `app/config.py`).

| Переменная | По умолчанию | Кто читает |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | все |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | relay, consumer |
| `API_KEY` | — (обязательна, от 8 символов) | api |
| `LOG_LEVEL` | `INFO` | все |
| `OUTBOX_BATCH_SIZE` / `OUTBOX_POLL_INTERVAL` | `100` / `0.5` с | relay |
| `CONSUMER_PREFETCH` | `20` — сколько платежей consumer обрабатывает одновременно | consumer |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_DELAY` | `3` / `2` с | consumer |
| `WEBHOOK_TIMEOUT` | `5` с | consumer |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` / `GATEWAY_SUCCESS_RATE` | `2` / `5` / `0.9` | consumer |

## Разработка и тесты

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

pytest                                  # unit-тесты, инфраструктура не нужна

docker compose up -d postgres           # + интеграционные тесты на настоящем PostgreSQL
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test pytest

ruff check . && ruff format --check . && mypy app tests
```

Базу `payments_test` тесты создают сами и накатывают на неё миграции.

**Unit-тесты** проверяют:
- валидацию и нормализацию тела запроса;
- логику consumer'а (повторная обработка не вызывает шлюз, гонка двух consumer'ов);
- решения retry/DLQ;
- формат вебхука;
- backoff relay;
- аутентификацию.

**Интеграционные тесты** проверяют:
- атомарную запись платежа и outbox;
- все сценарии `Idempotency-Key`, включая конкурентные запросы;
- relay: частичный сбой публикации и два relay параллельно без двойной публикации;
- однократное завершение платежа.

## Структура

```
app/
  api/            FastAPI: create_app, эндпоинты, зависимости (auth, сессия)
  messaging/      брокер и топология RabbitMQ, RetryPolicy
  outbox/         relay и его FastStream-приложение
  worker/         consumer: обработчик сообщений, процессор, эмулятор шлюза, вебхуки
  devtools/       демо-приёмник вебхуков
  models.py       ORM: payments, outbox
  schemas.py      Pydantic: API, событие, вебхук
  repositories.py SQL-запросы
  services.py     создание платежа: идемпотентность + outbox
migrations/       Alembic
tests/unit, tests/integration
```

## Решения и что я бы добавил для продакшена

- **Relay — отдельный процесс**, а не фоновая задача в API. API не зависит от брокера, а relay масштабируется и перезапускается независимо. Для меньших задержек вместо опроса можно будить relay через `LISTEN/NOTIFY` или перейти на CDC (Debezium).
- **Очистка outbox.** Опубликованные строки стоит удалять по расписанию или партиционировать таблицу по `created_at` и отрезать старые партиции.
- **Quorum-очереди с `x-delivery-limit`** вместо classic — для кластера и защиты от «ядовитых» сообщений, которые валят процесс целиком: при таком сбое счётчик `x-attempt` не растёт. Для одного узла в compose оставлены classic.
- **Вебхуки:** HMAC-подпись тела, защита от SSRF (запрет приватных адресов в `webhook_url`), jitter в задержках, circuit breaker на неотвечающих получателей.
- **Idempotency-Key** с областью действия на клиента (при нескольких API-ключах) и TTL, как у платёжных провайдеров.
- **Наблюдаемость:** метрики Prometheus и трейсинг OpenTelemetry через middleware FastStream, структурированные JSON-логи, алерт на рост `payments.new.dlq` и на возраст самой старой неопубликованной записи outbox.
