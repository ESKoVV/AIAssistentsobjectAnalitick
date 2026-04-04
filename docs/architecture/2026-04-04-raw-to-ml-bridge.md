# Raw ingestion → ML bridge (без зависимости от normalization)

Дата: 2026-04-04

## Что изменено

Добавлен отдельный сервис `parser_project/raw_to_ml_bridge.py`, который:
- читает `RawDocument` из Kafka topic `raw.documents` (`KAFKA_RAW_TOPIC`),
- собирает ML payload напрямую из raw-полей,
- пишет в `ml.documents` (`KAFKA_ML_TOPIC`).

`consumer.py` больше не нужен как путь доставки в ML topic.

## Почему выбран Kafka topic, а не PostgreSQL raw_documents

Для текущего проекта наименее рискованный и самый простой путь — читать из Kafka:
- не нужен дополнительный polling/locking слой для БД;
- сохраняется естественная потоковая обработка ingestion → ML;
- проще обеспечить `at-least-once` через ручной `commit` offset только после успешной отправки в ML topic.

Чтение из `raw_documents` потребовало бы отдельного механизма watermark/offset в БД и обработки гонок при параллельных воркерах.

## Схема потока данных

```text
[Collectors]
   ↓ publish RawDocument
Kafka topic: raw.documents
   ├─→ consumer.py → PostgreSQL.raw_documents   (raw persistence)
   └─→ raw_to_ml_bridge.py → Kafka topic: ml.documents → ml_consumer.py → PostgreSQL.ml_results
```

## Гарантия доставки

`raw_to_ml_bridge.py` использует:
- `enable_auto_commit=False` у Kafka consumer;
- commit offset только после `producer.send(...).get(...)` + `flush()`.

Это оставляет семантику `at-least-once`: при сбое до commit сообщение будет прочитано повторно.
