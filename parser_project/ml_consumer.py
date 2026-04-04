import json
from typing import Any

from kafka import KafkaConsumer
from pydantic import BaseModel, ValidationError

from config import load_config
from db import save_ml_error, save_ml_result

CONFIG = load_config()


class MLPayload(BaseModel):
    doc_id: str
    text: str


def validate_ml_consumer_config() -> None:
    missing: list[str] = []
    if not CONFIG.kafka_bootstrap_servers:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    if not CONFIG.kafka_ml_topic:
        missing.append("KAFKA_ML_TOPIC")
    if not CONFIG.kafka_group_id:
        missing.append("KAFKA_GROUP_ID")

    if missing:
        required = ", ".join(missing)
        raise RuntimeError(
            "Не хватает обязательных переменных окружения для ml_consumer: "
            f"{required}. Добавьте их в .env и повторите запуск."
        )


def process_document_for_ml(payload: dict) -> dict:
    text = payload.get("text") or ""
    if not isinstance(text, str):
        text = str(text)

    summary = text[:300]
    score = min(max(len(text) / 1000, 0.0), 1.0)

    result = {
        "summary": summary,
        "score": score,
        "category": "unclassified",
        "model_version": "stub-v1",
        "prompt_version": None,
    }
    result["raw_result"] = dict(result)

    return result


def main() -> None:
    validate_ml_consumer_config()

    consumer = KafkaConsumer(
        CONFIG.kafka_ml_topic,
        bootstrap_servers=CONFIG.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{CONFIG.kafka_group_id}-ml",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Слушаю Kafka topic: {CONFIG.kafka_ml_topic}")
    print("Нажми Ctrl+C, чтобы остановить.")
    print("-" * 80)

    try:
        for message in consumer:
            raw_data: Any = message.value

            try:
                payload = MLPayload.model_validate(raw_data)
            except ValidationError as exc:
                print(f"❌ Ошибка валидации ML payload offset={message.offset}: {exc}")
                doc_id = raw_data.get("doc_id") if isinstance(raw_data, dict) else "unknown"
                save_ml_error(doc_id=str(doc_id), error_message=f"validation_error: {exc}", raw_result={"payload": raw_data})
                continue

            try:
                ml_result = process_document_for_ml(payload.model_dump())
                save_ml_result(
                    doc_id=payload.doc_id,
                    summary=ml_result.get("summary"),
                    score=ml_result.get("score"),
                    category=ml_result.get("category"),
                    model_version=ml_result.get("model_version"),
                    prompt_version=ml_result.get("prompt_version"),
                    raw_result=ml_result.get("raw_result", ml_result),
                )
                print(f"✅ ML результат сохранен: {payload.doc_id}")
            except Exception as exc:
                print(f"❌ Ошибка ML-обработки offset={message.offset}, doc_id={payload.doc_id}: {exc}")
                save_ml_error(
                    doc_id=payload.doc_id,
                    error_message=str(exc),
                    raw_result={"payload": payload.model_dump()},
                )

    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
