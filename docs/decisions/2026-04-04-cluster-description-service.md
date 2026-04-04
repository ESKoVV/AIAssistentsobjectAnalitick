# Cluster Description Service Decisions

## Context
Для этапа cluster summarization в пайплайне добавляется отдельный сервис, который получает `clusters.updated`, формирует нейтральные описания через LLM и публикует `descriptions.updated`.

## Decisions
- Prompt хранится в `configs/prompts/summarization.md`, парсится из markdown-секций, а `prompt_version` считается как SHA-256 по нормализованным prompt-блокам.
- Для инвалидации кэша хранится `cluster_size_at_generation`, чтобы не оценивать рост кластера по `sample_doc_ids`.
- Репрезентативные документы выбираются по близости embedding к центроиду кластера с лимитом по авторам и prompt token budget.
- Каждая реальная LLM-попытка, включая retry и fallback, пишется в `llm_costs`; итоговая запись описания хранится в `cluster_descriptions`, а предыдущая версия переносится в `cluster_descriptions_history`.
- Мониторинг v1 опирается на `DescriptionMetrics`: fallback usage, validation failure ratio, avg generation latency и estimated token cost.
- Реальные provider adapters для OpenAI/Yandex/Ollama отложены; v1 вводит общий `LLMClient` интерфейс и `FallbackLLMClient`.

## Operational Notes
- `documents_table` остаётся конфигурируемой, чтобы сервис можно было направить либо на текущую таблицу документов, либо на будущий enriched upstream storage.
- При окончательном нарушении правил после retry сервис не блокирует пайплайн: запись сохраняется с `needs_review = true`.
