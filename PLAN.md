# План preprocessing 1-7 с обязательными тестами на каждую таску

## Summary
- Собрать детерминированный preprocessing pipeline: `RawMessageV1 -> NormalizedMessageV1 -> LanguageTaggedMessageV1 -> FilteredMessageV1 -> CleanedMessageV1 -> DeduplicatedMessageV1 -> GeoEnrichedMessageV1 -> EnrichedMessageV1`.
- Для каждой таски обязательны код + тесты. Таска считается незавершенной, если нет минимум одного целевого тестового модуля на ее контракт и ключевые edge cases.
- Тесты делятся на `contract` для входов/выходов этапа и `unit` для правил/эвристик. Сквозной integration test добавляется после сборки всех шагов.

## Взаимодействие шагов
1. `Структурная нормализация`
Принимает `RawMessageV1 {raw_id, source_type, source_config_id, collected_at_utc, payload}`.
Отдает `NormalizedMessageV1 {message_id, source_type, source_id, author_id, text, created_at_utc, reach, source_metrics, raw_payload, pipeline_version}`.
Шаг 2 работает только с каноническими полями.

2. `Определение языка`
Принимает `NormalizedMessageV1.text`.
Отдает те же поля плюс `language`, `language_confidence`, `is_supported_language`.
Шаг 3 использует этот флаг для выбора русских правил.

3. `Контентная фильтрация`
Принимает нормализованный текст и языковые флаги.
Отдает `filter_status = pass|review|drop`, `filter_reasons[]`, `quality_weight`.
`drop` остается в storage для аудита, `review` идет дальше с пониженным весом.

4. `Очистка текста`
Принимает сообщения со статусом `pass` или `review`.
Отдает `normalized_text`, `token_count`, `cleanup_flags`.
Шаг 5 использует только `normalized_text`.

5. `Дедупликация`
Принимает `normalized_text`, `created_at_utc`, `source_id`.
Отдает `text_sha256`, `duplicate_group_id`, `near_duplicate_flag`, `duplicate_cluster_size`, `canonical_message_id`.
Дубликаты не удаляются.

6. `Гео-обогащение`
Принимает deduplicated message и `source_config`.
Отдает `region_id`, `municipality_id`, `geo_confidence`, `geo_source`, `geo_evidence`.
Порядок гео-источников жесткий: explicit geotag -> text toponym -> source metadata -> source default.

7. `Разметка метаданных`
Принимает geo-enriched message и source registry.
Отдает финальный `EnrichedMessageV1` с `is_official`, `media_type`, `reach`, `engagement`.
Этот контракт является единственным входом в embeddings/clustering.

## Обязательные тесты по каждой таске
1. `Task 1 / normalization`
- Contract test: VK, Telegram и RSS приводятся к одному каноническому набору полей.
- Edge tests: пустой `text`, отсутствующий `reach`, локальный timestamp, разные author/source id форматы.

2. `Task 2 / language detection`
- Unit test: русский, казахский, смешанный, пустой, emoji-only текст.
- Contract test: не-русский текст не удаляется и получает `is_supported_language=false`.

3. `Task 3 / content filtering`
- Unit test: короткий шум, явная реклама, спорная жалоба, официальный пост.
- Contract test: `review` проходит дальше, `drop` остается аудируемым.

4. `Task 4 / text cleaning`
- Unit test: удаление URL, замена mention на `USER`, emoji-to-text, нормализация пробелов.
- Stability test: одинаковые тексты с разными ссылками дают одинаковый `normalized_text`.

5. `Task 5 / deduplication`
- Unit test: exact duplicate, near-duplicate с другой ссылкой, near-duplicate с опечаткой, похожий но не дубль.
- Contract test: запись остается в базе, а размер duplicate cluster увеличивается.

6. `Task 6 / geo enrichment`
- Priority test: geotag важнее NER, NER важнее channel metadata, metadata важнее default region.
- Fallback test: для non-ru отключается text NER, но работают explicit geo и source fallback.

7. `Task 7 / metadata enrichment`
- Unit test: official registry hit, media type extraction, reach snapshot, engagement formula.
- Contract test: итоговый `EnrichedMessageV1` содержит весь обязательный минимум из `AGENT.md`.

8. `Pipeline integration`
- Один сквозной integration test: сырое сообщение проходит 1-7 и выдает валидный `EnrichedMessageV1`.
- Один negative integration test: `drop` не попадает в downstream semantic stages.

## Полные prompts по таскам
1. `Task 1 / structural normalization`
Разработай deterministic structural normalization для VK, Telegram и RSS. Вход: raw payload источника плюс `source_config`. Выход: `NormalizedMessageV1` с каноническими полями `text`, `created_at_utc`, `source_type`, `source_id`, `author_id`, `reach`, `source_metrics`, `raw_payload`, `pipeline_version`. Все timestamps переводи в UTC, source-specific поля не выпускай downstream, raw payload храни только для аудита. Обязательно напиши тесты: contract test на единый выходной контракт для трех источников и edge tests для пустого текста, отсутствующего reach и разных timestamp/source id форматов.

2. `Task 2 / language detection`
Реализуй language detection сразу после normalization с использованием `fasttext-lid`. Вход: `NormalizedMessageV1.text`. Выход: `language`, `language_confidence`, `is_supported_language`, где поддерживаемым для русского preprocessing считается только `ru`. Не-русские сообщения нельзя удалять: их нужно сохранить и пометить флагом. Обязательно напиши тесты: русский, казахский, смешанный, пустой и emoji-only текст, а также contract test, подтверждающий, что non-ru сообщение сохраняется и корректно маркируется.

3. `Task 3 / content filtering`
Реализуй rule-based content filtering без LLM. Используй минимальную длину текста, словарь spam-signatures и паттерны рекламных хэштегов, чтобы выставлять `filter_status = pass|review|drop`, `filter_reasons[]` и `quality_weight`. Спорные сообщения, похожие на жалобы, нельзя жестко выбрасывать: они должны получать `review` и идти дальше с меньшим весом. Все правила храни в конфиге. Обязательно напиши тесты: короткий шум, явная реклама, спорная жалоба, официальный пост, плюс contract test на поведение `review` и `drop`.

4. `Task 4 / text cleaning`
Реализуй text cleaning для сообщений со статусом `pass` или `review`. Нужно полностью удалять URL, заменять `@mentions` на `USER`, конвертировать emoji в текст через `emoji.demojize`, нормализовать пробелы и выпускать `normalized_text`, `token_count`, `cleanup_flags`. Очищенный текст должен быть пригоден и для embeddings, и для дедупликации. Обязательно напиши тесты на удаление URL, замену mentions, emoji-to-text, нормализацию пробелов и стабильность `normalized_text` для текстов, отличающихся только ссылками.

5. `Task 5 / deduplication`
Реализуй двухуровневую дедупликацию поверх `normalized_text`: exact duplicates через `SHA-256`, near-duplicates через `MinHash LSH`. На выходе нужны `text_sha256`, `duplicate_group_id`, `near_duplicate_flag`, `duplicate_cluster_size`, `canonical_message_id`. Дубликаты нельзя удалять из хранилища: они должны оставаться отдельными сообщениями, но увеличивать размер duplicate cluster. Обязательно напиши тесты для exact duplicate, near-duplicate с другой ссылкой, near-duplicate с опечаткой и похожего, но не дублирующего сообщения, а также contract test, что duplicate cluster size растет без удаления записи.

6. `Task 6 / geo enrichment`
Реализуй deterministic geo enrichment после дедупликации. География должна определяться строго в порядке надежности: explicit geotag поста, затем топонимы в тексте, затем метаданные группы/канала, затем default region из `source_config`. На выходе нужны `region_id`, `municipality_id`, `geo_confidence`, `geo_source`, `geo_evidence`. Если язык не поддерживается русским pipeline, text NER пропускается. Обязательно напиши тесты на приоритет каждого geo-источника и fallback-сценарии для non-ru сообщений.

7. `Task 7 / metadata enrichment`
Реализуй финальную разметку source metadata перед embeddings. Нужно финализировать `is_official`, `media_type`, `reach`, `engagement`, используя collector metadata и registry официальных источников, не перетирая более надежные значения. Итогом должен быть полностью валидный `EnrichedMessageV1`. Обязательно напиши тесты на official registry hit, media type extraction, snapshot `reach`, формулу `engagement` и contract test на полный финальный объект.

## Assumptions
- Для каждой таски минимум: 1 contract test и 1+ unit/edge test набора.
- Тесты размещаются по слоям: `tests/contract` для stage contracts, `tests/unit` для правил и трансформаций, `tests/integration` для сквозного пайплайна.
- Таска не считается выполненной без собственного тестового покрытия, даже если есть общий integration test.
