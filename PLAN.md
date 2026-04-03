# План preprocessing 1-7 с обязательными тестами на каждую таску

## Базовый контракт

Базовый входной и выходной контракт шага 1 фиксируется как `NormalizedDocument`:

```python
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

class SourceType(str, Enum):
    VK_POST = "vk_post"
    VK_COMMENT = "vk_comment"
    MAX_POST = "max_post"
    MAX_COMMENT = "max_comment"
    RSS_ARTICLE = "rss_article"
    PORTAL_APPEAL = "portal_appeal"

class MediaType(str, Enum):
    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    LINK = "link"

@dataclass
class NormalizedDocument:
    doc_id: str
    source_type: SourceType
    source_id: str
    parent_id: Optional[str]
    text: str
    media_type: MediaType
    created_at: datetime
    collected_at: datetime
    author_id: str
    is_official: bool
    reach: int
    likes: int
    reposts: int
    comments_count: int
    region_hint: Optional[str]
    geo_lat: Optional[float]
    geo_lon: Optional[float]
    raw_payload: dict
```

Все последующие шаги используют этот контракт как основу и добавляют только свои stage-specific поля, не ломая исходные поля документа.

## Как шаги взаимодействуют

1. `Структурная нормализация`
Принимает source-specific raw payload и `source_config`.
Отдает `NormalizedDocument`.
Это единственная точка, где допускается логика форматов VK, MAX, RSS и порталов.

2. `Определение языка`
Принимает `NormalizedDocument`.
Отдает `LanguageAnnotatedDocument = NormalizedDocument + {language, language_confidence, is_supported_language}`.
Шаг 3 читает только эти флаги и исходный `text`.

3. `Контентная фильтрация`
Принимает `LanguageAnnotatedDocument`.
Отдает `FilteredDocument = LanguageAnnotatedDocument + {filter_status, filter_reasons, quality_weight}`.
`drop` сохраняется для аудита, `review` идет дальше с пониженным весом.

4. `Очистка текста`
Принимает `FilteredDocument` со статусом `pass` или `review`.
Отдает `CleanedDocument = FilteredDocument + {normalized_text, token_count, cleanup_flags}`.
Шаг 5 использует только `normalized_text`, а не исходный `text`.

5. `Дедупликация`
Принимает `CleanedDocument`.
Отдает `DeduplicatedDocument = CleanedDocument + {text_sha256, duplicate_group_id, near_duplicate_flag, duplicate_cluster_size, canonical_doc_id}`.
Дубликаты не удаляются, а только связываются в кластер.

6. `Гео-обогащение`
Принимает `DeduplicatedDocument` и `source_config`.
Отдает `GeoEnrichedDocument = DeduplicatedDocument + {region_id, municipality_id, geo_confidence, geo_source, geo_evidence}`.
Порядок гео-источников: explicit geo -> text toponym -> source metadata -> source default.

7. `Разметка метаданных`
Принимает `GeoEnrichedDocument` и registry официальных источников.
Отдает `EnrichedDocument = GeoEnrichedDocument + {engagement, metadata_version}`.
Этот объект становится единственным входом в embeddings/clustering.

## Обязательное правило по тестам

Для каждой таски обязательно пишутся:

- `contract`-тест на входной и выходной контракт этапа
- `unit` или `edge`-тесты на ключевые правила этапа

Минимальная структура тестов:

- `tests/contract/test_structural_normalization.py`
- `tests/contract/test_language_detection.py`
- `tests/contract/test_content_filtering.py`
- `tests/contract/test_text_cleaning.py`
- `tests/contract/test_deduplication.py`
- `tests/contract/test_geo_enrichment.py`
- `tests/contract/test_metadata_enrichment.py`
- `tests/unit/test_structural_normalization.py`
- `tests/unit/test_language_detection.py`
- `tests/unit/test_content_filtering.py`
- `tests/unit/test_text_cleaning.py`
- `tests/unit/test_deduplication.py`
- `tests/unit/test_geo_enrichment.py`
- `tests/unit/test_metadata_enrichment.py`
- `tests/integration/test_preprocessing_pipeline.py`

Таска не считается завершенной, если написан только код или только integration test.

## Обязательные тесты по каждой таске

1. `Task 1 / structural normalization`
- Contract test: VK, MAX, RSS и portal payload приводятся к одному `NormalizedDocument`.
- Edge tests: пустой `text`, отсутствующий `reach`, локальный timestamp, разные форматы `source_id` и `author_id`, комментарий с `parent_id`.

2. `Task 2 / language detection`
- Contract test: на вход подается `NormalizedDocument`, на выходе сохраняются все исходные поля и добавляются языковые поля.
- Unit tests: русский текст, казахский текст, смешанный текст, пустой текст, emoji-only текст.

3. `Task 3 / content filtering`
- Contract test: `drop` не теряет документ, `review` проходит дальше, `filter_reasons` всегда заполнен при `review/drop`.
- Unit tests: короткий шум, явная реклама, спорная жалоба, официальный пост, короткое но релевантное сообщение.

4. `Task 4 / text cleaning`
- Contract test: `text` остается неизменным, а очищенный результат пишется только в `normalized_text`.
- Unit tests: удаление URL, замена `@username` на `USER`, emoji-to-text, нормализация пробелов, одинаковый результат для текстов с разными ссылками.

5. `Task 5 / deduplication`
- Contract test: duplicate record остается отдельной записью, а `duplicate_cluster_size` увеличивается.
- Unit tests: exact duplicate, near-duplicate с другой ссылкой, near-duplicate с опечаткой, похожий но не дубликат.

6. `Task 6 / geo enrichment`
- Contract test: финальный geo выбирается по приоритету источников, а `geo_evidence` объясняет выбор.
- Unit tests: explicit geotag, топоним в тексте, metadata канала, default region, non-ru без text NER.

7. `Task 7 / metadata enrichment`
- Contract test: итоговый объект сохраняет весь `NormalizedDocument` и добавляет расчетные metadata-поля без потери исходных значений.
- Unit tests: official registry hit, media type confirmation, snapshot `reach`, формула `engagement = likes + reposts + comments_count`.

8. `Pipeline integration`
- Positive integration test: один raw input проходит 1-7 и выдает валидный `EnrichedDocument`.
- Negative integration test: документ со статусом `drop` не передается в downstream semantic stages.

## Полные prompts по таскам

1. `Task 1 / structural normalization`
Реализуй deterministic structural normalization для VK, MAX, RSS и portal sources. Вход: raw payload источника плюс `source_config`. Выход: строго `NormalizedDocument` с полями `doc_id`, `source_type`, `source_id`, `parent_id`, `text`, `media_type`, `created_at`, `collected_at`, `author_id`, `is_official`, `reach`, `likes`, `reposts`, `comments_count`, `region_hint`, `geo_lat`, `geo_lon`, `raw_payload`. Все timestamps должны быть в UTC, `raw_payload` должен сохраняться без изменений, source-specific поля не должны утекать downstream. Обязательно напиши `tests/contract/test_structural_normalization.py` и `tests/unit/test_structural_normalization.py` с кейсами для VK, MAX, RSS, portal, пустого текста, отсутствующего reach, разных форматов id и комментария с `parent_id`.

2. `Task 2 / language detection`
Реализуй language detection сразу после structural normalization с использованием `fasttext-lid`. Вход: `NormalizedDocument`. Выход: объект, который сохраняет все поля `NormalizedDocument` и добавляет `language`, `language_confidence`, `is_supported_language`. Поддерживаемым языком preprocessing-правил считается только `ru`; не-русские документы нельзя удалять, их нужно только маркировать. Обязательно напиши `tests/contract/test_language_detection.py` и `tests/unit/test_language_detection.py` с кейсами для русского, казахского, смешанного, пустого и emoji-only текста.

3. `Task 3 / content filtering`
Реализуй rule-based content filtering без LLM и без непрозрачной ML-модели. Вход: документ после language detection. Выход: объект с сохранением базового контракта и добавлением `filter_status`, `filter_reasons`, `quality_weight`. Используй минимальную длину текста, словарь spam-signatures и паттерны рекламных хэштегов. Спорные сообщения, похожие на жалобы, нельзя жестко удалять: они должны получать `review` и идти дальше с меньшим весом. Все правила должны жить в конфиге. Обязательно напиши `tests/contract/test_content_filtering.py` и `tests/unit/test_content_filtering.py` с кейсами для шума, рекламы, спорной жалобы, официального поста и короткого, но значимого сообщения.

4. `Task 4 / text cleaning`
Реализуй text cleaning для документов со статусом `pass` или `review`. Вход: документ после content filtering. Выход: объект с добавлением `normalized_text`, `token_count`, `cleanup_flags`, при этом исходное поле `text` должно остаться без изменений. Нужно полностью удалять URL, заменять `@mentions` на `USER`, конвертировать emoji в текст через `emoji.demojize` и нормализовать пробелы. Результат должен быть пригоден и для embeddings, и для дедупликации. Обязательно напиши `tests/contract/test_text_cleaning.py` и `tests/unit/test_text_cleaning.py` с кейсами на URL, mentions, emoji, пробелы и стабильность текста при разных ссылках.

5. `Task 5 / deduplication`
Реализуй двухуровневую дедупликацию поверх `normalized_text`: exact duplicates через `SHA-256`, near-duplicates через `MinHash LSH`. Вход: документ после text cleaning. Выход: объект с добавлением `text_sha256`, `duplicate_group_id`, `near_duplicate_flag`, `duplicate_cluster_size`, `canonical_doc_id`. Дубликаты запрещено удалять из хранилища: каждая запись должна сохраняться отдельно, а duplicate cluster должен отражать масштаб. Обязательно напиши `tests/contract/test_deduplication.py` и `tests/unit/test_deduplication.py` с кейсами на exact duplicate, near-duplicate со ссылкой, near-duplicate с опечаткой и похожий, но не дублирующий документ.

6. `Task 6 / geo enrichment`
Реализуй deterministic geo enrichment после дедупликации. Вход: документ после deduplication плюс `source_config`. Выход: объект с добавлением `region_id`, `municipality_id`, `geo_confidence`, `geo_source`, `geo_evidence`. География должна определяться строго по приоритету: explicit geotag поста, затем топонимы в тексте, затем метаданные группы/канала, затем default region из конфига. Если язык не поддерживается русским pipeline, text NER должен быть отключен. Обязательно напиши `tests/contract/test_geo_enrichment.py` и `tests/unit/test_geo_enrichment.py` с кейсами на приоритет geo-источников и non-ru fallback.

7. `Task 7 / metadata enrichment`
Реализуй финальную разметку metadata перед embeddings. Вход: документ после geo enrichment и registry официальных аккаунтов/каналов. Выход: `EnrichedDocument`, который сохраняет все поля `NormalizedDocument` и все stage fields, а также добавляет `engagement` и финальные metadata-маркеры. `reach` должен фиксироваться как snapshot на момент сбора, `is_official` должен опираться на registry, а не на эвристику имени. Обязательно напиши `tests/contract/test_metadata_enrichment.py` и `tests/unit/test_metadata_enrichment.py` с кейсами на official registry hit, media type, snapshot reach и расчет engagement.

## Принятые допущения

- Ваш `NormalizedDocument` является официальным контрактом после шага 1.
- Шаги 2-7 не меняют и не удаляют поля `NormalizedDocument`, а только добавляют новые поля этапа.
- Для каждого этапа нужен минимум один отдельный `contract`-тест и один отдельный `unit`-модуль.
- Сквозной integration test обязателен, но не заменяет тесты конкретных тасок.
