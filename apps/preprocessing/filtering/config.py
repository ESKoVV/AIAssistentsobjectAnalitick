from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ContentFilterConfig:
    min_text_length: int
    min_meaningful_length: int
    review_weight: float
    velocity_threshold: int
    near_velocity_threshold: int
    author_burst_threshold: int
    velocity_window_minutes: int
    author_burst_window_minutes: int
    noise_markers: tuple[str, ...]
    spam_signatures: tuple[str, ...]
    ad_hashtag_patterns: tuple[str, ...]
    complaint_markers: tuple[str, ...]
    relevant_short_markers: tuple[str, ...]


DEFAULT_CONTENT_FILTER_CONFIG = ContentFilterConfig(
    min_text_length=8,
    min_meaningful_length=20,
    review_weight=0.6,
    velocity_threshold=20,
    near_velocity_threshold=15,
    author_burst_threshold=10,
    velocity_window_minutes=30,
    author_burst_window_minutes=60,
    noise_markers=(
        "ок",
        "кек",
        "ага",
        "лол",
        "test",
        "тест",
        "!!!",
        "...",
    ),
    spam_signatures=(
        "подпишись",
        "подписывайтесь",
        "розыгрыш",
        "скидк",
        "купи",
        "покупай",
        "промокод",
        "доставка 24/7",
        "заработок",
        "переходи по ссылке",
        "пишите в директ",
        "в лс",
        "успей купить",
    ),
    ad_hashtag_patterns=(
        "#реклама",
        "#ad",
        "#sale",
        "#скидки",
        "#продам",
        "#магазин",
    ),
    complaint_markers=(
        "безобраз",
        "кошмар",
        "ужас",
        "опять",
        "никто не",
        "сколько можно",
        "почему до сих пор",
        "жалоб",
        "прошу разобраться",
        "не работает",
        "проблем",
    ),
    relevant_short_markers=(
        "пожар",
        "авар",
        "дтп",
        "света нет",
        "нет воды",
        "яма",
        "прорыв",
        "дым",
        "взрыв",
        "лифт",
        "мост",
        "отключ",
    ),
)
