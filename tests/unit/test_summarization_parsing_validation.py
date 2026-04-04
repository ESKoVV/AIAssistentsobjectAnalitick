from __future__ import annotations

from apps.ml.summarization.parsing import parse_response
from apps.ml.summarization.validation import validate_description
from tests.helpers import build_summarization_document_record


def test_parse_response_handles_multiline_sections() -> None:
    response = """
    ОПИСАНИЕ: Жители обсуждают перебои с горячей водой в жилых домах.
    Упоминаются сроки восстановления подачи и адреса домов.
    ФРАЗЫ: нет горячей воды; сроки восстановления подачи; адреса домов; ремонтные работы; подача воды
    """

    summary, key_phrases = parse_response(response)

    assert "Жители обсуждают перебои" in summary
    assert key_phrases == [
        "нет горячей воды",
        "сроки восстановления подачи",
        "адреса домов",
        "ремонтные работы",
        "подача воды",
    ]


def test_parse_response_falls_back_to_first_paragraph_when_labels_are_missing() -> None:
    response = (
        "Жители обсуждают перебои с горячей водой и сроки завершения ремонта.\n\n"
        "Дополнительно перечислены адреса домов."
    )

    summary, key_phrases = parse_response(response)

    assert summary == "Жители обсуждают перебои с горячей водой и сроки завершения ремонта."
    assert key_phrases == []


def test_validation_rejects_forbidden_words() -> None:
    documents = [
        build_summarization_document_record(
            text=(
                "Жители пишут, что нет горячей воды, обсуждают адреса домов, сроки восстановления "
                "подачи и график ремонтных работ."
            ),
        ),
    ]

    result = validate_description(
        "Жители обсуждают проблема с горячей водой и называют сроки восстановления подачи в домах.",
        [
            "нет горячей воды",
            "сроки восстановления подачи",
            "адреса домов",
            "ремонтных работ",
            "график работ",
        ],
        documents,
    )

    assert result.valid is False
    assert result.reason == "запрещённые слова: проблема"


def test_validation_rejects_key_phrase_that_is_not_verbatim() -> None:
    documents = [
        build_summarization_document_record(
            text=(
                "Жители пишут, что нет горячей воды, обсуждают сроки восстановления подачи, "
                "адреса домов, ремонтные работы и график отключения."
            ),
        ),
    ]

    result = validate_description(
        (
            "Жители обсуждают перебои с горячей водой и сроки восстановления подачи в домах. "
            "В сообщениях перечисляются адреса домов и график ремонтных работ."
        ),
        [
            "нет горячей воды",
            "сроки восстановления подачи",
            "адреса домов",
            "ремонтные работы",
            "неверифицированная фраза",
        ],
        documents,
    )

    assert result.valid is False
    assert result.reason == "ключевая фраза не найдена в текстах: неверифицированная фраза"
