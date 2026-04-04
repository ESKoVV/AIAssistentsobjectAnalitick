from __future__ import annotations

from apps.ml.classification import classify_document, load_taxonomy_config


def test_classify_document_returns_housing_for_waste_problem() -> None:
    result = classify_document(
        "мусор не вывозили третью неделю",
        config=load_taxonomy_config(),
    )

    assert result.category == "housing"
    assert result.category_label == "ЖКХ"


def test_classify_document_returns_roads_for_pothole_problem() -> None:
    result = classify_document(
        "яма на дороге, сломал колесо",
        config=load_taxonomy_config(),
    )

    assert result.category == "roads"


def test_classify_document_returns_ecology_for_fire_and_smoke() -> None:
    result = classify_document(
        "пожар на складе, дым видно по всему городу",
        config=load_taxonomy_config(),
    )

    assert result.category == "ecology"


def test_classify_document_returns_economy_for_salary_delay() -> None:
    result = classify_document(
        "задержали зарплату на заводе",
        config=load_taxonomy_config(),
    )

    assert result.category == "economy"


def test_classify_document_returns_other_with_max_confidence_when_no_hits() -> None:
    result = classify_document(
        "привет всем!",
        config=load_taxonomy_config(),
    )

    assert result.category == "other"
    assert result.confidence == 1.0


def test_classify_document_returns_secondary_category_when_gap_is_small() -> None:
    result = classify_document(
        "лифт не работает, а рядом яма на дороге",
        config=load_taxonomy_config(),
    )

    assert result.category in {"housing", "roads"}
    assert result.secondary_category in {"housing", "roads"}
    assert result.secondary_category is not None
