from region_extractor import extract_geo, extract_region_hint


def test_extract_region_hint_from_text_and_metadata() -> None:
    text = "В Ростове-на-Дону усилился ветер и дождь"
    payload = {"group": {"name": "Новости Юга"}}

    assert extract_region_hint(text, payload) == "Ростовская область"


def test_extract_region_hint_prefers_explicit_region() -> None:
    text = "Пожар в столице"
    payload = {"region_hint": "Тверская область"}

    assert extract_region_hint(text, payload) == "Тверская область"


def test_extract_geo_from_string_coordinates() -> None:
    payload = {"geo": {"coordinates": "48.708 44.5133"}}

    assert extract_geo(payload) == (48.708, 44.5133)


def test_extract_geo_from_numeric_location_and_fail_safe() -> None:
    assert extract_geo({"location": {"lat": 55.75, "lon": 37.62}}) == (55.75, 37.62)
    assert extract_geo({"geo": {"coordinates": "invalid"}}) == (None, None)
