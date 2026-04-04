from collect_rss import normalize_rss_entry
from normalizers.vk import normalize_vk_post


def test_vk_normalization_populates_region_and_geo() -> None:
    raw_post = {
        "id": 456,
        "owner_id": 123,
        "from_id": 123,
        "date": 1712150000,
        "text": "Сильный ливень в Ростове",
        "geo": {"coordinates": "47.2221 39.7203"},
        "views": {"count": 1},
        "likes": {"count": 1},
        "reposts": {"count": 0},
        "comments": {"count": 0},
    }

    doc = normalize_vk_post(raw_post)

    assert doc.region_hint == "Ростовская область"
    assert doc.geo_lat == 47.2221
    assert doc.geo_lon == 39.7203


def test_rss_normalization_populates_region_and_geo() -> None:
    entry = {
        "title": "Ситуация в Краснодаре",
        "summary": "Ожидаются осадки",
        "published": "Wed, 03 Apr 2024 10:00:00 +0300",
        "geo": {"lat": "45.035", "lon": "38.975"},
        "author": "Региональные новости",
    }

    doc = normalize_rss_entry("https://example.com/rss", entry)

    assert doc.region_hint == "Краснодарский край"
    assert doc.geo_lat == 45.035
    assert doc.geo_lon == 38.975
