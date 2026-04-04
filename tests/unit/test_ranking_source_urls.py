from apps.ml.ranking.source_urls import extract_source_url


def test_extract_source_url_prefers_explicit_payload_url() -> None:
    assert (
        extract_source_url(
            source_type="rss_article",
            source_id="article-1",
            raw_payload={"url": "https://example.test/article-1"},
        )
        == "https://example.test/article-1"
    )


def test_extract_source_url_builds_vk_wall_permalink_from_source_id() -> None:
    assert (
        extract_source_url(
            source_type="vk_post",
            source_id="-12345_67890",
            raw_payload={},
        )
        == "https://vk.com/wall-12345_67890"
    )


def test_extract_source_url_builds_vk_comment_permalink_from_source_id() -> None:
    assert (
        extract_source_url(
            source_type="vk_comment",
            source_id="-12345_67890_77",
            raw_payload={},
        )
        == "https://vk.com/wall-12345_67890?reply=77"
    )
