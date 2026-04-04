import uuid

from schema import SourceType


def build_doc_id(source_type: SourceType, source_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_type.value}:{source_id}"))


def build_vk_post_doc_id(source_id: str) -> str:
    return build_doc_id(SourceType.VK_POST, source_id)


def build_vk_comment_doc_id(source_id: str) -> str:
    return build_doc_id(SourceType.VK_COMMENT, source_id)


def build_rss_article_doc_id(source_id: str) -> str:
    return build_doc_id(SourceType.RSS_ARTICLE, source_id)


def build_portal_appeal_doc_id(source_id: str) -> str:
    return build_doc_id(SourceType.PORTAL_APPEAL, source_id)


def build_max_post_doc_id(source_id: str) -> str:
    return build_doc_id(SourceType.MAX_POST, source_id)


def build_max_comment_doc_id(source_id: str) -> str:
    return build_doc_id(SourceType.MAX_COMMENT, source_id)
