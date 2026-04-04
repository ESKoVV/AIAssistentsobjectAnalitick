import httpx
from config import ConfigError, load_config

CONFIG = load_config()


def vk_api_call(method: str, params: dict) -> dict:
    if not CONFIG.vk_token:
        raise ConfigError("Не найден VK_TOKEN. Добавьте переменную в .env.")

    url = f"https://api.vk.com/method/{method}"

    response = httpx.get(
        url,
        params={
            **params,
            "access_token": CONFIG.vk_token,
            "v": CONFIG.vk_api_version,
        },
        timeout=30.0,
    )

    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise RuntimeError(f"VK API error: {data['error']}")

    return data["response"]


def resolve_screen_name(screen_name: str) -> dict:
    return vk_api_call(
        "utils.resolveScreenName",
        {
            "screen_name": screen_name,
        },
    )


def get_wall_posts(owner_id: int, count: int = 5, offset: int = 0) -> list[dict]:
    response = vk_api_call(
        "wall.get",
        {
            "owner_id": owner_id,
            "count": count,
            "offset": offset,
            "filter": "owner",
        },
    )
    return response.get("items", [])


def get_post_comments(
    owner_id: int,
    post_id: int,
    count: int = 100,
) -> list[dict]:
    comments: list[dict] = []
    offset = 0

    while True:
        response = vk_api_call(
            "wall.getComments",
            {
                "owner_id": owner_id,
                "post_id": post_id,
                "count": count,
                "offset": offset,
                "need_likes": 1,
                "extended": 0,
                "thread_items_count": 0,
            },
        )

        items = response.get("items", [])
        total = response.get("count", 0)

        comments.extend(items)

        if not items:
            break

        offset += len(items)
        if offset >= total:
            break

    return comments
