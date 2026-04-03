import os
import httpx
from dotenv import load_dotenv

load_dotenv()

VK_TOKEN = os.getenv("VK_TOKEN")
VK_API_VERSION = os.getenv("VK_API_VERSION", "5.131")


def vk_api_call(method: str, params: dict) -> dict:
    if not VK_TOKEN:
        raise ValueError("Не найден VK_TOKEN в .env")

    url = f"https://api.vk.com/method/{method}"

    response = httpx.get(
        url,
        params={
            **params,
            "access_token": VK_TOKEN,
            "v": VK_API_VERSION,
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