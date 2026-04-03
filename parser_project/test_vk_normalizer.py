from normalizers.vk import normalize_vk_post

# фейковый VK пост (как будто с API)
raw_post = {
    "id": 456,
    "owner_id": 123,
    "from_id": 123,
    "date": 1712150000,
    "text": "Во дворе не убирают мусор",
    "views": {"count": 1000},
    "likes": {"count": 50},
    "reposts": {"count": 5},
    "comments": {"count": 20}
}

doc = normalize_vk_post(raw_post)

print(doc)