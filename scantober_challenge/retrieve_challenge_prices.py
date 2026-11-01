
import json
import requests

BASE_URL = "https://prices.openfoodfacts.org/api/v1/prices"
PARAMS = {
    "app_name": "TTalex/ChallengeAnalysis",
    "page": 1,
    "size": 100,
    "order_by": "-created",
    "tags__contains": "challenge-17",
}

all_items = []

while True:
    print(f"Fetching page {PARAMS['page']}...")

    response = requests.get(BASE_URL, params=PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()

    items = data.get("items", [])
    all_items.extend(items)

    if PARAMS["page"] >= data.get("pages", 1):
        break

    PARAMS["page"] += 1

with open("prices.json", "w", encoding="utf-8") as f:
    json.dump(all_items, f, ensure_ascii=False, indent=2)

print(f"Done! Saved {len(all_items)} records to prices.json")

