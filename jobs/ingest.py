"""Stage 3.1: call the Wistia API once and print the result.

Run:  uv run python jobs/ingest.py
"""

import os

import requests
from dotenv import find_dotenv, load_dotenv

# find_dotenv walks up from the current folder until it finds a .env file,
# so this also works from a git worktree inside the main project folder.
load_dotenv(find_dotenv(usecwd=True))

API_TOKEN = os.environ["WISTIA_API_TOKEN"]
MEDIA_ID = "9k4tbcdfg0"

url = f"https://api.wistia.com/v1/medias/{MEDIA_ID}.json"
headers = {"Authorization": f"Bearer {API_TOKEN}"}

response = requests.get(url, headers=headers, timeout=30)
response.raise_for_status()  # stop with an error on 4xx/5xx instead of printing an error page

media = response.json()
print(f"status: {response.status_code}")
print(f"name: {media['name']}")
print(f"hashed_id: {media['hashed_id']}")
print(f"duration (s): {media['duration']}")
print(f"created: {media['created']}")
print(f"updated: {media['updated']}")
