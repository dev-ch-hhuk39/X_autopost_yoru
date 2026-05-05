import datetime as dt
import json
import os
import sys
import time

import gspread
import requests
from google.oauth2.service_account import Credentials
from requests_oauthlib import OAuth1

TZ = dt.timezone(dt.timedelta(hours=9), name="JST")

POST_HEADERS = [
    "text",
    "image_url",
    "alt_text",
    "link_attachment",
    "reply_control",
    "topic_tag",
    "location_id",
    "status",
    "posted_at",
    "error",
]

TWEET_URL = "https://api.twitter.com/2/tweets"
STATUS_UPDATE_URL = "https://api.twitter.com/1.1/statuses/update.json"
MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"
MAX_TWEET_CHARS = int(os.environ.get("LEGACY_X_MAX_CHARS", "275"))
RETRY_V1_FALLBACK = os.environ.get("LEGACY_X_RETRY_V1_FALLBACK", "").strip().lower() in {"1", "true", "yes"}


def log(*args):
    print(*args)
    sys.stdout.flush()


def need(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"[FATAL] missing env: {name}")
    return value


SHEET_ID = need("SHEET_ID")
GCP_SA_JSON = need("GCP_SA_JSON")
SHEET_TAB = os.environ.get("SHEET_TAB", "x_autopost_yoru")

X_API_KEY = need("X_API_KEY")
X_API_SECRET = need("X_API_SECRET")
X_ACCESS_TOKEN = need("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = need("X_ACCESS_TOKEN_SECRET")


def get_oauth():
    return OAuth1(
        client_key=X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET,
    )


def get_sheet():
    info = json.loads(GCP_SA_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(SHEET_TAB)


def upload_image_bytes(media_bytes: bytes):
    req = requests.post(MEDIA_UPLOAD_URL, auth=get_oauth(), files={"media": media_bytes}, timeout=60)
    if req.status_code >= 400:
        raise RuntimeError(f"Image upload failed: {req.status_code} {req.text}")
    media_id = req.json().get("media_id_string")
    if not media_id:
        raise RuntimeError("Image upload succeeded but media_id_string was missing.")
    return media_id


def upload_media_from_url(image_url: str):
    if not image_url:
        return None
    log(f"[IMAGE] Downloading: {image_url}")
    response = requests.get(image_url, timeout=60)
    response.raise_for_status()
    return upload_image_bytes(response.content)


def normalize_tweet_text(text: str) -> str:
    text = str(text or "").strip()
    if len(text) <= MAX_TWEET_CHARS:
        return text
    trimmed = text[:MAX_TWEET_CHARS].rstrip()
    log(f"[WARN] Legacy X text was {len(text)} chars. Trimmed to {len(trimmed)} chars.")
    return trimmed


def post_tweet_v2(text: str, media_id=None):
    payload = {"text": text}
    if media_id:
        payload["media"] = {"media_ids": [media_id]}
    response = requests.post(TWEET_URL, auth=get_oauth(), json=payload, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Tweet failed: {response.status_code} {response.text}")
    return response.json()["data"]["id"]


def post_tweet_v1(text: str, media_id=None):
    data = {"status": text}
    if media_id:
        data["media_ids"] = media_id
    response = requests.post(STATUS_UPDATE_URL, auth=get_oauth(), data=data, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Tweet v1.1 fallback failed: {response.status_code} {response.text}")
    return str(response.json()["id_str"])


def post_tweet(text: str, media_id=None):
    try:
        return post_tweet_v2(text, media_id)
    except RuntimeError as exc:
        message = str(exc)
        if "Tweet failed: 403" not in message or not RETRY_V1_FALLBACK:
            raise
        log("[WARN] v2 tweet create returned 403. Retrying with v1.1 statuses/update.")
        return post_tweet_v1(text, media_id)


def update_cells(ws, row_number: int, updates):
    header_map = {header: idx + 1 for idx, header in enumerate(POST_HEADERS)}
    cells = []
    for header, value in updates.items():
        col = header_map.get(header)
        if col:
            cells.append(gspread.Cell(row=row_number, col=col, value=value))
    if cells:
        ws.update_cells(cells, value_input_option="RAW")


def is_pending_status(status: str) -> bool:
    text = str(status or "").strip().lower()
    return text not in {
        "posted",
        "done",
        "済",
        "posted✅",
        "error",
        "failed",
        "skip",
        "skipped",
        "投稿不可",
        "エラー",
    }


def pick_target(records):
    for index, row in enumerate(records, start=2):
        text = str(row.get("text", "")).strip()
        image_url = str(row.get("image_url", "")).strip()
        status = str(row.get("status", "")).strip()
        if (text or image_url) and is_pending_status(status):
            return index, row
    return None, None


def run():
    ws = get_sheet()
    records = ws.get_all_records(expected_headers=POST_HEADERS)
    row_number, row = pick_target(records)

    if not row:
        log("[DONE] No pending legacy X posts.")
        return

    now_str = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    text = normalize_tweet_text(row.get("text", ""))
    image_url = str(row.get("image_url", "")).strip()
    log(f"[TARGET] Row {row_number}: chars={len(text)} text={text[:40]}...")

    try:
        media_id = upload_media_from_url(image_url) if image_url else None
        tweet_id = post_tweet(text, media_id)
        log(f"[SUCCESS] Tweet ID: {tweet_id}")
        update_cells(
            ws,
            row_number,
            {
                "status": "posted",
                "posted_at": now_str,
                "error": "",
            },
        )
    except Exception as exc:
        message = str(exc)
        log(f"[FAIL] {message}")
        update_cells(
            ws,
            row_number,
            {
                "status": "error",
                "error": message[:3000],
            },
        )
        raise


if __name__ == "__main__":
    run()
