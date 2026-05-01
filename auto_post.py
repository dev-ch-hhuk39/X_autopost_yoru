import datetime as dt
import json
import os
import sys

import gspread
import requests
from google.oauth2.service_account import Credentials
from requests_oauthlib import OAuth1

TZ = dt.timezone(dt.timedelta(hours=9), name="JST")

QUEUE_HEADERS = [
    "キューID",
    "元投稿ID",
    "元投稿URL",
    "アカウント名",
    "投稿文",
    "画像URL",
    "動画URL",
    "採用案",
    "転載可否",
    "確認メモ",
    "X投稿対象",
    "X投稿状態",
    "X投稿日時",
    "Threads投稿対象",
    "Threads投稿状態",
    "Threads投稿日時",
    "キュー追加日時",
    "最終更新日時",
]

TWEET_URL = "https://api.twitter.com/2/tweets"
MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"


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
SHEET_TAB = os.environ.get("SHEET_TAB", "03_投稿キュー")
SHEET_GID = os.environ.get("SHEET_GID")

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

    if SHEET_GID:
        try:
            return sh.get_worksheet_by_id(int(SHEET_GID))
        except Exception:
            pass
    try:
        return sh.worksheet(SHEET_TAB)
    except Exception:
        return sh.sheet1


def header_index_map(ws):
    headers = ws.row_values(1)
    return {header: idx + 1 for idx, header in enumerate(headers)}


def upload_media(image_url: str):
    if not image_url:
        return None
    log(f"[IMAGE] Downloading: {image_url}")
    response = requests.get(image_url, timeout=60)
    response.raise_for_status()

    req = requests.post(MEDIA_UPLOAD_URL, auth=get_oauth(), files={"media": response.content}, timeout=60)
    if req.status_code >= 400:
        raise RuntimeError(f"Media upload failed: {req.status_code} {req.text}")
    media_id = req.json().get("media_id_string")
    if not media_id:
        raise RuntimeError("Media upload succeeded but media_id_string was missing.")
    return media_id


def post_tweet(text: str, media_id=None):
    payload = {"text": text}
    if media_id:
        payload["media"] = {"media_ids": [media_id]}
    response = requests.post(TWEET_URL, auth=get_oauth(), json=payload, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(f"Tweet failed: {response.status_code} {response.text}")
    return response.json()["data"]["id"]


def update_cells(ws, row_number: int, updates):
    header_map = header_index_map(ws)
    cells = []
    for header, value in updates.items():
        col = header_map.get(header)
        if col:
            cells.append(gspread.Cell(row=row_number, col=col, value=value))
    if cells:
        ws.update_cells(cells, value_input_option="RAW")


def pick_target(records):
    for index, row in enumerate(records, start=2):
        target = str(row.get("X投稿対象", "")).strip()
        status = str(row.get("X投稿状態", "")).strip()
        text = str(row.get("投稿文", "")).strip()
        if target == "投稿する" and status == "投稿待ち" and text:
            return index, row
    return None, None


def run():
    ws = get_sheet()
    records = ws.get_all_records(expected_headers=QUEUE_HEADERS)
    row_number, row = pick_target(records)

    if not row:
        log("[DONE] No pending X posts.")
        return

    now_str = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    log(f"[TARGET] Row {row_number}: {str(row.get('投稿文', ''))[:40]}...")

    try:
        image_url = str(row.get("画像URL", "")).strip()
        video_url = str(row.get("動画URL", "")).strip()
        if video_url and not image_url:
            raise RuntimeError("動画の自動投稿はまだ未対応です。画像付きまたはテキスト投稿で運用してください。")

        media_id = upload_media(image_url) if image_url else None
        tweet_id = post_tweet(str(row.get("投稿文", "")).strip(), media_id)
        log(f"[SUCCESS] Tweet ID: {tweet_id}")
        update_cells(
            ws,
            row_number,
            {
                "X投稿状態": "投稿済み",
                "X投稿日時": now_str,
                "最終更新日時": now_str,
            },
        )
    except Exception as exc:
        message = str(exc)
        log(f"[FAIL] {message}")
        note = str(row.get("確認メモ", "")).strip()
        merged_note = f"{note} / X投稿エラー: {message}".strip(" /")
        update_cells(
            ws,
            row_number,
            {
                "X投稿状態": "エラー",
                "確認メモ": merged_note,
                "最終更新日時": now_str,
            },
        )
        raise


if __name__ == "__main__":
    run()
