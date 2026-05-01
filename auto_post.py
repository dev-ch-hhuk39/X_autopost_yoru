import datetime as dt
import json
import os
import sys
import time

import gspread
import requests
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
    "ドライブ画像ファイルID",
    "ドライブ動画ファイルID",
    "Threads画像URL",
    "Threads動画URL",
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
VIDEO_CHUNK_SIZE = 5 * 1024 * 1024


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

def upload_image_bytes(media_bytes: bytes):
    req = requests.post(MEDIA_UPLOAD_URL, auth=get_oauth(), files={"media": media_bytes}, timeout=60)
    if req.status_code >= 400:
        raise RuntimeError(f"Image upload failed: {req.status_code} {req.text}")
    media_id = req.json().get("media_id_string")
    if not media_id:
        raise RuntimeError("Image upload succeeded but media_id_string was missing.")
    return media_id


def wait_for_video_processing(media_id: str):
    while True:
        status_resp = requests.get(
            MEDIA_UPLOAD_URL,
            auth=get_oauth(),
            params={"command": "STATUS", "media_id": media_id},
            timeout=60,
        )
        if status_resp.status_code >= 400:
            raise RuntimeError(f"Video STATUS failed: {status_resp.status_code} {status_resp.text}")
        payload = status_resp.json()
        processing = payload.get("processing_info") or {}
        state = processing.get("state")
        if state in {"succeeded", None}:
            return
        if state == "failed":
            raise RuntimeError(f"Video processing failed: {json.dumps(processing, ensure_ascii=False)}")
        time.sleep(int(processing.get("check_after_secs", 2)))


def upload_video_bytes(media_bytes: bytes, media_type: str):
    init_resp = requests.post(
        MEDIA_UPLOAD_URL,
        auth=get_oauth(),
        data={
            "command": "INIT",
            "total_bytes": len(media_bytes),
            "media_type": media_type,
            "media_category": "tweet_video",
        },
        timeout=60,
    )
    if init_resp.status_code >= 400:
        raise RuntimeError(f"Video INIT failed: {init_resp.status_code} {init_resp.text}")
    media_id = init_resp.json().get("media_id_string")
    if not media_id:
        raise RuntimeError("Video INIT succeeded but media_id_string was missing.")

    for segment_index, start in enumerate(range(0, len(media_bytes), VIDEO_CHUNK_SIZE)):
        chunk = media_bytes[start:start + VIDEO_CHUNK_SIZE]
        append_resp = requests.post(
            MEDIA_UPLOAD_URL,
            auth=get_oauth(),
            data={"command": "APPEND", "media_id": media_id, "segment_index": segment_index},
            files={"media": chunk},
            timeout=120,
        )
        if append_resp.status_code >= 400:
            raise RuntimeError(f"Video APPEND failed: {append_resp.status_code} {append_resp.text}")

    finalize_resp = requests.post(
        MEDIA_UPLOAD_URL,
        auth=get_oauth(),
        data={"command": "FINALIZE", "media_id": media_id},
        timeout=60,
    )
    if finalize_resp.status_code >= 400:
        raise RuntimeError(f"Video FINALIZE failed: {finalize_resp.status_code} {finalize_resp.text}")
    wait_for_video_processing(media_id)
    return media_id


def upload_media(image_url: str = "", video_url: str = ""):
    if not image_url and not video_url:
        return None
    target_url = video_url or image_url
    media_kind = "VIDEO" if video_url else "IMAGE"
    log(f"[{media_kind}] Downloading: {target_url}")
    response = requests.get(target_url, timeout=120)
    response.raise_for_status()
    media_bytes = response.content
    media_type = response.headers.get("Content-Type", "").split(";")[0].strip() or ("video/mp4" if video_url else "image/jpeg")
    if video_url:
        return upload_video_bytes(media_bytes, media_type)
    return upload_image_bytes(media_bytes)


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
        media_id = upload_media(image_url=image_url, video_url=video_url) if (image_url or video_url) else None
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
