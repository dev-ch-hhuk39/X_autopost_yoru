import os
import json
import sys
import datetime as dt
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from requests_oauthlib import OAuth1

# === 環境設定 ===
TZ = dt.timezone(dt.timedelta(hours=9), name="JST")

def log(*a): print(*a); sys.stdout.flush()
def need(name):
    v = os.environ.get(name)
    if not v: raise RuntimeError(f"[FATAL] missing env: {name}")
    return v

# 環境変数
SHEET_ID = need("SHEET_ID")
GCP_SA_JSON = need("GCP_SA_JSON")
SHEET_TAB = os.environ.get("SHEET_TAB", "x_autopost_yoru")
SHEET_GID = os.environ.get("SHEET_GID")

X_API_KEY = need("X_API_KEY")
X_API_SECRET = need("X_API_SECRET")
X_ACCESS_TOKEN = need("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = need("X_ACCESS_TOKEN_SECRET")

# API Endpoints
TWEET_URL = "https://api.twitter.com/2/tweets"
MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"

# 新ヘッダー定義 (Threads自動化と統一)
# 1行目: text, image_url, alt_text, link_attachment, reply_control, topic_tag, location_id, status, posted_at, error
EXPECTED_HEADERS = [
    "text", "image_url", "alt_text", "link_attachment", 
    "reply_control", "topic_tag", "location_id", "status", "posted_at", "error"
]

# === 認証関連 ===
def get_oauth():
    return OAuth1(
        client_key=X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET
    )

def get_sheet():
    info = json.loads(GCP_SA_JSON)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    
    if SHEET_GID:
        try:
            ws = sh.get_worksheet_by_id(int(SHEET_GID))
            log(f"[OK] use gid: {ws.id}")
            return ws
        except:
            pass
    try:
        ws = sh.worksheet(SHEET_TAB)
        log(f"[OK] use name: {SHEET_TAB}")
        return ws
    except:
        ws = sh.sheet1
        log(f"[WARN] fallback to sheet1")
        return ws

# === 画像アップロード (v1.1) ===
def upload_media(image_url):
    if not image_url: return None
    log(f"[IMAGE] Downloading: {image_url}")
    
    # 1. 画像ダウンロード
    try:
        img_resp = requests.get(image_url, timeout=30)
        img_resp.raise_for_status()
    except Exception as e:
        log(f"[WARN] Image download failed: {e}")
        return None

    # 2. Twitterへアップロード
    auth = get_oauth()
    files = {"media": img_resp.content}
    
    try:
        req = requests.post(MEDIA_UPLOAD_URL, auth=auth, files=files)
        if req.status_code >= 400:
            log(f"[ERROR] Media Upload Failed: {req.text}")
            return None
        
        media_id = req.json().get("media_id_string")
        log(f"[IMAGE] Uploaded Media ID: {media_id}")
        return media_id
    except Exception as e:
        log(f"[ERROR] Media Upload Logic Error: {e}")
        return None

# === ツイート投稿 (v2) ===
def post_tweet(text, media_id=None):
    auth = get_oauth()
    payload = {"text": text}
    
    if media_id:
        payload["media"] = {"media_ids": [media_id]}
    
    try:
        r = requests.post(TWEET_URL, auth=auth, json=payload)
        if r.status_code >= 400:
            log(f"[ERROR] Tweet Failed: {r.status_code} {r.text}")
            raise Exception(f"API Error: {r.text}")
        
        data = r.json()
        return data["data"]["id"]
    except Exception as e:
        raise e

# === メイン処理 ===
def run():
    ws = get_sheet()
    
    # 全データを辞書リストで取得
    # expected_headersを指定することで、列順が多少違ってもキーでアクセス可能
    records = ws.get_all_records(expected_headers=EXPECTED_HEADERS)
    
    target_index = None
    target_row = None

    # 上から順に未投稿を探す
    for i, row in enumerate(records):
        status = str(row.get("status", "")).strip()
        text = str(row.get("text", "")).strip()
        
        # statusが空 かつ 本文がある行
        if status == "" and text != "":
            target_index = i + 2  # 行番号 (1始まり + ヘッダー1行)
            target_row = row
            break
    
    if not target_row:
        log("[DONE] No pending posts.")
        return

    log(f"[TARGET] Row {target_index}: {target_row['text'][:30]}...")

    try:
        # 画像処理
        media_id = None
        img_url = str(target_row.get("image_url", "")).strip()
        if img_url:
            media_id = upload_media(img_url)

        # 投稿
        tid = post_tweet(target_row["text"], media_id)
        log(f"[SUCCESS] Tweet ID: {tid}")

        # スプシ更新
        now_str = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        
        # 列番号を探して更新 (安全策)
        header = ws.row_values(1)
        
        # status列更新
        try:
            col_status = header.index("status") + 1
            ws.update_cell(target_index, col_status, "Done")
        except: pass
            
        # posted_at列更新
        try:
            col_posted = header.index("posted_at") + 1
            ws.update_cell(target_index, col_posted, now_str)
        except: pass
        
        log("[UPDATE] Spreadsheet updated.")

    except Exception as e:
        log(f"[FAIL] {e}")
        # エラー書き込み
        try:
            header = ws.row_values(1)
            col_error = header.index("error") + 1
            ws.update_cell(target_index, col_error, str(e))
        except: pass
        sys.exit(1)

if __name__ == "__main__":
    run()
