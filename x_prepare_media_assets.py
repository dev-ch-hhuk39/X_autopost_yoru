import hashlib
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import gspread
import requests
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

from x_sheet_schema import REVIEW_DROPDOWNS, REVIEW_HEADERS
from x_sheet_utils import apply_dropdown_validation, ensure_exact_headers, get_or_create_worksheet, open_spreadsheet, sanitize_cell

CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")
DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true"
DRIVE_FILE_URL = "https://www.googleapis.com/drive/v3/files"


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def service_account_info():
    raw = os.environ.get("GCP_SA_JSON", "").strip()
    if not raw:
        raise RuntimeError("GCP_SA_JSON is required for Drive upload.")
    return json.loads(raw)


def drive_session():
    creds = Credentials.from_service_account_info(
        service_account_info(),
        scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"],
    )
    return AuthorizedSession(creds)


def validate_drive_folder_access(folder_id: str):
    if not folder_id:
        raise RuntimeError("Drive folder id is empty.")
    session = drive_session()
    url = f"{DRIVE_FILE_URL}/{folder_id}"
    params = {
        "supportsAllDrives": "true",
        "fields": "id,name,mimeType,capabilities(canAddChildren,canEdit),driveId,owners,emailAddress",
    }
    response = session.get(url, params=params, timeout=60)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"Drive folder check failed: status={response.status_code} folder_id={folder_id} body={response.text[:1000]}"
        ) from exc
    payload = response.json()
    caps = payload.get("capabilities", {})
    if not caps.get("canAddChildren", False):
        raise RuntimeError(
            f"Drive folder is visible but not writable: folder_id={folder_id} name={payload.get('name')} capabilities={caps}"
        )
    return payload


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def cloudinary_signature(params: Dict[str, str], api_secret: str) -> str:
    filtered = {k: v for k, v in params.items() if v not in (None, "", [])}
    payload = "&".join(f"{key}={filtered[key]}" for key in sorted(filtered))
    return hashlib.sha1(f"{payload}{api_secret}".encode("utf-8")).hexdigest()


def download_media(url: str) -> Tuple[bytes, str]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    mime = response.headers.get("Content-Type", "").split(";")[0].strip() or mimetypes.guess_type(url)[0] or "application/octet-stream"
    return response.content, mime


def upload_to_drive(data: bytes, mime_type: str, filename: str, folder_id: str) -> Tuple[str, str]:
    session = drive_session()
    boundary = f"codex-boundary-{int(time.time())}"
    metadata = {"name": filename, "parents": [folder_id]}
    body = (
        f"--{boundary}\r\n"
        "Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{json.dumps(metadata)}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: {mime_type}\r\n\r\n"
    ).encode("utf-8") + data + f"\r\n--{boundary}--".encode("utf-8")
    response = session.post(DRIVE_UPLOAD_URL, headers={"Content-Type": f"multipart/related; boundary={boundary}"}, data=body, timeout=120)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"Drive upload failed: status={response.status_code} folder_id={folder_id} filename={filename} body={response.text[:1000]}"
        ) from exc
    file_id = response.json()["id"]
    view_url = f"https://drive.google.com/file/d/{file_id}/view"
    return file_id, view_url


def upload_to_cloudinary(data: bytes, mime_type: str, public_id: str) -> str:
    cloud_name = env_required("CLOUDINARY_CLOUD_NAME")
    api_key = env_required("CLOUDINARY_API_KEY")
    api_secret = env_required("CLOUDINARY_API_SECRET")
    timestamp = str(int(time.time()))
    resource_type = "video" if mime_type.startswith("video/") else "image"
    params = {"timestamp": timestamp, "public_id": public_id}
    signature = cloudinary_signature(params, api_secret)
    files = {"file": ("upload", data, mime_type)}
    payload = {"api_key": api_key, "timestamp": timestamp, "public_id": public_id, "signature": signature}
    url = f"https://api.cloudinary.com/v1_1/{cloud_name}/{resource_type}/upload"
    response = requests.post(url, data=payload, files=files, timeout=120)
    response.raise_for_status()
    return response.json()["secure_url"]


def first_pipe_value(value: str) -> str:
    parts = [part.strip() for part in str(value or "").split("|") if part.strip()]
    return parts[0] if parts else ""


def safe_slug(text: str, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "-" for ch in text.lower()).strip("-")
    return cleaned or fallback


def build_public_id(post_id: str, account_name: str, suffix: str) -> str:
    account_slug = safe_slug(account_name, "account")
    return f"yorusyoku/{account_slug}/{post_id}-{suffix}"


def update_review_rows(ws, rows: List[Dict[str, str]]):
    values = [[sanitize_cell(row.get(header, "")) for header in REVIEW_HEADERS] for row in rows]
    ws.update("A2", values, raw=True)


def run():
    config = load_config()
    spreadsheet = open_spreadsheet()
    review_ws = get_or_create_worksheet(spreadsheet, config["sheet_tabs"]["review"], rows=5000, cols=len(REVIEW_HEADERS) + 5)
    ensure_exact_headers(review_ws, REVIEW_HEADERS)
    apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)
    rows = review_ws.get_all_records(default_blank="")

    drive_image_folder = os.environ.get("DRIVE_IMAGE_FOLDER_ID", "").strip()
    drive_video_folder = os.environ.get("DRIVE_VIDEO_FOLDER_ID", "").strip() or drive_image_folder
    image_folder_meta = validate_drive_folder_access(drive_image_folder)
    print(
        f"[OK] Drive image folder access: id={image_folder_meta.get('id')} name={image_folder_meta.get('name')}",
        flush=True,
    )
    if drive_video_folder:
        video_folder_meta = validate_drive_folder_access(drive_video_folder)
        print(
            f"[OK] Drive video folder access: id={video_folder_meta.get('id')} name={video_folder_meta.get('name')}",
            flush=True,
        )

    changed = False
    for row in rows:
        post_id = str(row.get("投稿ID", "")).strip()
        if not post_id:
            continue

        image_url = first_pipe_value(row.get("画像URL一覧", ""))
        video_url = first_pipe_value(row.get("動画URL一覧", ""))

        if image_url and not row.get("ドライブ画像ファイルID"):
            data, mime = download_media(image_url)
            ext = mimetypes.guess_extension(mime) or ".jpg"
            filename = f"{post_id}{ext}"
            file_id, view_url = upload_to_drive(data, mime, filename, drive_image_folder)
            public_url = upload_to_cloudinary(data, mime, build_public_id(post_id, str(row.get('アカウント名', '')), "image"))
            row["ドライブ画像ファイルID"] = file_id
            row["保存メディアURL"] = view_url
            row["保存メディアパス"] = f"x_images/{filename}"
            row["Threads公開画像URL"] = public_url
            changed = True

        if video_url and drive_video_folder and not row.get("ドライブ動画ファイルID"):
            try:
                data, mime = download_media(video_url)
                ext = mimetypes.guess_extension(mime) or ".mp4"
                filename = f"{post_id}{ext}"
                file_id, view_url = upload_to_drive(data, mime, filename, drive_video_folder)
                public_url = upload_to_cloudinary(data, mime, build_public_id(post_id, str(row.get('アカウント名', '')), "video"))
                row["ドライブ動画ファイルID"] = file_id
                if not row.get("保存メディアURL"):
                    row["保存メディアURL"] = view_url
                    row["保存メディアパス"] = f"x_videos/{filename}"
                row["Threads公開動画URL"] = public_url
                changed = True
            except Exception:
                # X API からは動画URLが十分に取れないケースが多いので画像側優先で継続する
                pass

    if changed and rows:
        update_review_rows(review_ws, rows)
        apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)
    print(f"[OK] Prepared media assets for {sum(1 for row in rows if row.get('ドライブ画像ファイルID') or row.get('Threads公開画像URL'))} review rows.")


if __name__ == "__main__":
    run()
