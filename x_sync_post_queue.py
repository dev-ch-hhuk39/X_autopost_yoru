import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from x_sheet_schema import QUEUE_DROPDOWNS, QUEUE_HEADERS, REVIEW_DROPDOWNS, REVIEW_HEADERS
from x_sheet_utils import apply_dropdown_validation, ensure_exact_headers, get_or_create_worksheet, open_spreadsheet, replace_sheet

JST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def now_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def pick_selected_text(row: Dict[str, str]) -> str:
    selected = str(row.get("採用案", "")).strip().upper()
    if selected == "A":
        return str(row.get("リライト案A", "")).strip()
    if selected == "B":
        return str(row.get("リライト案B", "")).strip()
    return ""


def first_media_url(value: str) -> str:
    parts = [part.strip() for part in str(value or "").split("|") if part.strip()]
    return parts[0] if parts else ""


def build_queue_rows(review_rows: List[Dict[str, str]], existing_rows: List[Dict[str, str]]) -> List[List[str]]:
    existing_index = {
        str(row.get("元投稿ID", "")).strip(): row
        for row in existing_rows
        if str(row.get("元投稿ID", "")).strip()
    }
    output_rows: List[List[str]] = []
    timestamp = now_str()

    for review in review_rows:
        source_id = str(review.get("投稿ID", "")).strip()
        if not source_id:
            continue

        selected_text = pick_selected_text(review)
        x_target = "投稿する" if str(review.get("X投稿するか", "")).strip() == "投稿する" else "投稿しない"
        threads_target = "投稿する" if str(review.get("Threads投稿するか", "")).strip() == "投稿する" else "投稿しない"
        approved = (
            str(review.get("転載可否", "")).strip() == "転載OK"
            and str(review.get("投稿可否", "")).strip() == "投稿OK"
            and str(review.get("採用案", "")).strip() in {"A", "B"}
            and bool(selected_text)
            and (x_target == "投稿する" or threads_target == "投稿する")
        )

        existing = existing_index.get(source_id, {})
        if not approved and not existing:
            continue

        queue_id = existing.get("キューID", "") or f"x-{source_id}"
        x_state = existing.get("X投稿状態", "")
        if not x_state:
            x_state = "投稿待ち" if x_target == "投稿する" and approved else "スキップ"
        threads_state = existing.get("Threads投稿状態", "")
        if not threads_state:
            threads_state = "投稿待ち" if threads_target == "投稿する" and approved else "スキップ"

        if not approved:
            x_target = existing.get("X投稿対象", x_target)
            threads_target = existing.get("Threads投稿対象", threads_target)

        merged = {
            "キューID": queue_id,
            "元投稿ID": source_id,
            "元投稿URL": review.get("元投稿URL", ""),
            "アカウント名": review.get("アカウント名", ""),
            "投稿文": selected_text or existing.get("投稿文", ""),
            "画像URL": review.get("保存メディアURL", "") or first_media_url(review.get("画像URL一覧", "")),
            "動画URL": first_media_url(review.get("動画URL一覧", "")),
            "ドライブ画像ファイルID": review.get("ドライブ画像ファイルID", existing.get("ドライブ画像ファイルID", "")),
            "ドライブ動画ファイルID": review.get("ドライブ動画ファイルID", existing.get("ドライブ動画ファイルID", "")),
            "Threads画像URL": review.get("Threads公開画像URL", existing.get("Threads画像URL", "")),
            "Threads動画URL": review.get("Threads公開動画URL", existing.get("Threads動画URL", "")),
            "採用案": review.get("採用案", existing.get("採用案", "")),
            "転載可否": review.get("転載可否", existing.get("転載可否", "")),
            "確認メモ": review.get("確認メモ", existing.get("確認メモ", "")),
            "X投稿対象": x_target,
            "X投稿状態": x_state,
            "X投稿日時": existing.get("X投稿日時", ""),
            "Threads投稿対象": threads_target,
            "Threads投稿状態": threads_state,
            "Threads投稿日時": existing.get("Threads投稿日時", ""),
            "キュー追加日時": existing.get("キュー追加日時", timestamp),
            "最終更新日時": timestamp,
        }
        output_rows.append([merged.get(header, "") for header in QUEUE_HEADERS])
    return output_rows


def run():
    config = load_config()
    spreadsheet = open_spreadsheet()
    tabs = config["sheet_tabs"]
    review_ws = get_or_create_worksheet(spreadsheet, tabs["review"], rows=5000, cols=len(REVIEW_HEADERS) + 5)
    queue_ws = get_or_create_worksheet(spreadsheet, tabs["post_queue"], rows=5000, cols=len(QUEUE_HEADERS) + 5)

    ensure_exact_headers(review_ws, REVIEW_HEADERS)
    ensure_exact_headers(queue_ws, QUEUE_HEADERS)
    apply_dropdown_validation(review_ws, REVIEW_HEADERS, REVIEW_DROPDOWNS)
    apply_dropdown_validation(queue_ws, QUEUE_HEADERS, QUEUE_DROPDOWNS)

    review_rows = review_ws.get_all_records(default_blank="")
    existing_rows = queue_ws.get_all_records(default_blank="")
    queue_rows = build_queue_rows(review_rows, existing_rows)
    replace_sheet(queue_ws, QUEUE_HEADERS, queue_rows)
    apply_dropdown_validation(queue_ws, QUEUE_HEADERS, QUEUE_DROPDOWNS)
    print(f"[OK] Synced {len(queue_rows)} queue rows from review sheet.")


if __name__ == "__main__":
    run()
