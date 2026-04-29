import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from x_sheet_utils import ensure_headers, get_or_create_worksheet, open_spreadsheet, upsert_rows, write_key_value_rows

JST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")

RAW_HEADERS = [
    "post_id",
    "post_url",
    "platform",
    "genre",
    "account_name",
    "account_id",
    "account_handle",
    "account_url",
    "follower_count",
    "posted_at",
    "posted_date",
    "weekday",
    "hour",
    "time_slot",
    "post_type",
    "text",
    "hook_text",
    "text_length",
    "emoji_count",
    "hashtag_count",
    "mention_count",
    "hashtags",
    "mentions",
    "link_urls",
    "has_external_link",
    "image_count",
    "has_image",
    "has_video",
    "has_media",
    "quote_count",
    "reply_count",
    "repost_count",
    "like_count",
    "bookmark_count",
    "impression_count",
    "matched_keywords",
    "matched_accounts",
    "matched_sources",
    "source_types",
    "is_from_account_monitor",
    "is_from_keyword_search",
    "first_collected_at",
    "last_metrics_update_at",
    "last_source_sync_at",
    "raw_payload_json",
]


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Prepare and sync X post collection data into Google Sheets."
    )
    parser.add_argument("--input-json", help="Path to a JSON file exported from your collector.")
    parser.add_argument(
        "--bootstrap-only",
        action="store_true",
        help="Create required tabs and state rows without importing posts.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Exit successfully when no import source is configured.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Print current sheet tab names and row counts without changing data.",
    )
    return parser.parse_args()


def now_jst():
    return datetime.now(JST)


def as_iso(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        return now_jst()

    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                dt = dt.replace(tzinfo=JST)
                break
            except ValueError:
                continue
        else:
            return now_jst()

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=JST)
    return dt.astimezone(JST)


def to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace(",", "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def to_bool_string(value: Any) -> str:
    return "TRUE" if bool(value) else "FALSE"


def normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if "|" in text:
        return [part.strip() for part in text.split("|") if part.strip()]
    return [text]


def uniq(items: Iterable[str]) -> List[str]:
    seen = set()
    ordered = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def join_pipe(items: Iterable[str]) -> str:
    return " | ".join(uniq(items))


def count_emojis(text: str) -> int:
    return sum(1 for ch in text if ord(ch) > 10000)


def extract_hashtags(text: str) -> List[str]:
    return uniq(part[1:] for part in text.split() if part.startswith("#"))


def extract_mentions(text: str) -> List[str]:
    return uniq(part[1:] for part in text.split() if part.startswith("@"))


def time_slot_for_hour(hour: int) -> str:
    if 0 <= hour <= 5:
        return "late_night"
    if 6 <= hour <= 11:
        return "morning"
    if 12 <= hour <= 17:
        return "afternoon"
    return "evening"


def first_line(text: str, limit: int = 30) -> str:
    compact = " ".join((text or "").split())
    return compact[:limit]


def load_posts_from_json(path: Optional[str]) -> List[Dict[str, Any]]:
    payload = None
    if path:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    else:
        env_json = Path.cwd().joinpath("data", "x_posts.json")
        raw_env = None
        try:
            import os

            raw_env = os.environ.get("X_POSTS_JSON", "").strip()
        except Exception:
            raw_env = ""
        if raw_env:
            payload = json.loads(raw_env)
        elif env_json.exists():
            payload = json.loads(env_json.read_text(encoding="utf-8"))

    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("posts"), list):
        return payload["posts"]
    raise RuntimeError("Unsupported JSON shape. Expected a list or {'posts': [...]} .")


def raw_key(post: Dict[str, Any]) -> str:
    return str(post.get("post_id") or post.get("post_url") or "").strip()


def normalize_post(post: Dict[str, Any], existing: Dict[str, str], config: Dict[str, Any]) -> Dict[str, str]:
    current = now_jst()
    posted_at = parse_datetime(
        str(
            post.get("posted_at")
            or post.get("created_at")
            or post.get("tweet_created_at")
            or ""
        )
    )
    text = str(post.get("text") or post.get("full_text") or post.get("content") or "").strip()
    hashtags = normalize_list(post.get("hashtags")) or extract_hashtags(text)
    mentions = normalize_list(post.get("mentions")) or extract_mentions(text)
    link_urls = normalize_list(post.get("link_urls") or post.get("urls"))
    matched_keywords = normalize_list(post.get("matched_keywords") or post.get("keywords"))
    matched_accounts = normalize_list(post.get("matched_accounts"))
    matched_sources = normalize_list(post.get("matched_sources"))
    source_types = normalize_list(post.get("source_types"))

    if not matched_accounts and post.get("account_handle"):
        matched_accounts = [str(post.get("account_handle")).strip()]
    if not matched_sources:
        matched_sources = matched_accounts + matched_keywords
    if not source_types:
        if matched_accounts:
            source_types.append("account_monitor")
        if matched_keywords:
            source_types.append("keyword_search")

    image_count = to_int(post.get("image_count"))
    if image_count == 0 and normalize_list(post.get("image_urls")):
        image_count = len(normalize_list(post.get("image_urls")))

    post_id = str(post.get("post_id") or post.get("id") or "").strip()
    account_handle = str(post.get("account_handle") or post.get("handle") or "").strip().lstrip("@")
    account_url = str(post.get("account_url") or "").strip()
    if not account_url and account_handle:
        account_url = f"https://x.com/{account_handle}"

    post_url = str(post.get("post_url") or post.get("url") or "").strip()
    if not post_url and account_handle and post_id:
        post_url = f"https://x.com/{account_handle}/status/{post_id}"

    resolved_key = post_id or post_url or existing.get("post_id", "")

    return {
        "post_id": resolved_key,
        "post_url": post_url or existing.get("post_url", ""),
        "platform": "x",
        "genre": config.get("genre", "夜職"),
        "account_name": str(post.get("account_name") or post.get("author_name") or "").strip(),
        "account_id": str(post.get("account_id") or post.get("author_id") or "").strip(),
        "account_handle": account_handle,
        "account_url": account_url,
        "follower_count": str(to_int(post.get("follower_count"))),
        "posted_at": as_iso(posted_at),
        "posted_date": posted_at.strftime("%Y-%m-%d"),
        "weekday": posted_at.strftime("%a"),
        "hour": str(posted_at.hour),
        "time_slot": time_slot_for_hour(posted_at.hour),
        "post_type": str(post.get("post_type") or post.get("tweet_type") or "post").strip(),
        "text": text,
        "hook_text": first_line(text),
        "text_length": str(len(text)),
        "emoji_count": str(count_emojis(text)),
        "hashtag_count": str(len(hashtags)),
        "mention_count": str(len(mentions)),
        "hashtags": join_pipe(hashtags),
        "mentions": join_pipe(mentions),
        "link_urls": join_pipe(link_urls),
        "has_external_link": to_bool_string(bool(link_urls)),
        "image_count": str(image_count),
        "has_image": to_bool_string(image_count > 0),
        "has_video": to_bool_string(bool(post.get("has_video") or post.get("video_url") or post.get("video_urls"))),
        "has_media": to_bool_string(image_count > 0 or bool(post.get("has_video") or post.get("video_url") or post.get("video_urls"))),
        "quote_count": str(to_int(post.get("quote_count"))),
        "reply_count": str(to_int(post.get("reply_count") or post.get("comment_count"))),
        "repost_count": str(to_int(post.get("repost_count") or post.get("retweet_count"))),
        "like_count": str(to_int(post.get("like_count") or post.get("favorite_count"))),
        "bookmark_count": str(to_int(post.get("bookmark_count"))),
        "impression_count": str(to_int(post.get("impression_count") or post.get("view_count"))),
        "matched_keywords": join_pipe(matched_keywords),
        "matched_accounts": join_pipe(matched_accounts),
        "matched_sources": join_pipe(matched_sources),
        "source_types": join_pipe(source_types),
        "is_from_account_monitor": to_bool_string("account_monitor" in source_types),
        "is_from_keyword_search": to_bool_string("keyword_search" in source_types),
        "first_collected_at": existing.get("first_collected_at") or as_iso(current),
        "last_metrics_update_at": as_iso(current),
        "last_source_sync_at": as_iso(current),
        "raw_payload_json": json.dumps(post, ensure_ascii=False),
    }


def bootstrap_sheet(config: Dict[str, Any]):
    spreadsheet = open_spreadsheet()
    tabs = config["sheet_tabs"]
    raw_ws = get_or_create_worksheet(spreadsheet, tabs["raw_posts"], rows=5000, cols=len(RAW_HEADERS) + 5)
    scored_ws = get_or_create_worksheet(spreadsheet, tabs["scored_posts"], rows=5000, cols=30)
    insights_ws = get_or_create_worksheet(spreadsheet, tabs["insights"], rows=1000, cols=10)
    state_ws = get_or_create_worksheet(spreadsheet, tabs["state"], rows=100, cols=3)

    ensure_headers(raw_ws, RAW_HEADERS)
    ensure_headers(scored_ws, ["post_id"])
    ensure_headers(insights_ws, ["section", "metric", "value", "note", "updated_at"])
    ensure_headers(state_ws, ["key", "value", "updated_at"])
    return spreadsheet, raw_ws, scored_ws, insights_ws, state_ws


def current_state_rows(config: Dict[str, Any], imported_count: int, status: str) -> List[Dict[str, str]]:
    now_str = as_iso(now_jst())
    collection = config["collection"]
    return [
        {"key": "genre", "value": config.get("genre", ""), "updated_at": now_str},
        {"key": "bootstrap_lookback_days", "value": str(collection.get("bootstrap_lookback_days", 30)), "updated_at": now_str},
        {"key": "refresh_recent_days", "value": str(collection.get("refresh_recent_days", 7)), "updated_at": now_str},
        {"key": "incremental_overlap_hours", "value": str(collection.get("incremental_overlap_hours", 24)), "updated_at": now_str},
        {"key": "last_collect_run_at", "value": now_str, "updated_at": now_str},
        {"key": "last_collect_status", "value": status, "updated_at": now_str},
        {"key": "last_imported_post_count", "value": str(imported_count), "updated_at": now_str},
    ]


def run():
    args = parse_args()
    config = load_config()
    spreadsheet, raw_ws, _, _, state_ws = bootstrap_sheet(config)

    if args.check_only:
        print(
            json.dumps(
                {
                    "spreadsheet_title": spreadsheet.title,
                    "tabs": [ws.title for ws in spreadsheet.worksheets()],
                    "raw_row_count": max(raw_ws.row_count - 1, 0),
                },
                ensure_ascii=False,
            )
        )
        return

    if args.bootstrap_only:
        write_key_value_rows(state_ws, current_state_rows(config, 0, "bootstrapped"))
        print("[OK] Created or validated required worksheets.")
        return

    imported_posts = load_posts_from_json(args.input_json)
    if not imported_posts:
        if args.allow_empty:
            write_key_value_rows(state_ws, current_state_rows(config, 0, "empty_noop"))
            print("[INFO] No input posts were provided. Sheet bootstrap completed.")
            return
        raise RuntimeError(
            "No post import source was provided. Supply --input-json, X_POSTS_JSON, or data/x_posts.json."
        )

    existing_index = {}
    existing_rows = raw_ws.get_all_records(default_blank="")
    for row in existing_rows:
        key = raw_key(row)
        if key:
            existing_index[key] = row

    normalized_rows = []
    for post in imported_posts:
        key = raw_key(post)
        existing = existing_index.get(key, {})
        normalized = normalize_post(post, existing, config)
        normalized_rows.append(normalized)

    upsert_rows(raw_ws, RAW_HEADERS, "post_id", normalized_rows)
    write_key_value_rows(state_ws, current_state_rows(config, len(normalized_rows), "imported"))
    print(f"[OK] Upserted {len(normalized_rows)} rows into {config['sheet_tabs']['raw_posts']}.")


if __name__ == "__main__":
    run()
