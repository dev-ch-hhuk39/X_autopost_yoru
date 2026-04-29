import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from x_sheet_utils import get_or_create_worksheet, open_spreadsheet, replace_sheet

JST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")

SCORED_HEADERS = [
    "post_id",
    "post_url",
    "account_handle",
    "matched_keywords",
    "posted_at",
    "weekday",
    "time_slot",
    "post_type",
    "text_length",
    "has_media",
    "has_image",
    "has_video",
    "content_angle",
    "hook_style",
    "like_count",
    "repost_count",
    "reply_count",
    "bookmark_count",
    "impression_count",
    "performance_score",
    "account_percentile",
    "keyword_percentile",
    "is_buzz_post",
    "is_relative_top_account",
    "is_relative_top_keyword",
    "why_it_grew",
]

INSIGHT_HEADERS = ["section", "metric", "value", "note", "updated_at"]


def load_config():
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return 0


def to_bool(value: Any) -> bool:
    return str(value).strip().upper() in {"TRUE", "1", "YES"}


def detect_content_angle(text: str) -> str:
    lower = (text or "").lower()
    rules = [
        ("experience", ["実際", "体験", "経験", "昔", "わたし", "自分"]),
        ("know_how", ["方法", "コツ", "やり方", "ポイント", "攻略"]),
        ("expose", ["裏", "暴露", "本音", "闇", "ぶっちゃけ"]),
        ("empathy", ["あるある", "つらい", "わかる", "共感", "しんどい"]),
        ("question", ["?", "？", "どう思う", "教えて", "ありますか"]),
    ]
    for label, patterns in rules:
        if any(pattern in lower for pattern in patterns):
            return label
    return "other"


def detect_hook_style(text: str) -> str:
    first = (text or "").strip()
    if not first:
        return "unknown"
    if first.startswith(("【", "[", "1.", "1 ", "・")):
        return "list_or_framed"
    if "?" in first[:40] or "？" in first[:40]:
        return "question_hook"
    if any(word in first[:40] for word in ["実は", "ぶっちゃけ", "正直", "結論"]):
        return "reveal_hook"
    if any(word in first[:40] for word in ["今日", "昨日", "この前", "さっき"]):
        return "story_hook"
    return "statement_hook"


def why_it_grew(row: pd.Series, buzz_likes: int, buzz_impressions: int) -> str:
    reasons: List[str] = []
    if row["like_count"] >= buzz_likes:
        reasons.append(f"like_count>={buzz_likes}")
    if row["impression_count"] >= buzz_impressions:
        reasons.append(f"impression_count>={buzz_impressions}")
    if row["has_image"]:
        reasons.append("image_attached")
    if row["has_video"]:
        reasons.append("video_attached")
    if row["account_percentile"] >= 0.8:
        reasons.append("top20_within_account")
    if row["keyword_percentile"] >= 0.8:
        reasons.append("top20_within_keyword")
    return " | ".join(reasons)


def build_dataframe(raw_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(raw_rows)
    if df.empty:
        return df

    numeric_cols = [
        "text_length",
        "like_count",
        "repost_count",
        "reply_count",
        "bookmark_count",
        "impression_count",
        "image_count",
        "follower_count",
    ]
    for col in numeric_cols:
        df[col] = df[col].apply(to_int)

    bool_cols = ["has_media", "has_image", "has_video"]
    for col in bool_cols:
        df[col] = df[col].apply(to_bool)

    df["performance_score"] = (
        df["like_count"]
        + (df["repost_count"] * 3)
        + (df["reply_count"] * 2)
        + (df["bookmark_count"] * 4)
        + (df["impression_count"] / 100.0)
    )
    df["content_angle"] = df["text"].fillna("").apply(detect_content_angle)
    df["hook_style"] = df["hook_text"].fillna(df["text"].fillna("")).apply(detect_hook_style)
    df["keyword_bucket"] = df["matched_keywords"].fillna("").replace("", "__none__")
    df["account_percentile"] = df.groupby("account_handle")["performance_score"].rank(pct=True, method="average")
    df["keyword_percentile"] = df.groupby("keyword_bucket")["performance_score"].rank(pct=True, method="average")
    return df


def top_metric_note(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df:
        return ""
    winner = df.groupby(column)["performance_score"].mean().sort_values(ascending=False)
    if winner.empty:
        return ""
    top_label = winner.index[0]
    top_value = winner.iloc[0]
    return f"{top_label}: avg_performance_score={top_value:.2f}"


def build_insights(df: pd.DataFrame, config: Dict[str, Any]) -> List[List[str]]:
    updated_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    thresholds = config["thresholds"]
    rows: List[List[str]] = []

    if df.empty:
        rows.append(["summary", "raw_post_count", "0", "No raw posts available yet.", updated_at])
        return rows

    buzz_df = df[df["is_buzz_post"]]
    rows.append(["summary", "raw_post_count", str(len(df)), "Imported posts currently stored.", updated_at])
    rows.append(["summary", "buzz_post_count", str(len(buzz_df)), "like>=100 or impression>=10000.", updated_at])
    rows.append(
        [
            "summary",
            "relative_top_threshold",
            str(thresholds["relative_top_percent"]),
            "Top percent cutoff used for account and keyword comparisons.",
            updated_at,
        ]
    )

    rows.append(
        [
            "theme",
            "best_content_angle",
            top_metric_note(df, "content_angle"),
            "Use this as the primary theme family for the next batch of ideas.",
            updated_at,
        ]
    )
    rows.append(
        [
            "hook",
            "best_hook_style",
            top_metric_note(df, "hook_style"),
            "This hook shape currently leads on average performance score.",
            updated_at,
        ]
    )
    rows.append(
        [
            "media",
            "image_vs_no_image",
            top_metric_note(df, "has_image"),
            "Check whether image-backed posts outperform text-only posts.",
            updated_at,
        ]
    )
    rows.append(
        [
            "timing",
            "best_time_slot",
            top_metric_note(df, "time_slot"),
            "Use this slot first when scheduling experiments.",
            updated_at,
        ]
    )
    rows.append(
        [
            "timing",
            "best_weekday",
            top_metric_note(df, "weekday"),
            "Strongest weekday based on current monitored dataset.",
            updated_at,
        ]
    )

    top_posts = df.sort_values("performance_score", ascending=False).head(5)
    for idx, row in enumerate(top_posts.itertuples(index=False), start=1):
        rows.append(
            [
                "top_posts",
                f"top_post_{idx}",
                row.post_url,
                f"{row.account_handle} | {row.why_it_grew}",
                updated_at,
            ]
        )
    return rows


def run():
    config = load_config()
    spreadsheet = open_spreadsheet()
    tabs = config["sheet_tabs"]
    raw_ws = get_or_create_worksheet(spreadsheet, tabs["raw_posts"])
    scored_ws = get_or_create_worksheet(spreadsheet, tabs["scored_posts"], rows=5000, cols=len(SCORED_HEADERS) + 5)
    insights_ws = get_or_create_worksheet(spreadsheet, tabs["insights"], rows=1000, cols=len(INSIGHT_HEADERS) + 2)

    raw_rows = raw_ws.get_all_records(default_blank="")
    df = build_dataframe(raw_rows)
    thresholds = config["thresholds"]

    if not df.empty:
        df["is_buzz_post"] = (df["like_count"] >= thresholds["buzz_like_count"]) | (
            df["impression_count"] >= thresholds["buzz_impression_count"]
        )
        cutoff = 1 - (thresholds["relative_top_percent"] / 100.0)
        df["is_relative_top_account"] = df["account_percentile"] >= cutoff
        df["is_relative_top_keyword"] = df["keyword_percentile"] >= cutoff
        df["why_it_grew"] = df.apply(
            lambda row: why_it_grew(row, thresholds["buzz_like_count"], thresholds["buzz_impression_count"]),
            axis=1,
        )
        scored_df = df[SCORED_HEADERS].copy()
        scored_df["has_media"] = scored_df["has_media"].map({True: "TRUE", False: "FALSE"})
        scored_df["has_image"] = scored_df["has_image"].map({True: "TRUE", False: "FALSE"})
        scored_df["has_video"] = scored_df["has_video"].map({True: "TRUE", False: "FALSE"})
        scored_df["is_buzz_post"] = scored_df["is_buzz_post"].map({True: "TRUE", False: "FALSE"})
        scored_df["is_relative_top_account"] = scored_df["is_relative_top_account"].map({True: "TRUE", False: "FALSE"})
        scored_df["is_relative_top_keyword"] = scored_df["is_relative_top_keyword"].map({True: "TRUE", False: "FALSE"})
        replace_sheet(scored_ws, SCORED_HEADERS, scored_df.fillna("").values.tolist())
    else:
        replace_sheet(scored_ws, SCORED_HEADERS, [])

    insights_rows = build_insights(df, config)
    replace_sheet(insights_ws, INSIGHT_HEADERS, insights_rows)
    print(f"[OK] Analyzed {len(df)} raw posts.")


if __name__ == "__main__":
    run()
