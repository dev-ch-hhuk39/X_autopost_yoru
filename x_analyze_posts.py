import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from x_sheet_utils import get_or_create_worksheet, open_spreadsheet, replace_sheet

JST = timezone(timedelta(hours=9))
CONFIG_PATH = Path(__file__).with_name("x_pipeline_config.json")

SCORED_HEADERS = [
    "投稿ID",
    "投稿URL",
    "アカウント",
    "キーワード",
    "投稿日",
    "曜日",
    "時間帯",
    "投稿タイプ",
    "文字数",
    "メディア有無",
    "画像有無",
    "動画有無",
    "切り口",
    "書き出し型",
    "いいね数",
    "リポスト数",
    "返信数",
    "保存数",
    "インプレッション数",
    "総合スコア",
    "アカウント内順位率",
    "キーワード内順位率",
    "バズ判定",
    "アカウント上位20%",
    "キーワード上位20%",
    "伸びた理由",
]

INSIGHT_HEADERS = ["分類", "指標", "値", "示唆", "更新日時"]


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
        ("体験談", ["実際", "体験", "経験", "昔", "わたし", "自分"]),
        ("ノウハウ", ["方法", "コツ", "やり方", "ポイント", "攻略"]),
        ("暴露", ["裏", "暴露", "本音", "闇", "ぶっちゃけ"]),
        ("共感", ["あるある", "つらい", "わかる", "共感", "しんどい"]),
        ("質問", ["?", "？", "どう思う", "教えて", "ありますか"]),
    ]
    for label, patterns in rules:
        if any(pattern in lower for pattern in patterns):
            return label
    return "その他"


def detect_hook_style(text: str) -> str:
    first = (text or "").strip()
    if not first:
        return "不明"
    if first.startswith(("【", "[", "1.", "1 ", "・")):
        return "リスト型"
    if "?" in first[:40] or "？" in first[:40]:
        return "質問型"
    if any(word in first[:40] for word in ["実は", "ぶっちゃけ", "正直", "結論"]):
        return "暴露型"
    if any(word in first[:40] for word in ["今日", "昨日", "この前", "さっき"]):
        return "体験談型"
    return "断定型"


def why_it_grew(row: pd.Series, buzz_likes: int, buzz_impressions: int) -> str:
    reasons: List[str] = []
    if row["like_count"] >= buzz_likes:
        reasons.append(f"いいね{buzz_likes}以上")
    if row["impression_count"] >= buzz_impressions:
        reasons.append(f"インプレッション{buzz_impressions}以上")
    if row["has_image"]:
        reasons.append("画像あり")
    if row["has_video"]:
        reasons.append("動画あり")
    if row["account_percentile"] >= 0.8:
        reasons.append("同一アカウント内で上位20%")
    if row["keyword_percentile"] >= 0.8:
        reasons.append("同一キーワード群で上位20%")
    return "、".join(reasons)


def text_length_bucket(length: int) -> str:
    if length <= 60:
        return "短文(0-60字)"
    if length <= 120:
        return "中短文(61-120字)"
    if length <= 180:
        return "中文(121-180字)"
    return "長文(181字以上)"


def bool_label(value: bool, true_label: str = "あり", false_label: str = "なし") -> str:
    return true_label if bool(value) else false_label


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
    df["keyword_bucket"] = df["matched_keywords"].fillna("").replace("", "キーワードなし")
    df["文字数帯"] = df["text_length"].apply(text_length_bucket)
    df["画像ラベル"] = df["has_image"].apply(lambda v: bool_label(v, "画像あり", "画像なし"))
    df["動画ラベル"] = df["has_video"].apply(lambda v: bool_label(v, "動画あり", "動画なし"))
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
    return f"{top_label}（平均スコア {top_value:.2f}）"


def average_metric_note(df: pd.DataFrame, column: str, target_metric: str) -> str:
    if df.empty or column not in df or target_metric not in df:
        return ""
    winner = df.groupby(column)[target_metric].mean().sort_values(ascending=False)
    if winner.empty:
        return ""
    top_label = winner.index[0]
    top_value = winner.iloc[0]
    metric_label_map = {
        "performance_score": "総合スコア",
        "like_count": "いいね数",
        "impression_count": "インプレッション数",
    }
    metric_label = metric_label_map.get(target_metric, target_metric)
    return f"{top_label}（平均{metric_label} {top_value:.1f}）"


def top_posts_rows(df: pd.DataFrame, updated_at: str) -> List[List[str]]:
    rows: List[List[str]] = []
    top_posts = df.sort_values("performance_score", ascending=False).head(5)
    for idx, row in enumerate(top_posts.itertuples(index=False), start=1):
        rows.append(
            [
                "上位投稿",
                f"上位投稿{idx}",
                row.post_url,
                f"@{row.account_handle} / {row.content_angle} / {row.hook_style} / {row.why_it_grew}",
                updated_at,
            ]
        )
    return rows


def build_insights(df: pd.DataFrame, config: Dict[str, Any]) -> List[List[str]]:
    updated_at = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    thresholds = config["thresholds"]
    rows: List[List[str]] = []

    if df.empty:
        rows.append(["概要", "取得投稿数", "0", "まだ投稿データが入っていません。", updated_at])
        return rows

    buzz_df = df[df["is_buzz_post"]]
    rows.append(["概要", "取得投稿数", str(len(df)), "分析対象として取り込まれている投稿数です。", updated_at])
    rows.append(["概要", "バズ投稿数", str(len(buzz_df)), "いいね100以上 または インプレッション1万以上の投稿数です。", updated_at])
    rows.append(
        [
            "概要",
            "相対評価の基準",
            str(thresholds["relative_top_percent"]),
            "同一アカウント内・同一キーワード群内で上位20%を強投稿として扱っています。",
            updated_at,
        ]
    )

    rows.append(
        [
            "勝ち筋",
            "強い切り口",
            top_metric_note(df, "content_angle"),
            "次に量産する投稿テーマの第一候補です。",
            updated_at,
        ]
    )
    rows.append(
        [
            "勝ち筋",
            "強い書き出し",
            top_metric_note(df, "hook_style"),
            "冒頭の入り方として最も強い型です。",
            updated_at,
        ]
    )
    rows.append(
        [
            "勝ち筋",
            "画像有無の比較",
            average_metric_note(df, "画像ラベル", "performance_score"),
            "画像あり・なしで平均スコアを比べた結果です。",
            updated_at,
        ]
    )
    rows.append(
        [
            "勝ち筋",
            "強い文字数帯",
            top_metric_note(df, "文字数帯"),
            "投稿の長さとして強いレンジです。",
            updated_at,
        ]
    )
    rows.append(
        [
            "勝ち筋",
            "強い時間帯",
            top_metric_note(df, "time_slot"),
            "次の投稿テストで優先して試す時間帯です。",
            updated_at,
        ]
    )
    rows.append(
        [
            "勝ち筋",
            "強い曜日",
            top_metric_note(df, "weekday"),
            "今のデータで最も反応が良い曜日です。",
            updated_at,
        ]
    )
    rows.append(
        [
            "示唆",
            "次に量産すべきテーマ",
            top_metric_note(df, "content_angle"),
            "まずはこの切り口をベースに、画像あり・強い書き出しで量産するのがおすすめです。",
            updated_at,
        ]
    )
    rows.append(
        [
            "示唆",
            "おすすめ投稿フォーマット",
            f"{top_metric_note(df, 'hook_style')} / {top_metric_note(df, '文字数帯')}",
            "書き出し型と文字数帯をセットで再現すると勝ち筋を試しやすいです。",
            updated_at,
        ]
    )
    rows.append(
        [
            "示唆",
            "避けたい弱いパターン",
            df.groupby("content_angle")["performance_score"].mean().sort_values().index[0],
            "平均スコアが最も低い切り口です。頻度を下げる候補として見てください。",
            updated_at,
        ]
    )
    return rows + top_posts_rows(df, updated_at)


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
        scored_df = pd.DataFrame(
            {
                "投稿ID": df["post_id"],
                "投稿URL": df["post_url"],
                "アカウント": df["account_handle"].apply(lambda v: f"@{v}" if str(v) else ""),
                "キーワード": df["matched_keywords"],
                "投稿日": df["posted_at"],
                "曜日": df["weekday"],
                "時間帯": df["time_slot"],
                "投稿タイプ": df["post_type"],
                "文字数": df["text_length"],
                "メディア有無": df["has_media"].map({True: "あり", False: "なし"}),
                "画像有無": df["has_image"].map({True: "あり", False: "なし"}),
                "動画有無": df["has_video"].map({True: "あり", False: "なし"}),
                "切り口": df["content_angle"],
                "書き出し型": df["hook_style"],
                "いいね数": df["like_count"],
                "リポスト数": df["repost_count"],
                "返信数": df["reply_count"],
                "保存数": df["bookmark_count"],
                "インプレッション数": df["impression_count"],
                "総合スコア": df["performance_score"].round(2),
                "アカウント内順位率": (df["account_percentile"] * 100).round(1).astype(str) + "%",
                "キーワード内順位率": (df["keyword_percentile"] * 100).round(1).astype(str) + "%",
                "バズ判定": df["is_buzz_post"].map({True: "該当", False: "非該当"}),
                "アカウント上位20%": df["is_relative_top_account"].map({True: "該当", False: "非該当"}),
                "キーワード上位20%": df["is_relative_top_keyword"].map({True: "該当", False: "非該当"}),
                "伸びた理由": df["why_it_grew"],
            }
        )
        replace_sheet(scored_ws, SCORED_HEADERS, scored_df.fillna("").values.tolist())
    else:
        replace_sheet(scored_ws, SCORED_HEADERS, [])

    insights_rows = build_insights(df, config)
    replace_sheet(insights_ws, INSIGHT_HEADERS, insights_rows)
    print(f"[OK] Analyzed {len(df)} raw posts.")


if __name__ == "__main__":
    run()
