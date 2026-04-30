import base64
import json
import os
import tempfile
from typing import Dict, Iterable, List, Sequence, Tuple

import gspread


def column_letter(index: int) -> str:
    result = []
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def sanitize_cell(value):
    text = str(value)
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def get_gspread_client():
    b64 = os.environ.get("SA_JSON_BASE64", "").strip()
    raw = os.environ.get("GCP_SA_JSON", "").strip()

    if b64:
        decoded = base64.b64decode(b64)
        tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="wb")
        try:
            tmp.write(decoded)
            tmp.close()
            return gspread.service_account(filename=tmp.name)
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

    if raw:
        from google.oauth2.service_account import Credentials

        info = json.loads(raw)
        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return gspread.authorize(creds)

    return gspread.service_account()


def open_spreadsheet():
    gc = get_gspread_client()
    sheet_url = os.environ.get("SHEET_URL", "").strip()
    sheet_id = os.environ.get("SHEET_ID", "").strip()

    if sheet_url:
        return gc.open_by_url(sheet_url)
    if sheet_id:
        return gc.open_by_key(sheet_id)
    raise RuntimeError("SHEET_URL or SHEET_ID must be set")


def get_or_create_worksheet(spreadsheet, title: str, rows: int = 2000, cols: int = 40):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_headers(ws, headers: Sequence[str]) -> List[str]:
    values = ws.get_all_values()
    if not values:
        ws.update(range_name="1:1", values=[list(headers)])
        return list(headers)

    current = values[0]
    changed = False
    for header in headers:
        if header not in current:
            current.append(header)
            changed = True

    if changed:
        ws.update(range_name="1:1", values=[current])
    return current


def records_with_row_numbers(ws, headers: Sequence[str]) -> List[Tuple[int, Dict[str, str]]]:
    values = ws.get_all_values()
    if len(values) <= 1:
        return []

    rows = []
    for row_number, raw in enumerate(values[1:], start=2):
        row = {header: (raw[idx] if idx < len(raw) else "") for idx, header in enumerate(headers)}
        rows.append((row_number, row))
    return rows


def replace_sheet(ws, headers: Sequence[str], rows: Iterable[Sequence[str]]):
    payload = [[sanitize_cell(cell) for cell in list(headers)]]
    payload.extend([[sanitize_cell(cell) for cell in list(row)] for row in rows])
    ws.clear()
    ws.update(range_name="A1", values=payload, raw=True)


def upsert_rows(
    ws,
    headers: Sequence[str],
    unique_header: str,
    rows: Iterable[Dict[str, str]],
):
    header_list = ensure_headers(ws, headers)
    existing = records_with_row_numbers(ws, header_list)
    index = {}
    for row_number, row in existing:
        key = str(row.get(unique_header, "")).strip()
        if key:
            index[key] = (row_number, row)

    appended: List[List[str]] = []
    for row in rows:
        key = str(row.get(unique_header, "")).strip()
        if not key:
            continue

        ordered = [sanitize_cell(row.get(header, "")) for header in header_list]
        if key in index:
            row_number, _ = index[key]
            start = "A"
            end_col = column_letter(len(header_list))
            ws.update(range_name=f"{start}{row_number}:{end_col}{row_number}", values=[ordered])
        else:
            appended.append(ordered)
            index[key] = (-1, row)

    if appended:
        ws.append_rows(appended, value_input_option="RAW")


def write_key_value_rows(ws, rows: Iterable[Dict[str, str]]):
    headers = ["key", "value", "updated_at"]
    replace_sheet(
        ws,
        headers,
        ([row.get("key", ""), row.get("value", ""), row.get("updated_at", "")] for row in rows),
    )
