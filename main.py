import os, json, re
from typing import Optional, List, Dict, Any, Union
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, root_validator
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from datetime import datetime, timedelta
import zoneinfo

# ------------------ env ------------------

load_dotenv()

SHEET_ID = os.environ.get("SHEET_ID")
TAB_NAME = os.environ.get("TAB_NAME", "Data")
API_KEY  = os.environ.get("API_KEY")
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Sofia")

# ------------------ localization (BG headers) ------------------

COL = {
    "timestamp": "Дата",
    "location":  "Локация",
    "items":     "Консумативи",
    "note":      "Бележка",
    "revenue":   "Оборот",
}
ROW_ORDER = [COL["timestamp"], COL["location"], COL["items"], COL["note"], COL["revenue"]]

# ------------------ app ------------------

app = FastAPI(title="Wonder Toys Sheets API (BG columns)", version="2.2.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ------------------ helpers ------------------

_tz = zoneinfo.ZoneInfo(TIMEZONE)
_sheet_id_cache: Optional[int] = None  # numeric sheet/tab id

def _bg_today_str() -> str:
    return datetime.now(_tz).strftime("%d-%m-%Y")  # DD-MM-YYYY

def _validate_date_ddmmyyyy(value: str) -> str:
    try:
        datetime.strptime(value, "%d-%m-%Y")
    except Exception:
        raise HTTPException(422, detail="Невалидна дата. Използвайте формат DD-MM-YYYY.")
    return value

def _parse_int_loose(value: Any) -> int:
    """Extract a whole number from '1 200', '1,200', 'лв 120', etc."""
    if value is None:
        return 0
    s = str(value)
    s = s.replace("\u00A0", " ").replace("\u2009", " ").replace("\u202F", " ")
    s_nogroup = s.replace(",", "").replace(" ", "")
    m = re.search(r'[-+]?\d+', s_nogroup) or re.search(r'[-+]?\d+', s)
    return int(m.group(0)) if m else 0

def _excel_serial_to_epoch(serial: Union[int, float]) -> float:
    """Excel serial → epoch seconds (midnight local). Excel's day 0 is 1899-12-30."""
    try:
        d = datetime(1899, 12, 30, tzinfo=_tz) + timedelta(days=float(serial))
        # normalize to date (strip time if any)
        d = datetime(d.year, d.month, d.day, tzinfo=_tz)
        return d.timestamp()
    except Exception:
        return None  # type: ignore

# Month names (EN + BG)
MONTHS = {
    # English
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    # Bulgarian
    "януари":1,"февруари":2,"март":3,"април":4,"май":5,"юни":6,
    "юли":7,"август":8,"септември":9,"октомври":10,"ноември":11,"декември":12,
}

_DATE_PATTERNS = [
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%d-%m-%y",
    "%d.%m.%y",
    "%d/%m/%y",
]

def _cell_date_to_epoch(cell: Any) -> Optional[float]:
    """
    Accept Excel serials, numeric date strings, and names like '1 март 2024' / '01 March 2024'.
    Return epoch seconds (local midnight) or None if unparseable.
    """
    if cell is None or cell == "":
        return None

    # Excel date serial (with UNFORMATTED_VALUE)
    if isinstance(cell, (int, float)) and not isinstance(cell, bool):
        return _excel_serial_to_epoch(cell)

    s = str(cell).strip()
    if s == "":
        return None

    # normalize thin/non-breaking spaces and strip trailing 'г.' (year) tokens
    s = (s.replace("\u00A0", " ")
         .replace("\u2009", " ")
         .replace("\u202F", " "))
    s = re.sub(r"\s*г\.?$", "", s, flags=re.IGNORECASE)  # remove Bulgarian 'г.' suffix

    # Try common numeric patterns first
    for fmt in _DATE_PATTERNS:
        try:
            dt = datetime.strptime(s, fmt)
            dt = datetime(dt.year, dt.month, dt.day, tzinfo=_tz)
            return dt.timestamp()
        except Exception:
            pass

    # Try flexible separator normalization for numeric dates
    s2 = re.sub(r"[.\-/]", "-", s)
    try:
        dt = datetime.strptime(s2, "%d-%m-%Y")
        dt = datetime(dt.year, dt.month, dt.day, tzinfo=_tz)
        return dt.timestamp()
    except Exception:
        pass

    # Month-name pattern: "1 март 2024" or "01 March 2024"
    m = re.match(r"^\s*(\d{1,2})\s+([A-Za-zА-Яа-я]+)\s+(\d{4})\s*$", s)
    if m:
        day = int(m.group(1))
        name = m.group(2).lower()
        year = int(m.group(3))
        month = MONTHS.get(name)
        if month and 1 <= day <= 31:
            try:
                dt = datetime(year, month, day, tzinfo=_tz)
                return dt.timestamp()
            except Exception:
                return None

    return None

def _row_with_aliases(row_map: Dict[str, Any]) -> Dict[str, Any]:
    """Add ASCII aliases so the assistant can aggregate."""
    out = dict(row_map)  # keep BG keys
    out["timestamp"] = row_map.get(COL["timestamp"], "")
    out["location"]  = row_map.get(COL["location"], "")
    out["items_en"]  = row_map.get(COL["items"], "")
    out["note_en"]   = row_map.get(COL["note"], "")
    out["revenue"]   = _parse_int_loose(row_map.get(COL["revenue"], ""))
    return out

def sheets_service():
    svc_json = os.environ.get("SERVICE_ACCOUNT_JSON")
    svc_file = os.environ.get("SERVICE_ACCOUNT_FILE")
    if not svc_json and not svc_file:
        raise RuntimeError("Provide SERVICE_ACCOUNT_JSON or SERVICE_ACCOUNT_FILE.")
    if svc_json:
        info = json.loads(svc_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        if not os.path.exists(svc_file):
            raise RuntimeError(f"SERVICE_ACCOUNT_FILE not found at: {svc_file}")
        creds = service_account.Credentials.from_service_account_file(svc_file, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def require_api_key(x_api_key: Optional[str]):
    if not API_KEY:
        raise HTTPException(500, "Server missing API_KEY.")
    if x_api_key != API_KEY:
        raise HTTPException(401, "Invalid API key.")

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _sheet_range_all_cols() -> str:
    last = _col_letter(len(ROW_ORDER))  # A..E
    return f"{TAB_NAME}!A:{last}"

def _get_values_and_index(svc, value_render="UNFORMATTED_VALUE") -> tuple[List[List[Any]], Dict[str, int]]:
    """
    Use UNFORMATTED_VALUE so dates come as numbers when the cell is a true date.
    We'll parse both numbers and strings in _cell_date_to_epoch.
    """
    values = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=_sheet_range_all_cols(),
        valueRenderOption=value_render,
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute().get("values", [])
    if not values or len(values) < 1:
        return [], {}
    header = values[0]
    idx = {name: i for i, name in enumerate(header)}
    return values, idx

def _require_header(idx: Dict[str, int]):
    for k in ROW_ORDER:
        if k not in idx:
            raise HTTPException(
                500,
                detail=f"Липсваща колона в заглавния ред. Очаква се точно: {ROW_ORDER}"
            )

# ------------------ models ------------------

class AppendRequest(BaseModel):
    location: str
    items: Optional[Union[str, List[str]]] = None
    note: Optional[str] = None
    revenue: Optional[Union[int, str]] = Field(None, description="цяло число")
    timestamp: Optional[str] = Field(None, description="DD-MM-YYYY")
    # legacy
    product: Optional[str] = None
    products: Optional[Union[str, List[str]]] = None
    notes: Optional[str] = None
    @root_validator(pre=True)
    def _merge_legacy(cls, v):
        if v.get("items") is None:
            prods = v.get("products")
            prod = v.get("product")
            v["items"] = prods if prods is not None else prod
        if v.get("note") is None and v.get("notes") is not None:
            v["note"] = v["notes"]
        return v

class QueryFilters(BaseModel):
    location: Optional[str] = None
    product: Optional[str] = None
    since_ts: Optional[float] = Field(None, description="UNIX timestamp lower bound")
    until_ts: Optional[float] = Field(None, description="UNIX timestamp upper bound")
    limit: Optional[int] = Field(50, ge=1, le=500)

class UpdateRowRequest(BaseModel):
    row_number: int = Field(..., ge=2, description="1 = header; data starts at 2")
    location: Optional[str] = None
    items: Optional[Union[str, List[str]]] = None
    note: Optional[str] = None
    revenue: Optional[Union[int, str]] = None
    timestamp: Optional[str] = Field(None, description="DD-MM-YYYY")
    # legacy aliases
    product: Optional[str] = None
    products: Optional[Union[str, List[str]]] = None
    notes: Optional[str] = None
    @root_validator(pre=True)
    def _merge_legacy(cls, v):
        if v.get("items") is None:
            prods = v.get("products")
            prod = v.get("product")
            v["items"] = prods if prods is not None else prod
        if v.get("note") is None and v.get("notes") is not None:
            v["note"] = v["notes"]
        return v

class DeleteRowRequest(BaseModel):
    row_number: int = Field(..., ge=2, description="1 = header; data starts at 2")

# ------------------ endpoints ------------------

@app.get("/health")
def health():
    return {"ok": True, "tz": TIMEZONE, "date_format": "DD-MM-YYYY", "columns": ROW_ORDER, "version": app.version}

@app.post("/append")
def append_row(payload: AppendRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()

    date_str = payload.timestamp.strip() if payload.timestamp else _bg_today_str()
    _validate_date_ddmmyyyy(date_str)

    # normalize items
    if isinstance(payload.items, list):
        items_str = ", ".join([str(x).strip() for x in payload.items if str(x).strip()])
    else:
        items_str = (payload.items or payload.product or "") if payload.items or payload.product else ""
        items_str = str(items_str).strip()
    if not items_str:
        raise HTTPException(422, detail="Задължително поле: консумативи (items).")

    revenue_str = str(_parse_int_loose(payload.revenue)) if payload.revenue not in (None, "") else ""

    row = {
        COL["timestamp"]: date_str,
        COL["location"]: payload.location.strip(),
        COL["items"]: items_str,
        COL["note"]: "" if not payload.note else str(payload.note).strip(),
        COL["revenue"]: revenue_str,
    }
    values = [[row[k] for k in ROW_ORDER]]
    try:
        result = svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=_sheet_range_all_cols(),
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (append): {e}")
    return {"ok": True, "row": _row_with_aliases(row), "update": result}

@app.get("/last-product")  # returns latest entry (by date) for a location
def last_product(location: str = Query(...), x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()
    try:
        values, idx = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")

    if not values or len(values) < 2:
        raise HTTPException(404, "Няма данни.")
    _require_header(idx)

    loc = location.strip().lower()
    rows = values[1:]
    matches: List[tuple[float, List[Any], int]] = []
    for i, r in enumerate(rows, start=2):
        if len(r) > idx[COL["location"]] and str(r[idx[COL["location"]]]).strip().lower() == loc:
            epoch = _cell_date_to_epoch(r[idx[COL["timestamp"]]] if idx[COL["timestamp"]] < len(r) else "")
            if epoch is not None:
                matches.append((epoch, r, i))

    if matches:
        # pick by max epoch
        epoch, last_row, rownum = max(matches, key=lambda t: t[0])
    else:
        # fallback: last occurrence by row order
        fallback = None
        for i, r in enumerate(rows, start=2):
            if len(r) > idx[COL["location"]] and str(r[idx[COL["location"]]]).strip().lower() == loc:
                fallback = (r, i)
        if not fallback:
            raise HTTPException(404, f"Няма редове за локация '{location}'.")
        last_row, rownum = fallback

    def get(colkey):
        c = COL[colkey]
        return last_row[idx[c]] if idx[c] < len(last_row) else ""

    items_raw = get("items") or ""
    items = [p.strip() for p in str(items_raw).split(",") if p.strip()]
    return {
        "location": location,
        "timestamp": str(get("timestamp")),
        "items": items,
        "last_item": (items[-1] if items else ""),
        "note": str(get("note")),
        "revenue": str(get("revenue")),
        "row_number": rownum,
    }

@app.post("/search")
def search_rows(filters: QueryFilters, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()
    try:
        values, idx = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")

    if not values or len(values) < 2:
        return {"rows": []}
    _require_header(idx)

    def ok(r):
        def get(colkey):
            c = COL[colkey]
            return r[idx[c]] if idx[c] < len(r) else ""
        if filters.location and str(get("location")).strip().lower() != filters.location.strip().lower():
            return False
        if filters.product:
            prod = filters.product.strip().lower()
            tokens = [p.strip().lower() for p in str(get("items")).split(",") if p.strip()]
            if prod not in tokens:
                return False
        if filters.since_ts or filters.until_ts:
            ts_val = _cell_date_to_epoch(get("timestamp"))
            # if we cannot parse the date, exclude from date-filtered queries
            if ts_val is None:
                return False
            if filters.since_ts and ts_val < float(filters.since_ts): return False
            if filters.until_ts and ts_val > float(filters.until_ts): return False
        return True

    out = []
    for i, r in enumerate(values[1:], start=2):
        if ok(r):
            obj_bg: Dict[str, Any] = {col: (r[idx[col]] if idx[col] < len(r) else "") for col in ROW_ORDER}
            obj_bg["row_number"] = i
            out.append(_row_with_aliases(obj_bg))
            if len(out) >= (filters.limit or 50): break
    return {"rows": out}

@app.post("/update-row")
def update_row(patch: UpdateRowRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()

    try:
        values, idx = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")
    if not values or patch.row_number < 2 or patch.row_number > len(values):
        raise HTTPException(404, f"Ред {patch.row_number} не е намерен.")
    _require_header(idx)

    existing = values[patch.row_number - 1]
    current: Dict[str, Any] = {k: (existing[idx[k]] if idx[k] < len(existing) else "") for k in ROW_ORDER}

    if patch.timestamp is not None:
        current[COL["timestamp"]] = _validate_date_ddmmyyyy(patch.timestamp.strip())
    if patch.location is not None:
        current[COL["location"]] = patch.location.strip()
    if patch.items is not None:
        if isinstance(patch.items, list):
            items_str = ", ".join([str(x).strip() for x in patch.items if str(x).strip()])
        else:
            items_str = str(patch.items).strip()
        if not items_str:
            raise HTTPException(422, detail="Невалидни консумативи.")
        current[COL["items"]] = items_str
    if patch.note is not None:
        current[COL["note"]] = str(patch.note).strip()
    if patch.revenue is not None:
        current[COL["revenue"]] = str(_parse_int_loose(patch.revenue)) if str(patch.revenue).strip() != "" else ""

    last_col_letter = _col_letter(len(ROW_ORDER))
    a1 = f"{TAB_NAME}!A{patch.row_number}:{last_col_letter}{patch.row_number}"
    try:
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=a1,
            valueInputOption="USER_ENTERED",
            body={"values": [[current[k] for k in ROW_ORDER]]},
        ).execute()
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (update): {e}")

    # read-after-write
    try:
        new_vals = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=a1,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute().get("values", [[]])
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (verify): {e}")

    row = new_vals[0] if new_vals else []
    if len(row) < len(ROW_ORDER):
        row += [""] * (len(ROW_ORDER) - len(row))
    returned = {ROW_ORDER[i]: row[i] for i in range(len(ROW_ORDER))}
    returned["row_number"] = patch.row_number
    return {"ok": True, "row": _row_with_aliases(returned)}

@app.post("/delete-row")
def delete_row(req: DeleteRowRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()

    try:
        values, _ = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")
    if not values or req.row_number < 2 or req.row_number > len(values):
        raise HTTPException(404, f"Ред {req.row_number} не е намерен.")

    try:
        meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_id = None
        for sh in meta.get("sheets", []):
            if sh["properties"]["title"] == TAB_NAME:
                sheet_id = sh["properties"]["sheetId"]
                break
        if sheet_id is None:
            raise HTTPException(500, "Tab not found to resolve sheetId.")

        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={
                "requests": [{
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": req.row_number - 1,
                            "endIndex": req.row_number
                        }
                    }
                }]
            }
        ).execute()
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (delete): {e}")

    return {"ok": True, "deleted_row_number": req.row_number}

# ------------------ sum revenue ------------------

@app.get("/sum-revenue")
def sum_revenue(
    location: Optional[str] = Query(None, description="Filter by location (case-insensitive exact match)"),
    since_ts: Optional[float] = Query(None, description="UNIX seconds inclusive lower bound"),
    until_ts: Optional[float] = Query(None, description="UNIX seconds inclusive upper bound"),
    x_api_key: Optional[str] = Header(None),
):
    require_api_key(x_api_key)
    svc = sheets_service()

    try:
        values, idx = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")

    if not values or len(values) < 2:
        return {"total_revenue": 0, "rows": 0}

    _require_header(idx)

    def get_val(r, key):
        col = COL[key]
        return r[idx[col]] if idx[col] < len(r) else ""

    total = 0
    rows_count = 0

    for r in values[1:]:
        if location:
            if str(get_val(r, "location")).strip().lower() != location.strip().lower():
                continue
        if since_ts or until_ts:
            ts_val = _cell_date_to_epoch(get_val(r, "timestamp"))
            if ts_val is None:  # cannot evaluate date → exclude from a date-filtered sum
                continue
            if since_ts and ts_val < since_ts:
                continue
            if until_ts and ts_val > until_ts:
                continue
        rev_raw = get_val(r, "revenue")
        total += _parse_int_loose(rev_raw)
        rows_count += 1

    return {"total_revenue": total, "rows": rows_count}
