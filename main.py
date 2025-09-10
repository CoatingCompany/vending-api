import os, json
from typing import Optional, List, Dict, Any, Union
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, root_validator
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from datetime import datetime
import zoneinfo
import re

# ------------------ env ------------------

load_dotenv()

SHEET_ID = os.environ.get("SHEET_ID")
TAB_NAME = os.environ.get("TAB_NAME", "Data")
API_KEY  = os.environ.get("API_KEY")
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Sofia")

# ------------------ localization (BG headers) ------------------

# Column name mapping (Sheet row 1 must match these exactly)
COL = {
    "timestamp": "Дата",
    "location":  "Локация",
    "items":     "Консумативи",
    "note":      "Бележка",
    "revenue":   "Оборот",
}

# Order of columns in the sheet
ROW_ORDER = [COL["timestamp"], COL["location"], COL["items"], COL["note"], COL["revenue"]]

# ------------------ app ------------------

app = FastAPI(title="Wonder Toys Sheets API (BG columns)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ------------------ helpers ------------------

def _row_with_aliases(row_map: Dict[str, str]) -> Dict[str, Any]:
    """
    Add ASCII aliases to a BG-keyed row map so the assistant can aggregate.
    Provides: timestamp, location, items_en, note_en, revenue (int).
    """
    out = dict(row_map)  # keep BG keys
    out["timestamp"] = row_map.get(COL["timestamp"], "")
    out["location"]  = row_map.get(COL["location"], "")
    out["items_en"]  = row_map.get(COL["items"], "")
    out["note_en"]   = row_map.get(COL["note"], "")
    rev = row_map.get(COL["revenue"], "")
    try:
        out["revenue"] = int(str(rev).strip()) if str(rev).strip() != "" else 0
    except:
        out["revenue"] = 0
    return out

_sheet_id_cache: Optional[int] = None  # numeric sheet/tab id

def _bg_today_str() -> str:
    tz = zoneinfo.ZoneInfo(TIMEZONE)
    return datetime.now(tz).strftime("%d-%m-%Y")  # DD-MM-YYYY

def _validate_date_ddmmyyyy(value: str) -> str:
    try:
        datetime.strptime(value, "%d-%m-%Y")
    except Exception:
        raise HTTPException(422, detail="Невалидна дата. Използвайте формат DD-MM-YYYY.")
    return value

def _validate_int_or_empty(value: Optional[Union[int, str]]) -> str:
    """Оборот: допуска само цели числа или празно."""
    if value is None or str(value).strip() == "":
        return ""
    s = str(value).strip()
    if not re.fullmatch(r"-?\d+", s):
        raise HTTPException(422, detail="Оборот трябва да е цяло число без десетични точки.")
    return s

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

def _get_values_and_index(svc) -> tuple[List[List[str]], Dict[str, int]]:
    values = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=_sheet_range_all_cols()
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

def _ts_to_epoch_ddmmyyyy(s: str) -> float:
    try:
        return datetime.strptime(s, "%d-%м-%Y").timestamp()
    except Exception:
        # fallback if locale char differs
        try:
            return datetime.strptime(s, "%d-%m-%Y").timestamp()
        except Exception:
            return -1.0

# ------------ normalization (compat) ------------

def _norm_items_to_string(val: Optional[Union[str, List[str]]]) -> Optional[str]:
    """Консумативи: приемаме 'a, b' или ['a','b'] → 'a, b'."""
    if val is None:
        return None
    if isinstance(val, list):
        parts = [str(x).strip() for x in val if str(x).strip()]
        return ", ".join(parts)
    return str(val).strip()

# ------------------ models ------------------

class AppendRequest(BaseModel):
    location: str
    items: Optional[Union[str, List[str]]] = None
    note: Optional[str] = None
    revenue: Optional[Union[int, str]] = Field(None, description="цяло число")
    timestamp: Optional[str] = Field(None, description="DD-MM-YYYY")

    # compatibility with older prompts / schema
    product: Optional[str] = None
    products: Optional[Union[str, List[str]]] = None
    notes: Optional[str] = None

    @root_validator(pre=True)
    def _merge_legacy(cls, v):
        if v.get("items") is None:
            prod = v.get("product")
            prods = v.get("products")
            if prods is not None:
                v["items"] = prods
            elif prod is not None:
                v["items"] = prod
        if v.get("note") is None and v.get("notes") is not None:
            v["note"] = v["notes"]
        return v

class QueryFilters(BaseModel):
    location: Optional[str] = None
    product: Optional[str] = None  # matches item token equal (case-insensitive)
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
            prod = v.get("product")
            prods = v.get("products")
            if prods is not None:
                v["items"] = prods
            elif prod is not None:
                v["items"] = prod
        if v.get("note") is None and v.get("notes") is not None:
            v["note"] = v["notes"]
        return v

class DeleteRowRequest(BaseModel):
    row_number: int = Field(..., ge=2, description="1 = header; data starts at 2")

# ------------------ endpoints ------------------

@app.get("/health")
def health():
    return {"ok": True, "tz": TIMEZONE, "date_format": "DD-MM-YYYY", "columns": ROW_ORDER}

@app.post("/append")
def append_row(payload: AppendRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()

    date_str = payload.timestamp.strip() if payload.timestamp else _bg_today_str()
    _validate_date_ddmmyyyy(date_str)

    items_str = _norm_items_to_string(payload.items)
    if not items_str:
        raise HTTPException(422, detail="Задължително поле: консумативи (items).")

    revenue_str = _validate_int_or_empty(payload.revenue)

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
    # return with aliases so the assistant can aggregate revenue
    return {"ok": True, "row": _row_with_aliases(row), "update": result}

@app.get("/last-product")  # keeping path for compatibility; returns last consumables
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
    matches = [r for r in rows if len(r) > idx[COL["location"]] and r[idx[COL["location"]]].strip().lower() == loc]
    if not matches:
        raise HTTPException(404, f"Няма редове за локация '{location}'.")

    last_row = max(matches, key=lambda r: _ts_to_epoch_ddmmyyyy(r[idx[COL["timestamp"]]] if idx[COL["timestamp"]] < len(r) else ""))
    def get(colkey):
        c = COL[colkey]
        return last_row[idx[c]] if idx[c] < len(last_row) else ""

    items_raw = get("items") or ""
    items = [p.strip() for p in items_raw.split(",") if p.strip()]
    return {
        "location": location,
        "timestamp": get("timestamp"),
        "items": items,
        "last_item": (items[-1] if items else ""),
        "note": get("note"),
        "revenue": get("revenue"),
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
        if filters.location and get("location").strip().lower() != filters.location.strip().lower():
            return False
        if filters.product:
            # exact token match (case-insensitive) in "Консумативи"
            prod = filters.product.strip().lower()
            tokens = [p.strip().lower() for p in (get("items") or "").split(",") if p.strip()]
            if prod not in tokens:
                return False
        if filters.since_ts or filters.until_ts:
            ts_val = _ts_to_epoch_ddmmyyyy(get("timestamp"))
            if ts_val < 0:
                return False
            if filters.since_ts and ts_val < filters.since_ts: return False
            if filters.until_ts and ts_val > filters.until_ts: return False
        return True

    out = []
    for i, r in enumerate(values[1:], start=2):
        if ok(r):
            obj_bg = {col: (r[idx[col]] if idx[col] < len(r) else "") for col in ROW_ORDER}
            obj_bg["row_number"] = i
            out.append(_row_with_aliases(obj_bg))
            if len(out) >= (filters.limit or 50): break
    return {"rows": out}

@app.post("/update-row")
def update_row(patch: UpdateRowRequest, x_api_key: Optional[str] = Header(None)):
    """
    Частичен update на ред (row_number >= 2).
    """
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
        items_str = _norm_items_to_string(patch.items)
        if not items_str:
            raise HTTPException(422, detail="Невалидни консумативи.")
        current[COL["items"]] = items_str
    if patch.note is not None:
        current[COL["note"]] = str(patch.note).strip()
    if patch.revenue is not None:
        current[COL["revenue"]] = _validate_int_or_empty(patch.revenue)

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
            spreadsheetId=SHEET_ID, range=a1
        ).execute().get("values", [[]])
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (verify): {e}")

    row = new_vals[0] if new_vals else []
    if len(row) < len(ROW_ORDER):
        row += [""] * (len(ROW_ORDER) - len(row))
    returned = {ROW_ORDER[i]: row[i] for i in range(len(ROW_ORDER))}
    returned["row_number"] = patch.row_number
    # return with aliases so the assistant can aggregate revenue
    return {"ok": True, "row": _row_with_aliases(returned)}

@app.post("/delete-row")
def delete_row(req: DeleteRowRequest, x_api_key: Optional[str] = Header(None)):
    """
    Изтриване на конкретен ред (>= 2).
    """
    require_api_key(x_api_key)
    svc = sheets_service()

    try:
        values, _ = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")
    if not values or req.row_number < 2 or req.row_number > len(values):
        raise HTTPException(404, f"Ред {req.row_number} не е намерен.")

    try:
        # physical delete (same as before)
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

# ------------------ NEW: sum revenue ------------------

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
            if get_val(r, "location").strip().lower() != location.strip().lower():
                continue
        if since_ts or until_ts:
            try:
                ts_val = datetime.strptime(get_val(r, "timestamp"), "%d-%m-%Y").timestamp()
            except:
                continue
            if since_ts and ts_val < since_ts:
                continue
            if until_ts and ts_val > until_ts:
                continue

        rev_raw = get_val(r, "revenue")
        try:
            total += int(str(rev_raw).strip())
        except:
            pass
        rows_count += 1

    return {"total_revenue": total, "rows": rows_count}
