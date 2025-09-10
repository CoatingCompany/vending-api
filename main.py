import os, time, json
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from datetime import datetime
import zoneinfo

# Load .env if present (local dev); on Render the env is provided by Settings → Environment
load_dotenv()

SHEET_ID = os.environ.get("SHEET_ID")
TAB_NAME = os.environ.get("TAB_NAME", "Data")
API_KEY  = os.environ.get("API_KEY")
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Sofia")

# sheet columns order (must match header row in the sheet)
ROW_ORDER = ["timestamp", "location", "product", "quantity", "note", "user"]

app = FastAPI(title="Wonder Toys Sheets API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ---------- helpers ----------

_sheet_id_cache: Optional[int] = None  # numeric sheet/tab id

def _bg_today_str() -> str:
    tz = zoneinfo.ZoneInfo(TIMEZONE)
    return datetime.now(tz).strftime("%d-%m-%Y")  # DD-MM-YYYY

def sheets_service():
    # Either SERVICE_ACCOUNT_JSON (full JSON text) OR SERVICE_ACCOUNT_FILE (path) must be set
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

def a1_range_for_tab(tab: str, start_col="A", end_col="F"):
    return f"{tab}!{start_col}:{end_col}"

def _get_values_and_index(svc) -> tuple[list[list[str]], Dict[str, int]]:
    """Return (values, header-index-map) for TAB_NAME."""
    values = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=a1_range_for_tab(TAB_NAME)
    ).execute().get("values", [])
    if not values or len(values) < 1:
        return [], {}
    header = values[0]
    idx = {name: i for i, name in enumerate(header)}
    return values, idx

def _get_numeric_sheet_id(svc) -> int:
    global _sheet_id_cache
    if _sheet_id_cache is not None:
        return _sheet_id_cache
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for sh in meta.get("sheets", []):
        if sh["properties"]["title"] == TAB_NAME:
            _sheet_id_cache = sh["properties"]["sheetId"]
            return _sheet_id_cache
    raise HTTPException(500, f"Tab '{TAB_NAME}' not found to resolve sheetId.")

# ---------- models ----------

class AppendRequest(BaseModel):
    location: str
    product: str
    quantity: Optional[int] = None
    note: Optional[str] = None
    user: Optional[str] = None

class QueryFilters(BaseModel):
    location: Optional[str] = None
    product: Optional[str] = None
    since_ts: Optional[float] = Field(None, description="UNIX timestamp lower bound")
    until_ts: Optional[float] = Field(None, description="UNIX timestamp upper bound")
    limit: Optional[int] = Field(50, ge=1, le=500)

class UpdateRowRequest(BaseModel):
    row_number: int = Field(..., ge=2, description="1 = header; data starts at 2")
    location: Optional[str] = None
    product: Optional[str] = None
    quantity: Optional[int] = None
    note: Optional[str] = None
    user: Optional[str] = None
    # If you pass timestamp, it replaces the date string; otherwise left as-is
    timestamp: Optional[str] = Field(None, description="DD-MM-YYYY")

class DeleteRowRequest(BaseModel):
    row_number: int = Field(..., ge=2, description="1 = header; data starts at 2")

# ---------- endpoints ----------

@app.get("/health")
def health():
    return {"ok": True, "tz": TIMEZONE, "date_format": "DD-MM-YYYY"}

@app.post("/append")
def append_row(payload: AppendRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()
    row = {
        "timestamp": _bg_today_str(),
        "location": payload.location.strip(),
        "product": payload.product.strip(),
        "quantity": "" if payload.quantity is None else str(payload.quantity),
        "note": "" if not payload.note else payload.note.strip(),
        "user": "" if not payload.user else payload.user.strip(),
    }
    values = [[row[k] for k in ROW_ORDER]]
    try:
        result = svc.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=a1_range_for_tab(TAB_NAME),
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (append): {e}")
    return {"ok": True, "appended": row, "update": result}

@app.get("/last-product")
def last_product(location: str = Query(...), x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()
    try:
        values, idx = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")

    if not values or len(values) < 2:
        raise HTTPException(404, "No data.")
    for k in ROW_ORDER:
        if k not in idx:
            raise HTTPException(500, f"Sheet header must include: {ROW_ORDER}")

    def ts(v):
        try: return datetime.strptime(v, "%d-%m-%Y").timestamp()
        except: return -1.0

    loc = location.strip().lower()
    rows = values[1:]
    matches = [r for r in rows if len(r) > idx["location"] and r[idx["location"]].strip().lower() == loc]
    if not matches:
        raise HTTPException(404, f"No rows for location '{location}'.")
    last_row = max(matches, key=lambda r: ts(r[idx["timestamp"]]))
    def get(col):
        return last_row[idx[col]] if idx[col] < len(last_row) else ""
    # split comma list into array for convenience
    products_raw = get("product") or ""
    products = [p.strip() for p in products_raw.split(",") if p.strip()]
    return {
        "location": location,
        "timestamp": get("timestamp"),
        "products": products,
        "last_product": (products[-1] if products else ""),
        "quantity": get("quantity"),
        "note": get("note"),
        "user": get("user"),
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
    for k in ROW_ORDER:
        if k not in idx:
            raise HTTPException(500, f"Sheet header must include: {ROW_ORDER}")

    rows = values[1:]

    def ok(r):
        def get(col): return r[idx[col]] if idx[col] < len(r) else ""
        if filters.location and get("location").strip().lower() != filters.location.strip().lower():
            return False
        if filters.product and get("product").strip().lower() != filters.product.strip().lower():
            return False
        if filters.since_ts or filters.until_ts:
            try:
                ts_val = datetime.strptime(get("timestamp"), "%d-%m-%Y").timestamp()
            except:
                return False
            if filters.since_ts and ts_val < filters.since_ts: return False
            if filters.until_ts and ts_val > filters.until_ts: return False
        return True

    out = []
    for i, r in enumerate(rows, start=2):  # row_number in the sheet
        if ok(r):
            obj = {col: (r[idx[col]] if idx[col] < len(r) else "") for col in ROW_ORDER}
            obj["row_number"] = i
            out.append(obj)
            if len(out) >= (filters.limit or 50): break
    return {"rows": out}

@app.post("/update-row")
def update_row(patch: UpdateRowRequest, x_api_key: Optional[str] = Header(None)):
    """
    Update any subset of fields on a specific row_number (>= 2).
    """
    require_api_key(x_api_key)
    svc = sheets_service()

    # Fetch existing row so we can merge partial updates
    try:
        values, idx = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")
    if not values or patch.row_number < 2 or patch.row_number > len(values):
        raise HTTPException(404, f"Row {patch.row_number} not found.")

    existing = values[patch.row_number - 1]  # 0-based in list
    # Build merged row dict
    current: Dict[str, Any] = {k: (existing[idx[k]] if idx[k] < len(existing) else "") for k in ROW_ORDER}
    # Apply patch
    if patch.timestamp is not None: current["timestamp"] = patch.timestamp
    if patch.location  is not None: current["location"]  = patch.location
    if patch.product   is not None: current["product"]   = patch.product
    if patch.quantity  is not None: current["quantity"]  = str(patch.quantity)
    if patch.note      is not None: current["note"]      = patch.note
    if patch.user      is not None: current["user"]      = patch.user

    # Write back to the row
    a1 = f"{TAB_NAME}!A{patch.row_number}:F{patch.row_number}"
    try:
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=a1,
            valueInputOption="USER_ENTERED",
            body={"values": [[current[k] for k in ROW_ORDER]]},
        ).execute()
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (update): {e}")

    return {"ok": True, "row_number": patch.row_number, "row": current}

@app.post("/delete-row")
def delete_row(req: DeleteRowRequest, x_api_key: Optional[str] = Header(None)):
    """
    Delete a specific row_number (>= 2).
    """
    require_api_key(x_api_key)
    svc = sheets_service()

    # Ensure row exists
    try:
        values, _ = _get_values_and_index(svc)
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (get): {e}")
    if not values or req.row_number < 2 or req.row_number > len(values):
        raise HTTPException(404, f"Row {req.row_number} not found.")

    # Delete dimension requires numeric sheetId
    try:
        sheet_id = _get_numeric_sheet_id(svc)
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": req.row_number - 1,  # 0-based
                                "endIndex": req.row_number        # exclusive
                            }
                        }
                    }
                ]
            }
        ).execute()
    except HttpError as e:
        raise HTTPException(502, detail=f"Google API error (delete): {e}")

    return {"ok": True, "deleted_row_number": req.row_number}
