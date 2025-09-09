import os, json
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Load .env if present
load_dotenv()

SHEET_ID = os.environ.get("SHEET_ID")
TAB_NAME = os.environ.get("TAB_NAME", "Data")
API_KEY  = os.environ.get("API_KEY")
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]

# Timezone & date format (Bulgarian)
TZ_NAME = os.environ.get("TIMEZONE", "Europe/Sofia")  # allow override via .env
BG_TZ = ZoneInfo(TZ_NAME)
BG_DATE_FMT = "%d-%m-%Y"  # DD-MM-YYYY

ROW_ORDER = ["timestamp", "location", "product", "quantity", "note", "user"]

app = FastAPI(title="Wonder Toys Sheets API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

def sheets_service():
    svc_file = os.environ.get("SERVICE_ACCOUNT_FILE")
    svc_json = os.environ.get("SERVICE_ACCOUNT_JSON")
    if svc_file:
        svc_file = os.path.abspath(svc_file)
        if not os.path.exists(svc_file):
            raise RuntimeError(f"SERVICE_ACCOUNT_FILE not found at: {svc_file}")
        creds = service_account.Credentials.from_service_account_file(svc_file, scopes=SCOPES)
    elif svc_json:
        info = json.loads(svc_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        raise RuntimeError("Provide SERVICE_ACCOUNT_FILE (preferred) or SERVICE_ACCOUNT_JSON in the environment.")
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def require_api_key(x_api_key: Optional[str]):
    if not API_KEY:
        raise HTTPException(500, "Server missing API_KEY.")
    if x_api_key != API_KEY:
        raise HTTPException(401, "Invalid API key.")

def a1_range_for_tab(tab: str, start_col="A", end_col="F"):
    return f"{tab}!{start_col}:{end_col}"

# ---------- Date handling (Bulgarian) ----------
def today_bg_date_str() -> str:
    # Date in Europe/Sofia, no time: DD-MM-YYYY
    return datetime.now(BG_TZ).date().strftime(BG_DATE_FMT)

def parse_timestamp_any(s: str) -> float:
    """
    Convert to UNIX seconds for sorting. Accepts:
      - Bulgarian date 'DD-MM-YYYY' (Europe/Sofia at 00:00)
      - ISO datetime (e.g., '2025-09-10T12:34:56Z' or with timezone)
      - UNIX seconds string
      - Leading apostrophe is ignored (Sheets 'text' hint)
    """
    if not s:
        return -1.0
    s = s.strip().lstrip("'")
    # Try UNIX seconds
    try:
        return float(s)
    except ValueError:
        pass
    # Try Bulgarian date
    try:
        d = datetime.strptime(s, BG_DATE_FMT).date()
        dt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=BG_TZ)
        return dt.timestamp()
    except Exception:
        pass
    # Try ISO 8601
    try:
        iso = s
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return -1.0

def split_products(cell: str) -> List[str]:
    if not cell:
        return []
    parts = [p.strip() for p in cell.split(",")]
    return [p for p in parts if p]

# ---------- Models ----------
class AppendRequest(BaseModel):
    location: str
    product: str = Field(..., description="Comma-separated list of products for this visit (e.g., 'A, B, C').")
    quantity: Optional[int] = Field(None, description="Optional total quantity for the visit.")
    note: Optional[str] = None
    user: Optional[str] = None

class QueryFilters(BaseModel):
    location: Optional[str] = None
    product: Optional[str] = None
    since_ts: Optional[float] = Field(None, description="UNIX timestamp lower bound")
    until_ts: Optional[float] = Field(None, description="UNIX timestamp upper bound")
    limit: Optional[int] = Field(50, ge=1, le=500)

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True, "tz": TZ_NAME, "date_format": BG_DATE_FMT}

@app.post("/append")
def append_row(payload: AppendRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()

    row = {
        "timestamp": today_bg_date_str(),              # DD-MM-YYYY in Europe/Sofia
        "location": payload.location.strip(),
        "product": payload.product.strip(),            # allow "A, B, C"
        "quantity": "" if payload.quantity is None else str(payload.quantity),
        "note": "" if not payload.note else payload.note.strip(),
        "user": "" if not payload.user else payload.user.strip(),
    }
    values = [[row[k] for k in ROW_ORDER]]

    result = svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=a1_range_for_tab(TAB_NAME),
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

    return {"ok": True, "appended": row, "update": result}

@app.get("/last-product")
def last_product(location: str = Query(...), x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()
    values = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=a1_range_for_tab(TAB_NAME)
    ).execute().get("values", [])

    if not values or len(values) < 2:
        raise HTTPException(404, "No data.")
    header, rows = values[0], values[1:]
    idx = {name: i for i, name in enumerate(header)}
    for k in ROW_ORDER:
        if k not in idx:
            raise HTTPException(500, f"Sheet header must include: {ROW_ORDER}")

    def get_val(r, col):
        return r[idx[col]] if idx[col] < len(r) else ""

    loc_norm = location.strip().lower()
    candidates = [r for r in rows if get_val(r, "location").strip().lower() == loc_norm]
    if not candidates:
        raise HTTPException(404, f"No rows for location '{location}'.")

    latest = max(candidates, key=lambda r: parse_timestamp_any(get_val(r, "timestamp")))

    product_cell = get_val(latest, "product")
    products = split_products(product_cell)
    last = products[-1] if products else ""

    return {
        "location": get_val(latest, "location"),
        "timestamp": get_val(latest, "timestamp"),  # DD-MM-YYYY
        "products": products,
        "last_product": last,
        "quantity": get_val(latest, "quantity"),
        "note": get_val(latest, "note"),
        "user": get_val(latest, "user"),
    }

@app.post("/search")
def search_rows(filters: QueryFilters, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    svc = sheets_service()
    values = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=a1_range_for_tab(TAB_NAME)
    ).execute().get("values", [])

    if not values or len(values) < 2:
        return {"rows": []}

    header, rows = values[0], values[1:]
    idx = {name: i for i, name in enumerate(header)}

    def get(r, col):
        return r[idx[col]] if idx[col] < len(r) else ""

    out = []
    for r in rows:
        if filters.location and get(r, "location").strip().lower() != filters.location.strip().lower():
            continue
        if filters.product:
            tokens = split_products(get(r, "product"))
            if filters.product.strip().lower() not in [t.lower() for t in tokens]:
                continue
        if filters.since_ts or filters.until_ts:
            ts_val = parse_timestamp_any(get(r, "timestamp"))
            if ts_val < 0:
                continue
            if filters.since_ts and ts_val < filters.since_ts:
                continue
            if filters.until_ts and ts_val > filters.until_ts:
                continue

        out.append({col: get(r, col) for col in ROW_ORDER})
        if len(out) >= (filters.limit or 50):
            break

    return {"rows": out}
