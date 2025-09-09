# Wonder Toys Sheets API (Windows-friendly)

## Quick Start (Windows / VS Code)
1) Open this folder in VS Code.
2) Copy `.env.example` to `.env` and fill:
   - SERVICE_ACCOUNT_JSON = full JSON (one line)
   - SHEET_ID = your spreadsheet id
   - TAB_NAME = Data (or your tab)
   - API_KEY = a strong key
3) In the VS Code terminal run:
   ```powershell
   scripts\setup.ps1
   ```
4) Start the server:
   ```powershell
   scripts\run.ps1
   ```
5) Test requests:
   ```powershell
   scripts\test-requests.ps1
   ```

## Deploy
- Deploy this folder to Render/Fly.io/Cloud Run.
- Set the same env vars there.
- Put your public base URL into `openapi.json` and import as a GPT Action.
