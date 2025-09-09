\
    # Activates venv and starts the API
    .\.venv\Scripts\Activate.ps1
    uvicorn main:app --reload --port 8000
