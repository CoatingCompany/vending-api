\
    # Creates .venv and installs requirements
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    pip install -r requirements.txt
    Write-Host "Setup complete. Create .env from .env.example, then run scripts\run.ps1"
