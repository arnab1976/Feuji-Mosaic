@echo off
REM Start MOSAIC locally on Windows
cd /d "%~dp0..\backend"
if not exist .venv python -m venv .venv
call .venv\Scripts\activate
pip install -q -r requirements.txt
echo MOSAIC -> http://localhost:8000  (docs at /docs)
uvicorn app.main:app --reload --port 8000
