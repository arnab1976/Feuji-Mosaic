#!/usr/bin/env bash
# Start MOSAIC locally (single-process reference app).
set -e
cd "$(dirname "$0")/../backend"
if [ ! -d .venv ]; then python3 -m venv .venv; fi
source .venv/bin/activate
pip install -q -r requirements.txt
echo "MOSAIC → http://localhost:8000   (docs at /docs)"
uvicorn app.main:app --reload --port 8000
