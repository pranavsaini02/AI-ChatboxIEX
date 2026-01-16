#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/../backend"
python3 -m venv .venv || true
source .venv/bin/activate
pip install -r requirements.txt
uvicorn src.main:app --reload --host 127.0.0.1 --port 8000 &
BACK_PID=$!
cd ../frontend
npm install
npm run dev &
FRONT_PID=$!
echo "Backend PID: $BACK_PID"
echo "Frontend PID: $FRONT_PID"
echo "Open http://127.0.0.1:3000"
wait
