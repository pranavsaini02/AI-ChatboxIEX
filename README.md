# AI-ChatboxIEX

Local chatbox for Text-to-SQL over IEX/Merit (and extensible sources), with planner/executor roles and chart rendering.

## Quick Start
1) Create and fill backend/.env with DB DSNs and model endpoints.
2) python -m venv .venv && source .venv/bin/activate
3) pip install -r backend/requirements.txt
4) uvicorn src.main:app --reload --host 127.0.0.1 --port 8000 (from backend/)
5) Frontend setup (see frontend-react/README.md)

All services run on localhost by default to keep data private.
