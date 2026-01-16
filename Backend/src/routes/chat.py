from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Any, Dict, List
from datetime import datetime, date
import logging
import uuid
import os
import pandas as pd
import re

from src.services.conversation_service import conversation_service
from src.services.query_executor import execute_query

router = APIRouter()
logger = logging.getLogger(__name__)

# Constants
UPLOAD_DIR = "uploads"
EXPORT_DIR = "exports"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)


class ChatRequest(BaseModel):
    prompt: str
    session_id: str
    forced_table: str | None = None
    forced_metric: str | None = None


class ExportRequest(BaseModel):
    session_id: str
    format: str = "csv"


def serialize_results(results: List[Dict]) -> List[Dict]:
    """Convert datetime to JSON-serializable types."""
    serialized = []
    for row in results:
        new_row = {}
        for key, value in row.items():
            if isinstance(value, (datetime, date)):
                new_row[key] = str(value)
            else:
                new_row[key] = value
        serialized.append(new_row)
    return serialized


# Gemini-native /chat route
@router.post("/chat")
async def chat(request: ChatRequest):
    try:
        logger.info("=" * 50)
        logger.info("NEW CHAT REQUEST")
        logger.info("Prompt: %s", request.prompt)
        logger.info("Session: %s", request.session_id)
        logger.info("=" * 50)

        forced_table = request.forced_table
        forced_metric = request.forced_metric

        response = await conversation_service.process_query_with_context(
            session_id=request.session_id,
            query=request.prompt,
            forced_table=forced_table,
            forced_metric=forced_metric,
        )

        logger.info(
            "FINAL HTTP RESPONSE → rows=%s columns=%s",
            len(response.get("rows", [])),
            response.get("columns"),
        )

        return response

    except Exception as e:
        logger.exception("❌ Chat route failed")
        raise HTTPException(status_code=500, detail=str(e))
        


@router.get("/history/{session_id}")
async def get_history(session_id: str):
    """Get conversation history."""
    history = conversation_service.get_or_create_session(session_id)
    return {
        "session_id": session_id,
        "history": history,
        "message_count": len(history)
    }


@router.post("/clear_session/{session_id}")
async def clear_session(session_id: str):
    """Clear conversation history."""
    conversation_service.clear_session(session_id)
    return {"message": "Session cleared", "session_id": session_id}


@router.get("/stats")
async def get_stats():
    """Get learning statistics."""
    stats = conversation_service.get_learning_stats()
    return {"learning_stats": stats, "message": "System learning metrics"}


@router.post("/upload")
async def upload_file(file: UploadFile = File(...), session_id: str = None):
    """Upload CSV/Excel for analysis."""
    if not session_id:
        session_id = str(uuid.uuid4())
    
    file_path = os.path.join(UPLOAD_DIR, f"{session_id}_{file.filename}")
    
    with open(file_path, "wb") as f:
        f.write(await file.read())
    
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path)
        else:
            return {"error": "Unsupported format. Use CSV or Excel."}
        
        return {
            "session_id": session_id,
            "filename": file.filename,
            "rows": len(df),
            "columns": list(df.columns),
            "preview": df.head(5).to_dict(orient="records"),
            "message": f"✅ Uploaded {file.filename} ({len(df):,} rows)"
        }
    except Exception as e:
        return {"error": f"Failed: {str(e)}"}


@router.post("/export")
async def export_data(request: ExportRequest):
    """Export conversation results."""
    history = conversation_service.get_or_create_session(request.session_id)
    
    data = []
    for msg in history:
        if msg.get("role") == "assistant":
            data.append({
                "timestamp": msg.get("timestamp"),
                "content": msg.get("content"),
                "table": msg.get("table_used"),
                "sql": msg.get("sql_query")
            })
    
    if not data:
        raise HTTPException(status_code=404, detail="No data to export")
    
    df = pd.DataFrame(data)
    filename = f"export_{request.session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if request.format == "csv":
        filepath = os.path.join(EXPORT_DIR, f"{filename}.csv")
        df.to_csv(filepath, index=False)
    elif request.format == "excel":
        filepath = os.path.join(EXPORT_DIR, f"{filename}.xlsx")
        df.to_excel(filepath, index=False)
    elif request.format == "json":
        filepath = os.path.join(EXPORT_DIR, f"{filename}.json")
        df.to_json(filepath, orient="records", indent=2)
    else:
        raise HTTPException(status_code=400, detail="Invalid format")
    
    return FileResponse(
        path=filepath,
        filename=os.path.basename(filepath),
        media_type="application/octet-stream"
    )

@router.get("/metric_candidates")
async def metric_candidates(table: str, prompt: str):
    """
    Return metric candidates + scores for UI override dropdown.
    """
    try:
        candidates = conversation_service.get_metric_candidates(table, prompt)
        return {"table": table, "prompt": prompt, "candidates": candidates}
    except Exception as e:
        logger.error(f"❌ Metric candidate error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
