# src/main.py (CLEANED)
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.routes import chat
import logging
import subprocess
import time
import atexit
import httpx
import logging
from src.utils.logging_config import setup_global_logging

setup_global_logging()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# Global file logging (same output as terminal)
# -------------------------------------------------
from pathlib import Path
from datetime import datetime

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"chatbox_{datetime.utcnow().strftime('%Y%m%d')}.log"

file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
)

logging.getLogger().addHandler(file_handler)

app = FastAPI(title="Energy Data API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(chat.router, tags=["chat"])

# Ollama process
ollama_process = None

def start_ollama():
    """Start Ollama server if not already running."""
    global ollama_process
    try:
        response = httpx.get("http://127.0.0.1:11434/api/tags", timeout=2.0)
        if response.status_code == 200:
            logger.info("[startup] ✓ Ollama already running at 127.0.0.1:11434")
            return
    except:
        pass
    
    try:
        logger.info("[startup] Starting Ollama server...")
        ollama_process = subprocess.Popen(
            ["ollama", "serve"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )
        
        max_retries = 10
        for i in range(max_retries):
            try:
                response = httpx.get("http://127.0.0.1:11434/api/tags", timeout=2.0)
                if response.status_code == 200:
                    logger.info(f"[startup] ✓ Ollama started successfully at 127.0.0.1:11434")
                    return
            except:
                time.sleep(1)
                logger.info(f"[startup] Waiting for Ollama to start... ({i+1}/{max_retries})")
        
        logger.warning("[startup] ⚠ Ollama started but not responding yet")
        
    except FileNotFoundError:
        logger.error("[startup] ✗ Ollama not found. Install with: brew install ollama")
    except Exception as e:
        logger.error(f"[startup] ✗ Failed to start Ollama: {e}")

def stop_ollama():
    """Stop Ollama server on shutdown."""
    global ollama_process
    if ollama_process:
        logger.info("[shutdown] Stopping Ollama...")
        ollama_process.terminate()
        try:
            ollama_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            ollama_process.kill()
        logger.info("[shutdown] Ollama stopped")

@app.on_event("startup")
async def startup_event():
    start_ollama()
    
    # Build schema catalog on startup
    from src.services.schema_catalog import load_schema_catalog
    try:
        catalog = load_schema_catalog()
        databases = catalog.get("databases", {})

        logger.info("[startup] 📊 Schema catalog summary:")
        logger.info("[startup] ├─ Databases loaded: %d", len(databases))

        for db_name, db_info in databases.items():
            tables = db_info.get("tables", {})
            logger.info(
            "[startup] │  ├─ %s → %d tables",
            db_name,
            len(tables),
        )

        logger.info("[startup] ✅ Schema catalog fully initialized")
    except Exception:
        logger.exception("[startup] ❌ Failed to load schema catalog")

@app.on_event("shutdown")
async def shutdown_event():
    stop_ollama()

atexit.register(stop_ollama)

@app.get("/")
async def root():
    return {
        "message": "Energy Data API",
        "version": "1.0.0",
        "endpoints": {
            "chat": "/chat",
            "history": "/history/{session_id}",
            "clear_session": "/clear_session/{session_id}",
            "stats": "/stats"
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "ollama": "running"}

# OPTIONAL: Admin endpoint to rebuild schema
@app.post("/admin/rebuild-schema")
async def rebuild_schema():
    """Manually rebuild schema catalog."""
    try:
        from src.services.schema_catalog import build_schema_catalog
        build_schema_catalog()
        return {"status": "success", "message": "Schema catalog rebuilt"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
