import logging
from pathlib import Path
from datetime import datetime

def setup_global_logging():
    root = logging.getLogger()

    # ✅ Prevent duplicate handlers
    if getattr(root, "_chatbox_logging_configured", False):
        return

    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler("logs/chatbox.log")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # 🔒 Mark as configured
    root._chatbox_logging_configured = True