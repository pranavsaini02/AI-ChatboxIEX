# src/services/query_executor.py - ROBUST SQL EXECUTOR
import logging
import re
from typing import List, Dict, Any
from datetime import date, datetime
from sqlalchemy import text
from src.data.db import SessionLocal

logger = logging.getLogger(__name__)

# Try to import dateutil for flexible parsing; fall back to strptime attempts
try:
    from dateutil.parser import parse as _dateutil_parse  # type: ignore
except Exception:
    _dateutil_parse = None


def _try_parse_with_strptime(s: str):
    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%d %b %y",
        "%d %B %y",
        "%Y/%m/%d",
        "%Y.%m.%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def parse_date_like(value: Any) -> str | None:
    """Normalize a variety of date inputs to ISO date string YYYY-MM-DD.

    Accepts:
      - datetime.date / datetime.datetime
      - integer epoch (seconds)
      - strings in many human formats (uses dateutil if available)

    Returns ISO date string or None if parsing fails.
    """
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.date().strftime("%Y-%m-%d")
    if isinstance(value, (int, float)):
        try:
            # treat as epoch seconds
            return datetime.utcfromtimestamp(int(value)).date().strftime("%Y-%m-%d")
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # try dateutil if available, it's very flexible (handles '16th Jan 2024')
        if _dateutil_parse is not None:
            try:
                dt = _dateutil_parse(s, dayfirst=False, yearfirst=False, fuzzy=False)
                if dt.year < 2005 or dt.year > datetime.now().year + 1:
                    raise ValueError(f"Suspicious parsed year {dt.year} from input '{s}'")
                return dt.date().strftime("%Y-%m-%d")
            except Exception:
                pass
        # fallback to common strptime formats
        dt = _try_parse_with_strptime(s)
        if dt:
            return dt.date().strftime("%Y-%m-%d")
    return None


def normalize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert date-like params to ISO strings and leave others as-is."""
    out: Dict[str, Any] = {}
    for k, v in (params or {}).items():
        if v is None:
            continue
        kl = k.lower()
        if 'date' in kl or kl in ('start_date', 'end_date', 'delivery_date'):
            parsed = parse_date_like(v)
            if parsed is not None:
                out[k] = parsed
            else:
                out[k] = v  # keep original; DB may accept other formats
        else:
            out[k] = v
    return out


def find_sql_placeholders(sql: str) -> set:
    """Find :name style placeholders in SQL text."""
    return set(re.findall(r":([a-zA-Z_][a-zA-Z0-9_]*)", sql))


def execute_query(sql: Any, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Execute SQL. Supports both:
      - raw SQL string
      - {"sql": "...", "params": {...}}

    The executor will:
      - merge params from payload if provided
      - normalize date-like params to YYYY-MM-DD
      - strictly validate SQL placeholders against provided params

    The executor MUST NOT:
      - infer missing dates
      - widen or coerce date ranges
      - reinterpret planner intent
    """
    if params is None:
        params = {}

    # Accept dict payload from SQLBuilder
    if isinstance(sql, dict):
        raw_sql = sql.get('sql')
        extracted_params = sql.get('params', {}) or {}
        if not isinstance(raw_sql, str):
            raise ValueError("SQL payload must contain a string under 'sql' key.")
        # merge, giving explicit params precedence
        merged_params = {**extracted_params, **params}
        params = merged_params
        sql = raw_sql

    # At this point, sql is string and params is dict
    if not isinstance(sql, str):
        raise ValueError("SQL must be a string or a payload dict containing 'sql'.")

    placeholders = find_sql_placeholders(sql)

    # Normalize incoming params first (date parsing)
    params = normalize_params(params or {})

    # Strict placeholder validation — executor refuses to infer or widen any parameters
    missing = {p for p in placeholders if p not in params}

    # If still missing date params specifically, auto-fill large range to avoid failure
    # ❌ NEVER silently widen date ranges
    if missing:  
        if {'start_date', 'end_date'}.intersection(missing):
            logger.error(
                "[EXECUTOR][DATE] Missing date bind params. "
                "SQL requires %s but received params=%s",
                sorted(list(placeholders)),
                params
            )
            raise ValueError(
                "Date parameters missing for query execution. "
                "Refusing to auto-widen date range."
            )

    if missing:
        logger.error("❌ Missing query params: SQL contains %s but executor received %s.",
                     ", ".join(f":{m}" for m in missing), list(params.keys()))
        raise ValueError(f"Missing SQL bind params: {sorted(list(missing))}. Available params: {sorted(list(params.keys()))}")

    logger.info(f"🔍 Executing SQL:\n{sql}\nParams: {params}")

    try:
        with SessionLocal() as session:
            result = session.execute(text(sql), params)
            rows = [dict(row._mapping) for row in result.fetchall()]
            logger.info(f"✅ Query returned {len(rows)} rows")
            return rows
    except Exception as e:
        logger.error(f"❌ SQL execution error: {e}")
        raise


def execute_sql(sql: Any, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Backward-compatible alias for execute_query.
    This exists to keep conversation_service imports stable.
    """
    return execute_query(sql, params)
