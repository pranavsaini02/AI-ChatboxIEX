# src/services/schema_catalog.py - COMPLETE WITH DATABASE NAMES
import logging
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from sqlalchemy import text
from src.data.db import SessionLocal
import time
from dateutil import parser as _dateparser
from datetime import datetime
from rapidfuzz import process, fuzz
import re
from decimal import Decimal
from datetime import timedelta


# Identifier sanitization (table/column names)
_SAFE_IDENT_RE = r"^[A-Za-z0-9_]+$"
def safe_ident(s: str):
    if not re.match(_SAFE_IDENT_RE, s or ""):
        raise ValueError(f"Unsafe SQL identifier: {s}")

logger = logging.getLogger(__name__)

CACHE_DIR = Path("cache")

SCHEMA_CATALOG_FILE = CACHE_DIR / "schema_catalog.json"
# In-memory singleton cache for schema catalog (per backend process)
_IN_MEMORY_CATALOG = None


def _is_numeric(col_type: str) -> bool:
    ct = col_type.lower()
    return any(x in ct for x in ("int", "decimal", "double", "float", "numeric", "real"))

def _is_datetime(col_type: str) -> bool:
    ct = col_type.lower()
    return any(x in ct for x in ("date", "time", "timestamp", "datetime", "year"))


# Fetch a few non-null sample values for a column.
def _fetch_sample_values(session, db: str, table: str, col: str, limit: int = 3):
    """Fetch a few non-null sample values for a column."""
    try:
        safe_ident(db)
        safe_ident(table)
        safe_ident(col)

        q = text(f"SELECT `{col}` FROM `{db}`.`{table}` WHERE `{col}` IS NOT NULL LIMIT :limit")
        rows = session.execute(q, {"limit": limit}).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def build_schema_catalog() -> Dict[str, Any]:
    """Build comprehensive database schema catalog."""
    logger.info("🔨 Building schema catalog...")
    catalog = {"version": "1.0", "databases": {}, "generated_at": None}
    from datetime import date, datetime, time as dt_time
    def _jsonify_value(val):
        if isinstance(val, (date, datetime, dt_time)):
            return str(val)
        if isinstance(val, timedelta):
            return str(val)
        if isinstance(val, Decimal):
            return float(val)
        return val
    with SessionLocal() as session:
        dbs = session.execute(text("SHOW DATABASES")).fetchall()
        discovered = [db[0] for db in dbs]
        logger.info("🔎 Discovered databases: %s", discovered)

        SYSTEM_DATABASES = {
            "information_schema",
            "mysql",
            "performance_schema",
            "sys",
            "phpmyadmin",
        }

        db_names = []
        for db in discovered:
            if db.lower() in SYSTEM_DATABASES:
                logger.info("⏭️ Skipping system database: %s", db)
                continue
            db_names.append(db)

        for db_name in db_names:
            logger.info(f"  📂 Parsing: {db_name}")
            catalog["databases"][db_name] = {"tables": {}}

            try:
                tables = session.execute(text("SHOW TABLES FROM `" + db_name + "`")).fetchall()

                for table_tuple in tables:
                    table_name = table_tuple[0]
                    logger.info(f"    📋 Table: {table_name}")
                    try:
                        safe_ident(table_name)
                    except ValueError as e:
                        logger.warning("⚠️ Skipping table with unsafe name %s.%s: %s", db_name, table_name, e)
                        continue
                    cols = session.execute(text("DESCRIBE `" + db_name + "`.`" + table_name + "`")).fetchall()

                    columns = []
                    primary_keys = []
                    column_values = {}

                    for col in cols:
                        col_name = col[0]
                        safe_ident(col_name)
                        # Build column info with special handling for SEGMENT column
                        if col_name.lower() == "segment":
                            try:
                                safe_ident(col_name)
                                distinct_vals = session.execute(
                                    text("SELECT DISTINCT `" + col_name + "` FROM `" + db_name + "`.`" + table_name + "`")
                                ).fetchall()
                                # Ensure all sample values are JSON-serializable
                                sample_vals = [_jsonify_value(v[0]) for v in distinct_vals if v[0] is not None]
                            except Exception:
                                sample_vals = []
                        else:
                            sample_vals = _fetch_sample_values(session, db_name, table_name, col_name, limit=3)
                            # Ensure all sample values are JSON-serializable
                            sample_vals = [_jsonify_value(v) for v in sample_vals if v is not None]

                        col_info = {
                            "name": col_name,
                            "type": col[1],
                            "nullable": col[2] == "YES",
                            "key": col[3],
                            "default": str(col[4]) if col[4] is not None else None,
                            "is_numeric": _is_numeric(col[1]),
                            "is_datetime": _is_datetime(col[1]),
                            "samples": sample_vals
                        }
                        # Collect distinct column values for non-numeric and non-datetime columns
                        if not _is_numeric(col[1]) and not _is_datetime(col[1]):
                            try:
                                safe_ident(col_name)
                                vals = session.execute(
                                    text("SELECT DISTINCT `" + col_name + "` FROM `" + db_name + "`.`" + table_name + "` LIMIT 50")
                                ).fetchall()
                                # Ensure all values are JSON-serializable
                                column_values[col_name] = [_jsonify_value(v[0]) for v in vals if v[0] is not None]
                            except Exception:
                                column_values[col_name] = []
                        columns.append(col_info)
                        if col[3] == "PRI":
                            primary_keys.append(col_name)

                    try:
                        count_query = text("SELECT COUNT(*) FROM `" + db_name + "`.`" + table_name + "`")
                        row_count = session.execute(count_query).scalar()
                    except:
                        row_count = 0

                    catalog["databases"][db_name]["tables"][table_name] = {
                        "columns": columns,
                        "primary_keys": primary_keys,
                        "column_values": column_values,
                        "row_count": row_count,
                        "description": f"{table_name.replace('_', ' ')} data"
                    }

            except Exception as e:
                logger.error(f"Permission or query failed for database {db_name}: {e}")
                continue
    CACHE_DIR.mkdir(exist_ok=True)
    catalog["generated_at"] = int(time.time())
    with open(SCHEMA_CATALOG_FILE, 'w') as f:
        json.dump(catalog, f, indent=2, default=str)

    db_count = len(catalog["databases"])
    table_count = sum(len(db["tables"]) for db in catalog["databases"].values())
    logger.info("📊 Schema summary → databases=%d tables=%d", db_count, table_count)
    logger.info("✅ Schema catalog built!")
    return catalog


def load_schema_catalog(ttl_seconds: int = 0) -> Dict[str, Any]:
    """Load cached schema catalog, building it only once per backend process."""
    global _IN_MEMORY_CATALOG
    logger.info("📦 Loading schema catalog (ttl=%s)", ttl_seconds)
    # If catalog is already loaded in memory, return it
    if _IN_MEMORY_CATALOG is not None:
        return _IN_MEMORY_CATALOG

    if SCHEMA_CATALOG_FILE.exists():
        with open(SCHEMA_CATALOG_FILE, 'r') as f:
            c = json.load(f)
            generated_at = c.get("generated_at", 0)
            # ttl_seconds == 0 → ALWAYS rebuild, but only once per process
            if ttl_seconds == 0:
                # Build once per backend process
                _IN_MEMORY_CATALOG = build_schema_catalog()
                # Write to disk is handled in build_schema_catalog
                return _IN_MEMORY_CATALOG

            # ttl > 0 → rebuild only if expired
            if time.time() - generated_at > ttl_seconds:
                _IN_MEMORY_CATALOG = build_schema_catalog()
                return _IN_MEMORY_CATALOG
            # Load from disk cache, and assign to in-memory cache
            _IN_MEMORY_CATALOG = c
            return _IN_MEMORY_CATALOG
    # No disk cache; build and cache in memory
    _IN_MEMORY_CATALOG = build_schema_catalog()
    return _IN_MEMORY_CATALOG


def get_latest_sample_datetime(catalog: Dict[str, Any], db: str, tbl: str):
    """Return the latest datetime parsed from sample values for datetime/date-like columns in the schema catalog.

    Scans columns for `is_datetime==True` or column type containing 'date' and parses sample values
    using dateutil.parser. Returns a `datetime` object (max) or None if no parsable samples are found.
    """
    try:
        dbs = catalog.get("databases", {})
        tmeta = dbs.get(db, {}).get("tables", {}).get(tbl)
        if not tmeta:
            return None
        max_dt = None
        for col in tmeta.get("columns", []):
            col_type = (col.get("type") or "").lower()
            if col.get("is_datetime") or "date" in col_type or "time" in col_type:
                samples = col.get("samples") or []
                for s in samples:
                    try:
                        dt = _dateparser.parse(str(s))
                    except Exception:
                        continue
                    if max_dt is None or dt > max_dt:
                        max_dt = dt
        return max_dt
    except Exception:
        return None

def get_date_range_from_samples(catalog: Dict[str, Any], db: str, tbl: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return (min_dt, max_dt) based on sample values in columns marked as datetime or
    whose type contains 'date'/'time'. If no eligible samples exist, returns (None, None).
    """
    try:
        dbs = catalog.get("databases", {})
        tmeta = dbs.get(db, {}).get("tables", {}).get(tbl)
        if not tmeta:
            return (None, None)

        parsed_dates = []
        for col in tmeta.get("columns", []):
            col_type = (col.get("type") or "").lower()
            if col.get("is_datetime") or "date" in col_type or "time" in col_type:
                samples = col.get("samples") or []
                for s in samples:
                    if s is None:
                        continue
                    try:
                        dt = _dateparser.parse(str(s))
                    except Exception:
                        continue
                    parsed_dates.append(dt)

        if not parsed_dates:
            return (None, None)

        return (min(parsed_dates), max(parsed_dates))
    except Exception:
        return (None, None)


def get_relevant_schema_context(query: str, max_tables: int = 2) -> str:
    """Get schema context with full DATABASE.TABLE names."""
    catalog = load_schema_catalog()
    logger.debug("Databases and Tables Available:")
    for db, db_info in catalog["databases"].items():
      logger.debug(f"Database: {db}")
      for table in db_info["tables"]:
        logger.debug(f" - Table: {table}")
    query_lower = query.lower()
    
    relevant_tables = []
    
    for db_name, db_data in catalog["databases"].items():
        for table_name, table_data in db_data["tables"].items():
            if table_data.get("row_count", 0) == 0:
                continue
                
            score = 0
            
            for word in query_lower.split():
                if len(word) > 2:
                    if word in table_name.lower():
                        score += 5
                    if word in table_data.get("description", "").lower():
                        score += 2
                    for col in table_data.get("columns", []):
                        if word in col["name"].lower():
                            score += 3
            
            if score > 0:
                relevant_tables.append({
                    "score": score,
                    "db": db_name,
                    "table": table_name,
                    "data": table_data
                })
    
    relevant_tables.sort(key=lambda x: x["score"], reverse=True)
    relevant_tables = relevant_tables[:max_tables]
    
    if not relevant_tables:
        return "No relevant tables found."
    
    context_lines = ["=== DATABASE SCHEMA ===\n"]
    
    for item in relevant_tables:
        db, table, data = item["db"], item["table"], item["data"]
        full_name = f"{db}.{table}"
        
        context_lines.append(f"\n📊 Table: {full_name}")
        context_lines.append(f"   Rows: {data['row_count']:,}")
        context_lines.append(f"   Description: {data.get('description', 'N/A')}")
        context_lines.append("   Columns:")
        
        for col in data["columns"][:15]:
            pk = " [PK]" if col["name"] in data.get("primary_keys", []) else ""
            context_lines.append(f"     - {col['name']}: {col['type']}{pk}")
    
    context_lines.append("\n=== END SCHEMA ===")
    
    return "\n".join(context_lines)


def get_example_queries() -> List[Dict[str, str]]:
    """Concrete examples with exact table names."""
    return [
        {
            "question": "Show India demand for last 60 days",
            "sql": "SELECT DATE(timestamp_recorded) as date, AVG(demand_met) as avg_demand FROM IEX_StatePowerGeneration.all_india_power_position WHERE timestamp_recorded >= DATE_SUB(NOW(), INTERVAL 60 DAY) GROUP BY DATE(timestamp_recorded) ORDER BY date;",
            "explanation": "Real table: IEX_StatePowerGeneration.all_india_power_position, columns: timestamp_recorded, demand_met"
        },
        {
            "question": "Show demand by time blocks",
            "sql": "SELECT (HOUR(timestamp_recorded) * 4 + FLOOR(MINUTE(timestamp_recorded) / 15) + 1) AS time_block, AVG(demand_met) as avg_demand FROM IEX_StatePowerGeneration.all_india_power_position WHERE DATE(timestamp_recorded) = CURDATE() GROUP BY time_block ORDER BY time_block;",
            "explanation": "Time blocks 1-96 from timestamp_recorded column"
        },
        {
            "question": "Show India demand in time blocks over 60 days",
            "sql": "SELECT DATE(timestamp_recorded) as date, (HOUR(timestamp_recorded) * 4 + FLOOR(MINUTE(timestamp_recorded) / 15) + 1) AS time_block, AVG(demand_met) as avg_demand FROM IEX_StatePowerGeneration.all_india_power_position WHERE timestamp_recorded >= DATE_SUB(NOW(), INTERVAL 60 DAY) GROUP BY date, time_block ORDER BY date, time_block;",
            "explanation": "Combines date grouping with time block calculation"
        },
        {
            "question": "Current India demand",
            "sql": "SELECT demand_met FROM IEX_StatePowerGeneration.all_india_power_position ORDER BY timestamp_recorded DESC LIMIT 1;",
            "explanation": "Latest reading only"
        }
    ]

def get_table_for_query(query: str) -> str:
    """Map query keywords to actual tables."""
    query_lower = query.lower()
    
    # RTM/DAM queries
    if 'rtm' in query_lower or 'real time' in query_lower or 'real-time' in query_lower:
        if 'mcv' in query_lower or 'volume' in query_lower:
            return "IEXInternetData.rtm_market_clearing"  # ← YOUR ACTUAL TABLE NAME
        if 'mcp' in query_lower or 'price' in query_lower:
            return "IEXInternetData.rtm_market_pricing"  # ← YOUR ACTUAL TABLE NAME
    
    if 'dam' in query_lower or 'day ahead' in query_lower:
        if 'mcv' in query_lower or 'volume' in query_lower:
            return "IEXInternetData.dam_market_clearing"  # ← YOUR ACTUAL TABLE NAME
    
    # India demand queries
    if 'india' in query_lower and 'demand' in query_lower:
        return "IEX_StatePowerGeneration.all_india_power_position"
    
    return None


def build_llm_prompt_with_schema(user_query: str) -> str:
    """Force correct table selection."""
    
    # Try to get exact table match
    forced_table = get_table_for_query(user_query)
    
    if forced_table:
        # Build focused prompt with ONLY this table
        catalog = load_schema_catalog()
        db, table = forced_table.split('.')
        table_schema = catalog["databases"][db]["tables"][table]
        
        cols = ", ".join([col["name"] for col in table_schema["columns"][:10]])
        
        prompt = f"""Generate MySQL query for: {user_query}

Use this table: {forced_table}
Available columns: {cols}

Rules:
- Use format: {forced_table}
- For RTM: WHERE segment = 'RTM'
- For DAM: WHERE segment = 'DAM'

Generate SQL:"""
        return prompt
    
    # Otherwise fall back to schema context
    schema_context = get_relevant_schema_context(user_query)
    prompt = f"""Generate MySQL query for: {user_query}

{schema_context}

Generate SQL:"""
    return prompt

def fuzzy_find_tables(catalog, query: str, limit: int = 5):
    choices = []
    for db, db_data in catalog["databases"].items():
        for tname in db_data["tables"]:
            choices.append(f"{db}.{tname}")
    return process.extract(query, choices, scorer=fuzz.partial_ratio, limit=limit)
