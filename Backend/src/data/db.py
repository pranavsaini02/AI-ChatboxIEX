import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

engine_str = os.getenv("DB_ENGINE", "mysql+pymysql")
host = os.getenv("DB_HOST", "localhost")
port_str = os.getenv("DB_PORT", "3307")
user = os.getenv("DB_USER", "root")
passw = os.getenv("DB_PASS", "")

DB_URL = f"{engine_str}://{user}:{passw}@{host}:{port_str}/"
print(f"Debug DB_URL: {DB_URL}")

engine = create_engine(DB_URL, pool_pre_ping=True, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def run_sql(sql: str, db_name: Optional[str] = None) -> List[Dict[str, Any]]:
    try:
        with SessionLocal() as session:
            if db_name:
                session.execute(text(f"USE `{db_name}`"))
            result = session.execute(text(sql))
            return [dict(row._mapping) for row in result.fetchall()]
    except Exception as e:
        print(f"DB query error: {e}")
        return []

def list_databases() -> List[str]:
    rows = run_sql("SHOW DATABASES")
    return [r["Database"] for r in rows if r["Database"] not in ["information_schema","mysql","performance_schema","sys"]]

def list_tables(db: str) -> List[str]:
    rows = run_sql(f"SHOW TABLES FROM `{db}`")
    key = f"Tables_in_{db}"
    return [r[key] for r in rows]

def table_exists(db: str, table: str) -> bool:
    return table in list_tables(db)

def get_columns(db: str, table: str) -> List[Dict[str, Any]]:
    return run_sql(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
    """)

def first_datetime_col(cols: List[Dict[str, Any]]) -> Optional[str]:
    for c in cols:
        if c["DATA_TYPE"] in ["datetime","timestamp","date"]:
            return c["COLUMN_NAME"]
    return None

def first_numeric_col(cols: List[Dict[str, Any]]) -> Optional[str]:
    for c in cols:
        if c["DATA_TYPE"] in ["int","bigint","decimal","double","float","numeric"]:
            return c["COLUMN_NAME"]
    return None
