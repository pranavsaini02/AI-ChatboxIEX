# src/utils/query_builder.py
from typing import Dict, Any, Tuple, List
from src.data.db import get_columns, run_sql

# Candidate columns by metric
CANDIDATES = {
    "demand": ["demand_met","demand","total_demand","value","mw","Demand","Demand_MW"],
    "price":  ["mcp","price","dam_price","rtm_price","value"],
    "generation": ["generation","gen_mw","mw","value"],
    "power_position": ["power_mw","mw","value"],
    "outages": ["outage_count","count","value"]
}

def choose_time_col(db: str, table: str) -> str:
    cols = get_columns(db, table)
    # prefer datetime/timestamp/date
    for c in cols:
        if c["DATA_TYPE"] in ("datetime","timestamp","date"):
            return c["COLUMN_NAME"]
    # heuristic names
    for c in cols:
        n = c["COLUMN_NAME"].lower()
        if "time" in n or "date" in n:
            return c["COLUMN_NAME"]
    # fallback: first column
    return cols[0]["COLUMN_NAME"]

def choose_value_col(db: str, table: str, metric: str) -> str:
    cols = get_columns(db, table)
    names = [c["COLUMN_NAME"] for c in cols]
    for cand in CANDIDATES.get(metric, []) + ["value"]:
        if cand in names:
            return cand
    # fallback: first numeric
    for c in cols:
        if c["DATA_TYPE"] in ("int","bigint","decimal","double","float","numeric"):
            return c["COLUMN_NAME"]
    return names[0]

def table_exists(db: str, table: str) -> bool:
    key = f"Tables_in_{db}"
    rows = run_sql(f"SHOW TABLES FROM `{db}`")
    tables = [r[key] if key in r else list(r.values())[0] for r in rows]
    return table in tables

def build_sql_from_plan(plan: Dict[str, Any]) -> Tuple[str, str]:
    db = plan.get("db", "IEXTesting")
    table_hint = plan.get("table", "damData")
    metric = plan.get("metric", "demand")
    limit = int(plan.get("limit", 200))
    where = plan.get("where", "1=1")

    # resolve table if hint missing
    table = table_hint
    if not table_exists(db, table_hint):
        for cand in ["damData","rtmData","state_power_supply","all_india_power_position"]:
            if table_exists(db, cand):
                table = cand
                break

    # pick columns (respect explicit plan.columns if present)
    cols: List[str] = plan.get("columns") or []
    if len(cols) >= 2:
        tcol, vcol = cols[0], cols[1]
    else:
        tcol = choose_time_col(db, table)
        vcol = choose_value_col(db, table, metric)

    full_table = f"`{db}`.`{table}`"
    sql = f"SELECT {tcol} AS timestamp, {vcol} AS value FROM {full_table} WHERE {where} ORDER BY {tcol} DESC LIMIT {limit};"
    return sql, db
