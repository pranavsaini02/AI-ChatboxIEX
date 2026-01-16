# src/services/conversation_service.py

import logging
import uuid
from datetime import datetime
from typing import Dict, List
from decimal import Decimal

from src.services.schema_catalog import load_schema_catalog
from src.services.gemini_sql_generator import generate_sql, generate_narrative_from_insight
from src.services.query_executor import execute_sql

logger = logging.getLogger(__name__)

# =========================================================
# Helper functions for JSON-safe serialization
# =========================================================

def _json_safe(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value

def normalize_rows_for_json(rows: list[dict]) -> list[dict]:
    safe_rows = []
    for row in rows:
        safe_row = {}
        for k, v in row.items():
            safe_row[k] = _json_safe(v)
        safe_rows.append(safe_row)
    return safe_rows

def build_chart_rows(rows: list[dict], columns: list[str]) -> list[list]:
    """
    Convert list[dict] rows into list[list] aligned with column order,
    which is required by frontend chart renderers.
    """
    chart_rows = []
    for r in rows:
        chart_rows.append([r.get(c) for c in columns])
    return chart_rows

def classify_error(error: str | None, planner_output: dict | None = None) -> str:
    if not error:
        return ""
    err = error.lower()
    if "quota" in err or "rate limit" in err or "free tier" in err:
        return "⚠️ Model rate limit reached (free tier). Please try again later."
    if "sql" in err or "syntax" in err or "execution" in err:
        return f"❌ SQL execution failed: {error}"
    if planner_output and not planner_output.get("queries"):
        return "❌ Query planning failed. The model could not generate a valid query."
    return f"❌ Unexpected error occurred: {error}"

# =========================================================
# Narrative helpers
# =========================================================

def split_narrative_into_blocks(narrative: str) -> list[dict]:
    """
    Split narrative text into paragraph-level blocks.
    Safe fallback: single block.
    """
    if not narrative:
        return []

    parts = [p.strip() for p in narrative.split("\n\n") if p.strip()]
    blocks = []

    for idx, part in enumerate(parts):
        blocks.append({
            "id": f"block_{idx}",
            "title": None,
            "text": part,
            "chart_refs": []
        })

    return blocks


# =========================================================
# Core Conversation Service (FINAL FORM)
# =========================================================

def normalize_key(key: str) -> str:
    return key.lower()

class ConversationService:
    def __init__(self):
        self.sessions: Dict[str, List[Dict]] = {}

    # ---------------- Session Helpers ----------------

    def get_or_create_session(self, session_id: str) -> List[Dict]:
        self.sessions.setdefault(session_id, [])
        return self.sessions[session_id]

    def add_to_conversation(
        self,
        session_id: str,
        user_query: str,
        assistant_payload: Dict,
    ):
        history = self.get_or_create_session(session_id)
        ts = datetime.now().isoformat()

        history.append({
            "role": "user",
            "content": user_query,
            "timestamp": ts,
        })

        history.append({
            "role": "assistant",
            "assistant": assistant_payload,
            "timestamp": ts,
        })

        if len(history) > 200:
            self.sessions[session_id] = history[-200:]

    # =========================================================
    # Main Entry Point
    # =========================================================

    async def process_query_with_context(
        self,
        session_id: str,
        query: str,
        forced_table: str | None = None,
        forced_metric: str | None = None,
    ):
        """
        Orchestrates a single user query.

        Principles:
        - Gemini output is treated as authoritative and immutable
        - Backend ONLY validates, executes, enriches, and persists
        - No inference, guessing, or mutation of model intent
        """

        request_id = str(uuid.uuid4())
        logger.info("[REQ %s] Processing query: %s", request_id, query)

        # Ensure variables always exist (Python safety)
        narrative_blocks: list = []
        final_charts: list = []
        narrative_charts: list = []

        # --------------------------------------------------
        # Load schema (read-only context)
        # --------------------------------------------------
        schema_catalog = load_schema_catalog()

        # --------------------------------------------------
        # Gemini planning step (authoritative)
        # --------------------------------------------------
        gemini_payload = generate_sql(
            query=query,
            schema_catalog=schema_catalog,
            forced_table=forced_table,
            forced_metric=forced_metric,
        )

        # --------------------------------------------------
        # Clarification short-circuit (STRUCTURED, UI-ENFORCED)
        # --------------------------------------------------
        if gemini_payload.get("needs_clarification") or gemini_payload.get("needs_metric_clarification"):
            buttons = (
                gemini_payload.get("options")
                or gemini_payload.get("buttons")
                or gemini_payload.get("ui", {}).get("buttons")
                or []
            )
            clarification_payload = {
                "success": False,
                "needs_clarification": bool(gemini_payload.get("needs_clarification")),
                "needs_metric_clarification": bool(gemini_payload.get("needs_metric_clarification")),
                "message": gemini_payload.get(
                    "message",
                    "Multiple options match your request. Please select one."
                ),
                "ui": {
                    "buttons": buttons
                },
                "rows": [],
                "columns": [],
                "planner": gemini_payload,
            }

            logger.info(
                "[REQ %s] Clarification required → %d options",
                request_id,
                len(buttons),
            )

            self.add_to_conversation(
                session_id=session_id,
                user_query=query,
                assistant_payload=clarification_payload,
            )

            return clarification_payload

        queries = gemini_payload.get("queries", {})
        if not queries or "output" not in queries:
            error = "Planner did not return output query"

            assistant_payload = {
                "success": False,
                "text": classify_error(error, gemini_payload),
                "rows": [],
                "columns": [],
                "planner": gemini_payload,
            }

            self.add_to_conversation(
                session_id=session_id,
                user_query=query,
                assistant_payload=assistant_payload,
            )

            return assistant_payload

        insight_query = queries.get("insight", {})
        output_query = queries.get("output", {})

        insight_sql = insight_query.get("sql")
        output_sql = output_query.get("sql")

        # --- Execute INSIGHT SQL (NOT exposed to UI) ---
        insight_rows = []
        if insight_sql:
            try:
                insight_rows = execute_sql(insight_sql)
            except Exception:
                logger.warning(
                    "[REQ %s] Insight SQL execution failed (non-fatal)",
                    request_id,
                    exc_info=True,
                )

        narrative_result = {
            "narrative": "",
            "evidence": {"charts": []}
        }
        
        if insight_rows and gemini_payload.get("narrative_intent", {}).get("explain"):
            safe_insight_rows = normalize_rows_for_json(insight_rows)

            narrative_result = generate_narrative_from_insight(
                user_query=query,
                insight_rows=safe_insight_rows,
                narrative_intent=gemini_payload.get("narrative_intent", {}),
                insight_scope=gemini_payload.get("insight_scope", {}),
            )
            narrative_charts = (
                narrative_result.get("evidence", {}).get("charts") or []
            )

            logger.info(
                "[REQ %s] Narrative charts from LLM: count=%d, sample=%s",
                request_id,
                len(narrative_charts),
                narrative_charts[0] if narrative_charts else None,
            )

            # Narrative charts are already normalized and authoritative
            # Mark them as paragraph-evidence charts
            for c in narrative_charts:
                c["scope"] = "paragraph"

        # --- Execute OUTPUT SQL (UI-visible) ---
        try:
            rows = execute_sql(output_sql) or []
            columns = list(rows[0].keys()) if rows else []
            # Build case-insensitive column map
            column_map = {c.lower(): c for c in columns}

            logger.info(
                "[REQ %s] Output rows for UI: %d rows, columns=%s",
                request_id,
                len(rows),
                columns,
            )

            # Assign UI rows/columns immediately after successful execution
            ui_rows = rows
            ui_columns = columns

            execution_error = None
            success = True

            # --------------------------------------------------
            # Deterministic BASE charts from OUTPUT table (legacy behavior)
            # --------------------------------------------------
            base_charts = []

            if success and ui_rows and ui_columns:
                # detect time column
                time_col = None
                for c in ui_columns:
                    if c.lower() in ("report_date", "date"):
                        time_col = c
                        break
                if not time_col:
                    time_col = ui_columns[0]

                # numeric columns except time
                metric_cols = [c for c in ui_columns if c != time_col]

                # single multi-metric line chart (legacy overall chart)
                if metric_cols:
                    base_charts.append({
                        "id": "base_chart_0",
                        "title": "Overall Trend",
                        "chart_type": "line",
                        "columns": [time_col] + metric_cols,
                        "rows": build_chart_rows(ui_rows, [time_col] + metric_cols),
                        "scope": "output",
                        "metrics": set(metric_cols),
                    })
        except Exception as e:
            logger.exception("[REQ %s] Output SQL execution failed", request_id)
            rows = []
            columns = []
            execution_error = str(e)
            success = False

        # --------------------------------------------------
        # Chart handling (NON-DESTRUCTIVE)
        # Base charts = deterministic output visuals (legacy)
        # Narrative charts = proof-of-insight visuals
        # --------------------------------------------------
        final_charts = []
        final_charts.extend(base_charts)
        final_charts.extend(narrative_charts)
        analysis_text = ""
        if success:
            analysis_text = narrative_result.get("narrative", "").strip()

        # Split narrative into paragraph blocks for planner
        narrative_blocks = split_narrative_into_blocks(analysis_text)
        block_map = {b["id"]: b for b in narrative_blocks}

        for chart in narrative_charts:
            pid = chart.get("paragraph_id")
            if pid and pid in block_map:
                block_map[pid]["chart_refs"].append(chart["id"])

        # --------------------------------------------------
        # UI DATA CONTRACT (NON-NEGOTIABLE)
        # --------------------------------------------------
        # If an OUTPUT query was planned, it MUST be rendered
        # (ui_rows and ui_columns already assigned after SQL execution)

        # --------------------------------------------------
        # Enrichment (derived, not inferred)
        # --------------------------------------------------

        error_message = None
        if not success:
            error_message = classify_error(execution_error, gemini_payload)

        response_payload = {
            "success": success,
            "text": (
                analysis_text
                if success and analysis_text
                else error_message or ""
            ),
            "rows": ui_rows if success else [],
            "columns": ui_columns if success else [],
            **({"charts": final_charts} if success and final_charts else {}),
            **({"narrative_blocks": narrative_blocks} if narrative_blocks else {}),
            "planner": gemini_payload,
            "execution_error": execution_error,
            "insight_rows": normalize_rows_for_json(insight_rows) if insight_rows else [],
        }

        self.add_to_conversation(
            session_id=session_id,
            user_query=query,
            assistant_payload=response_payload,
        )

        return response_payload


conversation_service = ConversationService()