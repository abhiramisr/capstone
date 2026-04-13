from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
import os
from typing import Any, List, Dict

from src.orchestrator import run_pipeline
from src.models.schemas import FinalResponse, ColumnInfo
from src.connectors.fabric_connector import USE_FABRIC, execute_sql_fabric, get_fabric_connection
from src.tools.schema_introspect import get_active_datasource, load_csv_to_duckdb, load_datasource_config
from src.tools.sql_execute import execute_sql
from src.chat_store import init_db, save_chat, get_recent_chats, delete_chat
from src.anomaly_store import init_anomaly_db, get_recent_findings
from src.anomaly_runner import run_anomaly_detection, start_anomaly_scheduler


load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConversationTurn(BaseModel):
    question: str
    answer: str
    sql: str = ""


class QueryRequest(BaseModel):
    query: str
    source: str | None = None
    conversation_history: list[ConversationTurn] = []


def _resolve_csv_path(source: str | None) -> str:
    """Mirror the CLI behavior for locating the CSV file."""
    csv_path = source or "new_retail_data 1.csv"
    if not os.path.isabs(csv_path):
        if not os.path.exists(csv_path):
            alt = os.path.join("data", "sources", csv_path)
            if os.path.exists(alt):
                csv_path = alt
    return csv_path


def _build_chart_data(preview_rows: List[Dict[str, Any]], columns: List[ColumnInfo]) -> List[Dict[str, Any]]:
    """
    Build a simple chart configuration grounded in the preview rows.

    Heuristic:
      - Use the first column as the categorical "name"
      - Use the first numeric-looking column as "value"
    """
    if not preview_rows or not columns:
        return []

    dim_col = columns[0].name

    metric_col = None
    for col in columns[1:]:
        t = col.type.lower()
        if any(tok in t for tok in ("int", "double", "decimal", "numeric", "float", "real", "hugeint")):
            metric_col = col.name
            break

    if metric_col is None and len(columns) > 1:
        metric_col = columns[1].name

    chart_data: List[Dict[str, Any]] = []
    for row in preview_rows:
        name = row.get(dim_col)
        value = row.get(metric_col) if metric_col else None
        if name is not None and value is not None:
            chart_data.append({"name": str(name), "value": value})
    return chart_data


@app.on_event("startup")
async def on_startup() -> None:
    init_db()
    init_anomaly_db()
    if os.environ.get("ANOMALY_ENABLED", "").lower() in ("1", "true", "yes"):
        start_anomaly_scheduler()


@app.post("/ask")
async def handle_query(request: QueryRequest):
    csv_path = _resolve_csv_path(request.source)

    # When an API key is not configured, fall back to a direct,
    # deterministic query over the CSV so the UI still receives
    # grounded data and SQL.
    if not os.environ.get("OPENAI_API_KEY"):
        if USE_FABRIC:
            ds = get_active_datasource(load_datasource_config(), use_fabric=True)
            table = ""
            if ds:
                table = str(ds.get("default_table") or "")
                if not table and ds.get("tables"):
                    table = str(ds["tables"][0].get("name") or "")
            if not table:
                table = "tbltransactions"
            sql = f"SELECT TOP (20) * FROM {table}"
            conn = get_fabric_connection()
            try:
                exec_result = execute_sql_fabric(conn, sql, run_id="no_api_key")
            finally:
                conn.close()
            citations = [table]
            data_backend = "fabric"
        else:
            conn = load_csv_to_duckdb(csv_path)
            sql = "SELECT * FROM retail_transactions_typed LIMIT 20"
            exec_result = execute_sql(conn, sql, run_id="no_api_key")
            citations = ["retail_transactions_typed"]
            data_backend = "duckdb"

        preview_rows = exec_result.preview_rows
        columns = exec_result.columns
        chart_data = _build_chart_data(preview_rows, columns)

        answer = (
            "Environment is running without an OPENAI_API_KEY. "
            "Here is a preview slice of the underlying data."
        )

        save_chat(request.query, answer, sql)

        return {
            "answer": answer,
            "sql": sql,
            "citations": citations,
            "chartData": chart_data,
            "previewRows": preview_rows,
            "columns": [c.model_dump() for c in columns],
            "dataBackend": data_backend,
        }

    # Delegate to the existing multi‑agent text‑to‑SQL pipeline.
    final: FinalResponse = await run_pipeline(
        request.query, csv_path, conversation_history=request.conversation_history
    )

    # Persist this exchange for history
    save_chat(final.question, final.answer, final.sql or "")

    # Build grounded chart data + preview table
    chart_data = _build_chart_data(final.preview_rows, final.columns)

    citations = final.citation_tables

    return {
        "answer": final.answer,
        "sql": final.sql,
        "citations": citations,
        "chartData": chart_data,
        "previewRows": final.preview_rows,
        "columns": [c.model_dump() for c in final.columns],
        "dataBackend": final.data_backend or ("fabric" if USE_FABRIC else "duckdb"),
    }


@app.get("/history")
def history(limit: int = 20):
    """
    Return the most recent chat questions for easy re-use in the sidebar.
    """
    rows = get_recent_chats(limit=limit)
    # Only expose fields the UI needs
    history_items = [
        {
            "id": r["id"],
            "question": r["question"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]
    return {"history": history_items}


@app.delete("/history/{chat_id}")
def delete_history_item(chat_id: int):
    """Delete a single history item by id."""
    delete_chat(chat_id)
    return {"status": "ok", "id": chat_id}


@app.get("/anomalies")
def get_anomalies(limit: int = 10):
    """Return the most recent anomaly findings."""
    findings = get_recent_findings(limit=limit)
    return {"findings": findings}


@app.post("/anomalies/run")
async def trigger_anomaly_run():
    """Trigger an on-demand anomaly detection run."""
    report = await run_anomaly_detection()
    if report is None:
        return {"status": "error", "message": "Anomaly detection failed — check server logs"}
    return {
        "status": "ok",
        "severity": report.severity,
        "summary": report.summary,
        "anomaly_count": len(report.anomalies),
        "trend_count": len(report.trends),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)